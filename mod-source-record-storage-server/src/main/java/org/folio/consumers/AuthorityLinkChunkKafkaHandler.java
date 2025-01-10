package org.folio.consumers;

import static java.util.Objects.nonNull;
import static org.apache.commons.collections4.CollectionUtils.isEmpty;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.folio.EntityLinksKafkaTopic.LINKS_STATS;
import static org.folio.RecordStorageKafkaTopic.MARC_BIB;
import static org.folio.consumers.RecordMappingUtils.mapObjectRepresentationToParsedContentJsonString;
import static org.folio.consumers.RecordMappingUtils.readParsedContentToObjectRepresentation;
import static org.folio.rest.jaxrs.model.LinkUpdateReport.Status.FAIL;
import static org.folio.services.util.EventHandlingUtil.createProducer;
import static org.folio.services.util.EventHandlingUtil.toOkapiHeaders;
import static org.folio.services.util.KafkaUtil.extractHeaderValue;
import static org.folio.util.AuthorityLinksUtils.getAuthorityIdSubfield;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.Json;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaHeader;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;

import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.dao.util.RecordType;
import org.folio.kafka.AsyncRecordHandler;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.services.KafkaTopic;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.rest.jaxrs.model.BibAuthorityLinksUpdate;
import org.folio.rest.jaxrs.model.Link;
import org.folio.rest.jaxrs.model.LinkUpdateReport;
import org.folio.rest.jaxrs.model.MarcBibUpdate;
import org.folio.rest.jaxrs.model.Metadata;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.RecordCollection;
import org.folio.rest.jaxrs.model.RecordsBatchResponse;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.rest.jaxrs.model.SubfieldsChange;
import org.folio.rest.jaxrs.model.UpdateTarget;
import org.folio.services.RecordService;
import org.folio.services.SnapshotService;
import org.folio.services.entities.RecordsModifierOperator;
import org.folio.services.handlers.links.DeleteLinkProcessor;
import org.folio.services.handlers.links.LinkProcessor;
import org.folio.services.handlers.links.UpdateLinkProcessor;
import org.marc4j.marc.impl.DataFieldImpl;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class AuthorityLinkChunkKafkaHandler implements AsyncRecordHandler<String, String> {
  private static final AtomicLong INDEXER = new AtomicLong();
  private static final Logger LOGGER = LogManager.getLogger();
  private final Map<KafkaTopic, KafkaProducer<String, String>> producers = new HashMap<>();
  private final KafkaConfig kafkaConfig;
  private final RecordService recordService;
  private final SnapshotService snapshotService;

  @Value("${srs.kafka.AuthorityLinkChunkKafkaHandler.maxDistributionNum:100}")
  private int maxDistributionNum;

  public AuthorityLinkChunkKafkaHandler(RecordService recordService, KafkaConfig kafkaConfig,
                                        SnapshotService snapshotService) {
    this.kafkaConfig = kafkaConfig;
    this.recordService = recordService;
    this.snapshotService = snapshotService;

    producers.put(MARC_BIB, createProducer(MARC_BIB.moduleTopicName(), kafkaConfig));
    producers.put(LINKS_STATS, createProducer(LINKS_STATS.moduleTopicName(), kafkaConfig));
  }

  @Override
  public Future<String> handle(KafkaConsumerRecord<String, String> consumerRecord) {
    LOGGER.trace("handle:: Start handling kafka record value: {}", consumerRecord.value());
    LOGGER.info("handle:: Start Handling kafka record");
    var userId = extractHeaderValue(XOkapiHeaders.USER_ID, consumerRecord.headers());

    var result = mapToEvent(consumerRecord)
      .compose(this::createSnapshot)
      .compose(linksUpdate -> {
        var instanceIds = getBibRecordExternalIds(linksUpdate);
        var okapiHeaders = toOkapiHeaders(consumerRecord.headers(), linksUpdate.getTenant());
        RecordsModifierOperator recordsModifier = recordsCollection ->
          this.mapRecordFieldsChanges(linksUpdate, recordsCollection, userId);

        return recordService.saveRecordsByExternalIds(instanceIds, RecordType.MARC_BIB, recordsModifier, okapiHeaders)
          .compose(recordsBatchResponse -> {
            sendReports(recordsBatchResponse, linksUpdate, consumerRecord.headers());
            var marcBibUpdateStats = mapRecordsToBibUpdateEventsByInstanceId(recordsBatchResponse, linksUpdate);
            return Future.all(marcBibUpdateStats.entrySet().stream()
                .map(entry -> sendEvents(entry.getValue(), linksUpdate, entry.getKey(), consumerRecord))
                .toList()
              )
              .map(unused -> consumerRecord.key());
          });
      });

    LOGGER.info("handle:: Finish Handling kafka record");
    return result;
  }

  private Future<BibAuthorityLinksUpdate> mapToEvent(KafkaConsumerRecord<String, String> consumerRecord) {
    try {
      var event = Json.decodeValue(consumerRecord.value(), BibAuthorityLinksUpdate.class);
      LOGGER.info("Decoded {} event for jobId {}, authorityId {}",
        BibAuthorityLinksUpdate.class, event.getJobId(), event.getAuthorityId());
      return Future.succeededFuture(event);
    } catch (Exception e) {
      LOGGER.error("Failed to decode event with key {}", consumerRecord.key());
      return Future.failedFuture(e);
    }
  }

  private Future<BibAuthorityLinksUpdate> createSnapshot(BibAuthorityLinksUpdate bibAuthorityLinksUpdate) {
    var now = new Date();
    var snapshot = new Snapshot()
      .withJobExecutionId(bibAuthorityLinksUpdate.getJobId())
      .withStatus(Snapshot.Status.COMMITTED)
      .withProcessingStartedDate(now)
      .withMetadata(new Metadata()
        .withCreatedDate(now)
        .withUpdatedDate(now));

    return snapshotService.saveSnapshot(snapshot, bibAuthorityLinksUpdate.getTenant())
      .map(result -> bibAuthorityLinksUpdate);
  }

  private List<String> getBibRecordExternalIds(BibAuthorityLinksUpdate linksUpdate) {
    return linksUpdate.getUpdateTargets().stream()
      .flatMap(updateTarget -> updateTarget.getLinks().stream()
        .map(Link::getInstanceId))
      .distinct()
      .toList();
  }

  private RecordCollection mapRecordFieldsChanges(BibAuthorityLinksUpdate bibAuthorityLinksUpdate,
                                                  RecordCollection recordCollection, String userId) {
    LOGGER.debug("Retrieved {} bib records for jobId {}, authorityId {}",
      recordCollection.getTotalRecords(), bibAuthorityLinksUpdate.getJobId(), bibAuthorityLinksUpdate.getAuthorityId());

    var linkProcessor = getLinkProcessorForEvent(bibAuthorityLinksUpdate);
    recordCollection.getRecords().forEach(bibRecord -> {
      var newRecordId = UUID.randomUUID().toString();
      var instanceId = bibRecord.getExternalIdsHolder().getInstanceId();
      var parsedRecord = bibRecord.getParsedRecord();
      var parsedRecordContent = readParsedContentToObjectRepresentation(bibRecord);
      var fields = new LinkedList<>(parsedRecordContent.getDataFields());

      var updateTargetFieldCodes = extractUpdateTargetFieldCodesForInstance(bibAuthorityLinksUpdate, instanceId);
      var subfieldChanges = bibAuthorityLinksUpdate.getSubfieldsChanges().stream()
        .filter(subfieldsChange -> updateTargetFieldCodes.contains(subfieldsChange.getField()))
        .collect(Collectors.toMap(SubfieldsChange::getField, SubfieldsChange::getSubfields));

      fields.forEach(field -> {
        if (!updateTargetFieldCodes.contains(field.getTag())) {
          return;
        }

        var subfields = field.getSubfields();
        if (isEmpty(subfields)) {
          return;
        }

        var authorityId = getAuthorityIdSubfield(subfields);
        if (authorityId.isEmpty() || !bibAuthorityLinksUpdate.getAuthorityId().equals(authorityId.get().getData())) {
          return;
        }

        var newSubfields = linkProcessor.process(field.getTag(), subfieldChanges.get(field.getTag()), subfields);
        LOGGER.trace("JobId {}, AuthorityId {}, instanceId {}, field {}, old subfields: {}, new subfields: {}",
          bibAuthorityLinksUpdate.getJobId(), bibAuthorityLinksUpdate.getAuthorityId(),
          instanceId, field.getTag(), subfields, newSubfields);

        var newField = new DataFieldImpl(field.getTag(), field.getIndicator1(), field.getIndicator2());
        newSubfields.forEach(newField::addSubfield);

        var dataFields = parsedRecordContent.getDataFields();
        var fieldPosition = dataFields.indexOf(field);
        dataFields.remove(fieldPosition);
        dataFields.add(fieldPosition, newField);
      });

      parsedRecord.setContent(mapObjectRepresentationToParsedContentJsonString(parsedRecordContent));
      parsedRecord.setFormattedContent(EMPTY);
      parsedRecord.setId(newRecordId);
      bibRecord.setId(newRecordId);
      bibRecord.getRawRecord().setId(newRecordId);
      bibRecord.setSnapshotId(bibAuthorityLinksUpdate.getJobId());
      setUpdatedBy(bibRecord, userId);
    });
    return recordCollection;
  }

  private LinkProcessor getLinkProcessorForEvent(BibAuthorityLinksUpdate bibAuthorityLinksUpdate) {
    var eventType = bibAuthorityLinksUpdate.getType();
    switch (eventType) {
      case DELETE -> {
        LOGGER.debug("Precessing DELETE event for jobId {}, authorityId {}",
          bibAuthorityLinksUpdate.getJobId(), bibAuthorityLinksUpdate.getAuthorityId());
        return new DeleteLinkProcessor();
      }
      case UPDATE -> {
        LOGGER.debug("Precessing UPDATE event for jobId {}, authorityId {}",
          bibAuthorityLinksUpdate.getJobId(), bibAuthorityLinksUpdate.getAuthorityId());
        return new UpdateLinkProcessor();
      }
      default ->
        throw new IllegalArgumentException(
          String.format("Unsupported event type: %s for jobId %s, authorityId %s",
            eventType, bibAuthorityLinksUpdate.getJobId(), bibAuthorityLinksUpdate.getAuthorityId()));
    }
  }

  private List<String> extractUpdateTargetFieldCodesForInstance(BibAuthorityLinksUpdate bibAuthorityLinksUpdate,
                                                                String instanceId) {
    return bibAuthorityLinksUpdate.getUpdateTargets().stream()
      .filter(updateTarget -> updateTarget.getLinks().stream()
        .anyMatch(link -> link.getInstanceId().equals(instanceId)))
      .map(UpdateTarget::getField)
      .toList();
  }

  private Map<String, List<MarcBibUpdate>> mapRecordsToBibUpdateEventsByInstanceId(RecordsBatchResponse batchResponse,
                                                                                   BibAuthorityLinksUpdate event) {
    LOGGER.debug("Updated {} bibs for jobId {}, authorityId {}",
      batchResponse.getTotalRecords(), event.getJobId(), event.getAuthorityId());

    var errors = batchResponse.getErrorMessages();
    if (!errors.isEmpty()) {
      LOGGER.error("Unable to batch update some of linked bib records for jobId {}, authorityId {}."
                   + " Total number of records: {}, successful: {}, failures: {}",
        event.getJobId(), event.getAuthorityId(),
        batchResponse.getTotalRecords(), batchResponse.getRecords().size(), errors);
    }

    return toMarcBibUpdateEventsByInstanceId(batchResponse, event);
  }

  private Map<String, List<MarcBibUpdate>> toMarcBibUpdateEventsByInstanceId(RecordsBatchResponse batchResponse,
                                                                             BibAuthorityLinksUpdate bibAuthorityLinksUpdate) {
    var instanceIdToLinkIds = bibAuthorityLinksUpdate.getUpdateTargets().stream()
      .flatMap(updateTarget -> updateTarget.getLinks().stream())
      .collect(Collectors.groupingBy(Link::getInstanceId, Collectors.mapping(Link::getLinkId, Collectors.toList())));

    return batchResponse.getRecords().stream()
      .map(bibRecord -> {
        var instanceId = bibRecord.getExternalIdsHolder().getInstanceId();
        var marcBibUpdate = new MarcBibUpdate()
          .withJobId(bibAuthorityLinksUpdate.getJobId())
          .withLinkIds(instanceIdToLinkIds.get(instanceId))
          .withTenant(bibAuthorityLinksUpdate.getTenant())
          .withType(MarcBibUpdate.Type.UPDATE)
          .withTs(bibAuthorityLinksUpdate.getTs())
          .withRecord(bibRecord);
        return Map.entry(instanceId, marcBibUpdate);
      })
      .collect(Collectors.groupingBy(this::entryKey, Collectors.mapping(this::entryValue, Collectors.toList())));
  }

  private String entryKey(Map.Entry<String, MarcBibUpdate> entry) {
    return entry.getKey();
  }

  private MarcBibUpdate entryValue(Map.Entry<String, MarcBibUpdate> entry) {
    return entry.getValue();
  }

  private List<LinkUpdateReport> toFailedLinkUpdateReports(List<Record> errorRecords,
                                                           BibAuthorityLinksUpdate bibAuthorityLinksUpdate) {
    var instanceIdToLinkIds = bibAuthorityLinksUpdate.getUpdateTargets().stream()
      .flatMap(updateTarget -> updateTarget.getLinks().stream())
      .collect(Collectors.groupingBy(Link::getInstanceId, Collectors.mapping(Link::getLinkId, Collectors.toList())));

    return errorRecords.stream()
      .map(bibRecord -> {
        var instanceId = bibRecord.getExternalIdsHolder().getInstanceId();
        return new LinkUpdateReport()
          .withInstanceId(instanceId)
          .withJobId(bibAuthorityLinksUpdate.getJobId())
          .withLinkIds(instanceIdToLinkIds.get(instanceId))
          .withTenant(bibAuthorityLinksUpdate.getTenant())
          .withTs(bibAuthorityLinksUpdate.getTs())
          .withFailCause(bibRecord.getErrorRecord().getDescription())
          .withStatus(FAIL);
      })
      .toList();
  }

  private void setUpdatedBy(Record changedRecord, String userId) {
    if (StringUtils.isNotBlank(userId)) {
      if (changedRecord.getMetadata() != null) {
        changedRecord.getMetadata().setUpdatedByUserId(userId);
      } else {
        changedRecord.withMetadata(new Metadata().withUpdatedByUserId(userId));
      }
    }
  }

  private void sendReports(RecordsBatchResponse batchResponse, BibAuthorityLinksUpdate event,
                           List<KafkaHeader> headers) {
    var errorRecords = getErrorRecords(batchResponse);
    if (!errorRecords.isEmpty()) {
      LOGGER.info("Errors detected. Sending {} linking reports for jobId {}, authorityId {}",
        errorRecords.size(), event.getJobId(), event.getAuthorityId());

      toFailedLinkUpdateReports(errorRecords, event).forEach(report ->
        sendEventToKafka(LINKS_STATS, report.getTenant(), report.getJobId(), null, report, headers));
    }
  }

  private Future<String> sendEvents(List<MarcBibUpdate> marcBibUpdateEvents,
                                    BibAuthorityLinksUpdate event,
                                    String instanceId,
                                    KafkaConsumerRecord<String, String> consumerRecord) {
    LOGGER.info("Sending {} bib update events for jobId {}, authorityId {}",
      marcBibUpdateEvents.size(), event.getJobId(), event.getAuthorityId());

    return Future.fromCompletionStage(
      CompletableFuture.allOf(
        marcBibUpdateEvents.stream()
          .map(bibUpdateEvent -> sendEventToKafka(MARC_BIB, bibUpdateEvent.getTenant(), bibUpdateEvent.getJobId(),
            instanceId, bibUpdateEvent, consumerRecord.headers()))
          .map(Future::toCompletionStage)
          .map(CompletionStage::toCompletableFuture)
          .toArray(CompletableFuture[]::new)
      ).minimalCompletionStage()
    ).map(unused -> consumerRecord.key());
  }

  private Future<Boolean> sendEventToKafka(KafkaTopic topic, String tenant, String jobId, String recordKey,
                                           Object marcRecord, List<KafkaHeader> kafkaHeaders) {
    var promise = Promise.<Boolean>promise();
    try {
      var kafkaRecord = createKafkaProducerRecord(topic, tenant, recordKey, marcRecord, kafkaHeaders);
      producers.get(topic).write(kafkaRecord, ar -> {
        if (ar.succeeded()) {
          LOGGER.debug("Event with type {}, jobId {} was sent to kafka", topic.topicName(), jobId);
          promise.complete(true);
        } else {
          var cause = ar.cause();
          LOGGER.error("Failed to sent event {} for jobId {}, cause: {}", topic.topicName(), jobId, cause);
          promise.fail(cause);
        }
      });
    } catch (Exception e) {
      LOGGER.error("Failed to send an event for eventType {}, jobId {}, cause {}", topic.topicName(), jobId, e);
      return Future.failedFuture(e);
    }
    return promise.future()
      .onFailure(th -> LOGGER.error("Failed to send {} event for jobId {}.", topic.topicName(), jobId, th));
  }

  private KafkaProducerRecord<String, String> createKafkaProducerRecord(KafkaTopic topic, String tenant,
                                                                        String recordKey, Object marcRecord,
                                                                        List<KafkaHeader> kafkaHeaders) {
    var topicName = topic.fullTopicName(kafkaConfig, tenant);
    var key = Optional.ofNullable(recordKey).orElse(String.valueOf(INDEXER.incrementAndGet() % maxDistributionNum));
    var kafkaRecord = KafkaProducerRecord.create(topicName, key, Json.encode(marcRecord));
    kafkaHeaders.removeIf(kafkaHeader -> !StringUtils.startsWithIgnoreCase(kafkaHeader.key(), "x-okapi-"));
    kafkaRecord.addHeaders(kafkaHeaders);

    return kafkaRecord;
  }

  private List<Record> getErrorRecords(RecordsBatchResponse batchResponse) {
    return batchResponse.getRecords().stream()
      .filter(marcRecord -> nonNull(marcRecord.getErrorRecord()))
      .toList();
  }

}
