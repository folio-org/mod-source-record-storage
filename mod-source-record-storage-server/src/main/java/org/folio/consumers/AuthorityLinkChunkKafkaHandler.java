package org.folio.consumers;

import static java.util.Collections.emptyList;
import static org.apache.commons.collections4.CollectionUtils.isEmpty;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.folio.RecordStorageKafkaTopic.MARC_BIB;
import static org.folio.consumers.RecordMappingUtils.mapObjectRepresentationToParsedContentJsonString;
import static org.folio.consumers.RecordMappingUtils.readParsedContentToObjectRepresentation;
import static org.folio.services.util.EventHandlingUtil.createProducer;
import static org.folio.services.util.EventHandlingUtil.createTopicName;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.Json;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaHeader;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.dao.util.IdType;
import org.folio.dao.util.RecordDaoUtil;
import org.folio.dao.util.RecordType;
import org.folio.kafka.AsyncRecordHandler;
import org.folio.kafka.KafkaConfig;
import org.folio.rest.jaxrs.model.BibAuthorityLinksUpdate;
import org.folio.rest.jaxrs.model.Link;
import org.folio.rest.jaxrs.model.MarcBibUpdate;
import org.folio.rest.jaxrs.model.RecordCollection;
import org.folio.rest.jaxrs.model.RecordsBatchResponse;
import org.folio.rest.jaxrs.model.SubfieldsChange;
import org.folio.rest.jaxrs.model.UpdateTarget;
import org.folio.services.RecordService;
import org.marc4j.marc.Subfield;
import org.marc4j.marc.impl.DataFieldImpl;
import org.marc4j.marc.impl.SubfieldImpl;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class AuthorityLinkChunkKafkaHandler implements AsyncRecordHandler<String, String> {

  public static final String SRS_BIB_UPDATE_TOPIC = MARC_BIB.moduleName() + "." + MARC_BIB.topicName();

  private static final char AUTHORITY_ID_SUBFIELD = '9';
  private static final AtomicLong INDEXER = new AtomicLong();
  private static final Logger LOGGER = LogManager.getLogger();

  private final RecordService recordService;
  private final KafkaConfig kafkaConfig;
  private final KafkaProducer<String, String> producer;

  @Value("${srs.kafka.AuthorityLinkChunkKafkaHandler.maxDistributionNum:100}")
  private int maxDistributionNum;

  public AuthorityLinkChunkKafkaHandler(RecordService recordService, KafkaConfig kafkaConfig) {
    this.recordService = recordService;
    this.kafkaConfig = kafkaConfig;

    producer = createProducer(SRS_BIB_UPDATE_TOPIC, kafkaConfig);
  }

  @Override
  public Future<String> handle(KafkaConsumerRecord<String, String> consumerRecord) {
    return mapToEvent(consumerRecord)
      .compose(event -> retrieveRecords(event, event.getTenant())
        .compose(recordCollection -> mapRecordFieldsChanges(event, recordCollection))
        .compose(recordCollection -> recordService.saveRecords(recordCollection, event.getTenant()))
        .map(recordsBatchResponse -> mapRecordsToBibUpdateEvents(recordsBatchResponse, event))
        .compose(marcBibUpdates -> sendEvents(marcBibUpdates, event, consumerRecord))
      ).recover(th -> {
          LOGGER.error("Failed to handle {} event", SRS_BIB_UPDATE_TOPIC, th);
          return Future.failedFuture(th);
        }
      );
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

  private Future<RecordCollection> retrieveRecords(BibAuthorityLinksUpdate bibAuthorityLinksUpdate, String tenantId) {
    LOGGER.trace("Retrieving bibs for jobId {}, authorityId {}",
      bibAuthorityLinksUpdate.getJobId(), bibAuthorityLinksUpdate.getAuthorityId());
    var instanceIds = bibAuthorityLinksUpdate.getUpdateTargets().stream()
      .flatMap(updateTarget -> updateTarget.getLinks().stream()
        .map(Link::getInstanceId))
      .distinct()
      .collect(Collectors.toList());

    var condition = RecordDaoUtil.getExternalIdsCondition(instanceIds, IdType.INSTANCE)
      .and(RecordDaoUtil.filterRecordByDeleted(false));
    return recordService.getRecords(condition, RecordType.MARC_BIB, emptyList(), 0, instanceIds.size(), tenantId);
  }

  private Future<RecordCollection> mapRecordFieldsChanges(BibAuthorityLinksUpdate bibAuthorityLinksUpdate,
                                                          RecordCollection recordCollection) {
    LOGGER.debug("Retrieved {} bib records for jobId {}, authorityId {}",
      recordCollection.getTotalRecords(), bibAuthorityLinksUpdate.getJobId(), bibAuthorityLinksUpdate.getAuthorityId());

    return getLinkProcessorForEvent(bibAuthorityLinksUpdate).map(linkProcessor -> {
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

          parsedRecordContent.removeVariableField(field);
          parsedRecordContent.addVariableField(newField);
        });

        parsedRecord.setContent(mapObjectRepresentationToParsedContentJsonString(parsedRecordContent));
        parsedRecord.setFormattedContent(EMPTY);
        parsedRecord.setId(newRecordId);
        bibRecord.setId(newRecordId);
        bibRecord.getRawRecord().setId(newRecordId);
        bibRecord.setSnapshotId(bibAuthorityLinksUpdate.getJobId());
      });

      return recordCollection;
    });
  }

  private Future<LinkProcessor> getLinkProcessorForEvent(BibAuthorityLinksUpdate bibAuthorityLinksUpdate) {
    var eventType = bibAuthorityLinksUpdate.getType();
    switch (eventType) {
      case DELETE: {
        LOGGER.debug("Precessing DELETE event for jobId {}, authorityId {}",
          bibAuthorityLinksUpdate.getJobId(), bibAuthorityLinksUpdate.getAuthorityId());
        return Future.succeededFuture(deleteLinkProcessor());
      }
      case UPDATE: {
        LOGGER.debug("Precessing UPDATE event for jobId {}, authorityId {}",
          bibAuthorityLinksUpdate.getJobId(), bibAuthorityLinksUpdate.getAuthorityId());
        return Future.succeededFuture(updateLinkProcessor());
      }
      default: {
        return Future.failedFuture(new IllegalArgumentException(
          String.format("Unsupported event type: %s for jobId %s, authorityId %s",
            eventType, bibAuthorityLinksUpdate.getJobId(), bibAuthorityLinksUpdate.getAuthorityId())));
      }
    }
  }

  private List<String> extractUpdateTargetFieldCodesForInstance(BibAuthorityLinksUpdate bibAuthorityLinksUpdate, String instanceId) {
    return bibAuthorityLinksUpdate.getUpdateTargets().stream()
      .filter(updateTarget -> updateTarget.getLinks().stream()
        .anyMatch(link -> link.getInstanceId().equals(instanceId)))
      .map(UpdateTarget::getField)
      .collect(Collectors.toList());
  }

  private Optional<Subfield> getAuthorityIdSubfield(List<Subfield> subfields) {
    return subfields.stream()
      .filter(subfield -> subfield.getCode() == AUTHORITY_ID_SUBFIELD)
      .findFirst();
  }

  private List<MarcBibUpdate> mapRecordsToBibUpdateEvents(RecordsBatchResponse batchResponse, BibAuthorityLinksUpdate event) {
    LOGGER.debug("Updated {} bibs for jobId {}, authorityId {}",
      batchResponse.getTotalRecords(), event.getJobId(), event.getAuthorityId());

    var errors = batchResponse.getErrorMessages();
    if (!errors.isEmpty()) {
      LOGGER.error("Unable to batch update some of linked bib records for jobId {}, authorityId {}."
          + " Total number of records: {}, successful: {}, failures: {}",
        event.getJobId(), event.getAuthorityId(),
        batchResponse.getTotalRecords(), batchResponse.getRecords().size(), errors);
    }

    return toMarcBibUpdateEvents(batchResponse, event);
  }

  private List<MarcBibUpdate> toMarcBibUpdateEvents(RecordsBatchResponse batchResponse,
                                                    BibAuthorityLinksUpdate bibAuthorityLinksUpdate) {
    var instanceIdToLinkIds = bibAuthorityLinksUpdate.getUpdateTargets().stream()
      .flatMap(updateTarget -> updateTarget.getLinks().stream())
      .collect(Collectors.groupingBy(Link::getInstanceId, Collectors.mapping(Link::getLinkId, Collectors.toList())));
    return batchResponse.getRecords().stream()
      .map(bibRecord -> {
        var instanceId = bibRecord.getExternalIdsHolder().getInstanceId();
        return new MarcBibUpdate()
          .withJobId(bibAuthorityLinksUpdate.getJobId())
          .withLinkIds(instanceIdToLinkIds.get(instanceId))
          .withTenant(bibAuthorityLinksUpdate.getTenant())
          .withType(MarcBibUpdate.Type.UPDATE)
          .withTs(bibAuthorityLinksUpdate.getTs())
          .withRecord(bibRecord);
      })
      .collect(Collectors.toList());
  }

  private Future<String> sendEvents(List<MarcBibUpdate> marcBibUpdateEvents, BibAuthorityLinksUpdate event,
                                    KafkaConsumerRecord<String, String> consumerRecord) {
    LOGGER.info("Sending {} bib update events for jobId {}, authorityId {}",
      marcBibUpdateEvents.size(), event.getJobId(), event.getAuthorityId());
    return Future.fromCompletionStage(
      CompletableFuture.allOf(
        marcBibUpdateEvents.stream()
          .map(marcBibUpdate -> sendEventToKafka(marcBibUpdate, consumerRecord.headers())
            .onFailure(th -> LOGGER.error("Failed to send {} event for jobId {}.",
              SRS_BIB_UPDATE_TOPIC, marcBibUpdate.getJobId(), th)))
          .map(Future::toCompletionStage)
          .map(CompletionStage::toCompletableFuture)
          .toArray(CompletableFuture[]::new)
      ).minimalCompletionStage()
    ).map(unused -> consumerRecord.key());
  }

  private Future<Boolean> sendEventToKafka(MarcBibUpdate marcBibUpdate, List<KafkaHeader> kafkaHeaders) {
    var promise = Promise.<Boolean>promise();
    try {
      var kafkaRecord = createKafkaProducerRecord(marcBibUpdate, kafkaHeaders);
      producer.write(kafkaRecord, war -> {
        if (war.succeeded()) {
          LOGGER.debug("Event with type {}, jobId {} was sent to kafka", SRS_BIB_UPDATE_TOPIC, marcBibUpdate.getJobId());
          promise.complete(true);
        } else {
          var cause = war.cause();
          LOGGER.error("Failed to sent event {} for jobId {}, cause: {}", SRS_BIB_UPDATE_TOPIC, marcBibUpdate.getJobId(), cause);
          promise.fail(cause);
        }
      });
    } catch (Exception e) {
      LOGGER.error("Failed to send an event for eventType {}, jobId {}, cause {}", SRS_BIB_UPDATE_TOPIC, marcBibUpdate.getJobId(), e);
      return Future.failedFuture(e);
    }
    return promise.future();
  }

  private KafkaProducerRecord<String, String> createKafkaProducerRecord(MarcBibUpdate marcBibUpdate,
                                                                        List<KafkaHeader> kafkaHeaders) {
    var topicName = createTopicName(SRS_BIB_UPDATE_TOPIC, marcBibUpdate.getTenant(), kafkaConfig);
    var key = String.valueOf(INDEXER.incrementAndGet() % maxDistributionNum);
    var kafkaRecord = KafkaProducerRecord.create(topicName, key,Json.encode(marcBibUpdate));
    kafkaRecord.addHeaders(kafkaHeaders);

    return kafkaRecord;
  }

  private LinkProcessor updateLinkProcessor() {
    return (fieldCode, subfieldsChanges, oldSubfields) -> {
      var newSubfields = new HashMap<Character, Subfield>();

      //add old subfields
      oldSubfields.forEach(subfield -> newSubfields.put(subfield.getCode(), subfield));

      //add subfields from incoming event, removing ones with empty value
      subfieldsChanges.forEach(subfield -> {
        var code = subfield.getCode().charAt(0);
        if (isBlank(subfield.getValue())) {
          newSubfields.remove(code);
          return;
        }
        newSubfields.put(code, new SubfieldImpl(code, subfield.getValue()));
      });

      return newSubfields.values();
    };
  }

  private LinkProcessor deleteLinkProcessor() {
    return (fieldCode, subfieldsChanges, oldSubfields) -> {
      var newSubfields = new LinkedList<>(oldSubfields);

      newSubfields.removeIf(subfield -> subfield.getCode() == AUTHORITY_ID_SUBFIELD);

      return newSubfields;
    };
  }

  private interface LinkProcessor {
    Collection<Subfield> process(String fieldCode,
                                 List<org.folio.rest.jaxrs.model.Subfield> subfieldsChanges,
                                 List<Subfield> oldSubfields);
  }

}
