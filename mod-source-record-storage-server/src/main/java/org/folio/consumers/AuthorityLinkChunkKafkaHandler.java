package org.folio.consumers;

import static java.util.Collections.emptyList;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.folio.RecordStorageKafkaTopic.MARC_BIB;
import static org.folio.dao.util.ParsedRecordDaoUtil.normalize;
import static org.folio.services.util.EventHandlingUtil.createProducer;
import static org.folio.services.util.EventHandlingUtil.createTopicName;

import com.google.common.collect.Maps;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaHeader;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class AuthorityLinkChunkKafkaHandler implements AsyncRecordHandler<String, String> {

  public static final String SRS_BIB_UPDATE_TOPIC = MARC_BIB.moduleName() + "." + MARC_BIB.topicName();

  private static final Logger log = LogManager.getLogger();
  private static final String PARSED_RECORD_FIELDS_KEY = "fields";
  private static final String PARSED_RECORD_SUBFIELDS_KEY = "subfields";
  private static final String AUTHORITY_ID_SUBFIELD = "9";
  private static final AtomicLong INDEXER = new AtomicLong();

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
          log.error("Failed to handle {} event", SRS_BIB_UPDATE_TOPIC, th);
          return Future.failedFuture(th);
        }
      );
  }

  private Future<BibAuthorityLinksUpdate> mapToEvent(KafkaConsumerRecord<String, String> consumerRecord) {
    try {
      var event = Json.decodeValue(consumerRecord.value(), BibAuthorityLinksUpdate.class);
      log.info("Decoded {} event for authorityId {}", BibAuthorityLinksUpdate.class, event.getAuthorityId());
      return Future.succeededFuture(event);
    } catch (Exception e) {
      log.error("Failed to decode event with key {}", consumerRecord.key());
      return Future.failedFuture(e);
    }
  }

  private Future<RecordCollection> retrieveRecords(BibAuthorityLinksUpdate bibAuthorityLinksUpdate, String tenantId) {
    log.trace("Retrieving bibs for authorityId {}", bibAuthorityLinksUpdate.getAuthorityId());
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
    log.debug("Retrieved {} records for authorityId {}.",
      recordCollection.getTotalRecords(), bibAuthorityLinksUpdate.getAuthorityId());

    return getSubfieldProcessorForEvent(bibAuthorityLinksUpdate).map(subfieldProcessor -> {
      recordCollection.getRecords().forEach(bibRecord -> {
        var newRecordId = UUID.randomUUID().toString();
        var instanceId = bibRecord.getExternalIdsHolder().getInstanceId();
        var parsedRecord = bibRecord.getParsedRecord();
        var parsedRecordContent = normalize(parsedRecord.getContent());
        var fieldsArray = parsedRecordContent.getJsonArray(PARSED_RECORD_FIELDS_KEY);


        var updateTargetFieldCodes = extractUpdateTargetFieldCodesForInstance(bibAuthorityLinksUpdate, instanceId);
        var subfieldChanges = bibAuthorityLinksUpdate.getSubfieldsChanges().stream()
          .filter(subfieldsChange -> updateTargetFieldCodes.contains(subfieldsChange.getField()))
          .collect(Collectors.toList());

        var fieldsList = fieldsArray.stream()
          .map(JsonObject::mapFrom)
          .map(fieldJson -> fieldJson.getMap().entrySet().stream().findFirst().orElse(null))
          .filter(Objects::nonNull)
          .map(field -> {
            var fieldCode = field.getKey();
            if (!updateTargetFieldCodes.contains(fieldCode)) {
              return field;
            }

            var fieldValueJson = JsonObject.mapFrom(field.getValue());
            var subfieldsJson = fieldValueJson.getJsonArray(PARSED_RECORD_SUBFIELDS_KEY);
            if (subfieldsJson == null) {
              return field;
            }
            var subfields = extractSubfields(subfieldsJson);

            var authorityId = extractAuthorityId(subfields);
            if (authorityId.isEmpty() || !bibAuthorityLinksUpdate.getAuthorityId().equals(authorityId.get())) {
              return field;
            }

            var newSubfields = subfieldProcessor.process(fieldCode, subfieldChanges, subfields);
            log.trace("AuthorityId {}, instanceId {}, field {}, old subfields: {}, new subfields: {}",
              bibAuthorityLinksUpdate.getAuthorityId(), instanceId, fieldCode, subfieldsJson, newSubfields);

            fieldValueJson.put(PARSED_RECORD_SUBFIELDS_KEY, newSubfields);
            field.setValue(fieldValueJson);
            return field;
          })
          .collect(Collectors.toList());

        parsedRecordContent.put(PARSED_RECORD_FIELDS_KEY, fieldsList);
        parsedRecord.setContent(parsedRecordContent.encode());
        parsedRecord.setFormattedContent(EMPTY);
        parsedRecord.setId(newRecordId);
        bibRecord.setId(newRecordId);
        bibRecord.getRawRecord().setId(newRecordId);
        bibRecord.setSnapshotId(bibAuthorityLinksUpdate.getJobId());
      });

      return recordCollection;
    });
  }

  private Future<SubfieldProcessor> getSubfieldProcessorForEvent(BibAuthorityLinksUpdate bibAuthorityLinksUpdate) {

    var eventType = bibAuthorityLinksUpdate.getType();
    switch (eventType) {
      case DELETE: {
        log.debug("Precessing DELETE event for authorityId {}", bibAuthorityLinksUpdate.getAuthorityId());
        return Future.succeededFuture(deleteSubfieldProcessor());
      }
      case UPDATE: {
        log.debug("Precessing UPDATE event for authorityId {}", bibAuthorityLinksUpdate.getAuthorityId());
        return Future.succeededFuture(updateSubfieldProcessor());
      }
      default: {
        return Future.failedFuture(new IllegalArgumentException(
          "Unsupported event type: " + eventType + " for authorityId " + bibAuthorityLinksUpdate.getAuthorityId()));
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

  private List<Map.Entry<String, Object>> extractSubfields(JsonArray subfields) {
    return subfields.stream()
      .map(JsonObject::mapFrom)
      .map(subfieldJson -> subfieldJson.getMap().entrySet().stream().findFirst().orElse(null))
      .filter(Objects::nonNull)
      .collect(Collectors.toList());
  }

  private Optional<Object> extractAuthorityId(List<Map.Entry<String, Object>> subfields) {
    return subfields.stream()
      .filter(entry -> entry.getKey().equals(AUTHORITY_ID_SUBFIELD))
      .map(Map.Entry::getValue)
      .findFirst();
  }

  private List<MarcBibUpdate> mapRecordsToBibUpdateEvents(RecordsBatchResponse batchResponse, BibAuthorityLinksUpdate event) {
    log.debug("Updated {} bibs for authorityId {}", batchResponse.getTotalRecords(), event.getAuthorityId());

    var errors = batchResponse.getErrorMessages();
    if (!errors.isEmpty()) {
      log.error("Unable to batch update some of linked bib records for authority: {}."
          + " Total number of records: {}, successful: {}, failures: {}",
        event.getAuthorityId(), batchResponse.getTotalRecords(), batchResponse.getRecords().size(), errors);
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
    log.info("Sending {} bib update events for authorityId {}", marcBibUpdateEvents.size(), event.getAuthorityId());
    return Future.fromCompletionStage(
      CompletableFuture.allOf(
        marcBibUpdateEvents.stream()
          .map(marcBibUpdate -> sendEventToKafka(marcBibUpdate, consumerRecord.headers())
            .onFailure(th -> log.error("Failed to send {} event", SRS_BIB_UPDATE_TOPIC, th)))
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
          log.debug("Event with type {} was sent to kafka", SRS_BIB_UPDATE_TOPIC);
          promise.complete(true);
        } else {
          var cause = war.cause();
          log.error("Failed to sent event {}, cause: {}", SRS_BIB_UPDATE_TOPIC, cause);
          promise.fail(cause);
        }
      });
    } catch (Exception e) {
      log.error("Failed to send an event for eventType {}, cause {}", SRS_BIB_UPDATE_TOPIC, e);
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

  private SubfieldProcessor updateSubfieldProcessor() {
    return (fieldCode, subfieldsChanges, oldSubfields) -> {
      var newSubfields = new HashMap<String, Map.Entry<String, Object>>();

      //add subfields from incoming event
      subfieldsChanges.stream()
        .filter(updateFieldValue -> updateFieldValue.getField().equals(fieldCode))
        .flatMap(subfieldsChange -> subfieldsChange.getSubfields().stream())
        .forEach(subfield -> newSubfields.put(subfield.getCode(),
          Maps.immutableEntry(subfield.getCode(), subfield.getValue())));

      //add old subfields which codes are absent in incoming event
      oldSubfields.stream()
        .filter(subfield -> !newSubfields.containsKey(subfield.getKey()))
        .forEach(subfield -> newSubfields.put(subfield.getKey(), subfield));

      return newSubfields.values();
    };
  }

  private SubfieldProcessor deleteSubfieldProcessor() {
    return (fieldCode, subfieldsChanges, oldSubfields) -> {
      var newSubfields = new LinkedList<>(oldSubfields);

      newSubfields.removeIf(subfield -> subfield.getKey().equals(AUTHORITY_ID_SUBFIELD));

      return newSubfields;
    };
  }

  private interface SubfieldProcessor {
    Collection<Map.Entry<String, Object>> process(String fieldCode,
                                                  List<SubfieldsChange> subfieldsChanges,
                                                  List<Map.Entry<String, Object>> oldSubfields);
  }

}
