package org.folio.consumers;

import static org.folio.dao.util.RecordDaoUtil.filterRecordByExternalId;
import static org.folio.services.util.EventHandlingUtil.toOkapiHeaders;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import java.util.Collections;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.dao.util.IdType;
import org.folio.dao.util.RecordType;
import org.folio.kafka.AsyncRecordHandler;
import org.folio.rest.jooq.enums.RecordState;
import org.folio.services.RecordService;
import org.folio.services.util.KafkaUtil;
import org.springframework.stereotype.Component;

@Component
public class AuthorityDomainKafkaHandler implements AsyncRecordHandler<String, String> {

  private static final Logger log = LogManager.getLogger();

  private static final String DOMAIN_EVENT_TYPE_HEADER = "domain-event-type";
  private static final String DELETE_DOMAIN_EVENT_TYPE = "DELETE";
  private static final String DELETE_EVENT_SUB_TYPE_FLD = "deleteEventSubType";
  private static final String TENANT_FLD = "tenant";

  private final RecordService recordService;

  public AuthorityDomainKafkaHandler(RecordService recordService) {
    this.recordService = recordService;
  }

  @Override
  public Future<String> handle(KafkaConsumerRecord<String, String> consumerRecord) {
    log.trace("handle:: Handling kafka record: '{}'", consumerRecord);

    var kafkaHeaders = consumerRecord.headers();
    var okapiHeaders = toOkapiHeaders(kafkaHeaders);

    String authorityId = consumerRecord.key();
    if (isUnexpectedDomainEvent(consumerRecord)) {
      log.trace("handle:: Expected only {} domain type. Skipping authority domain kafka record [ID: '{}']",
        DELETE_DOMAIN_EVENT_TYPE, authorityId);
      return Future.succeededFuture(authorityId);
    }

    var eventPayload = new JsonObject(consumerRecord.value());
    var tenantId = eventPayload.getString(TENANT_FLD);
    var eventSubType = EventSubType.valueOf(eventPayload.getString(DELETE_EVENT_SUB_TYPE_FLD));

    logInput(authorityId, eventSubType, tenantId);
    return (switch (eventSubType) {
      case SOFT_DELETE -> performSoftDelete(authorityId, tenantId);
      case HARD_DELETE -> performHardDelete(authorityId, okapiHeaders);
    }).onFailure(throwable -> logError(authorityId, eventSubType, tenantId));
  }

  private Future<String> performSoftDelete(String authorityId, String tenantId) {
    var condition = filterRecordByExternalId(authorityId);
    return recordService.getRecords(condition, RecordType.MARC_AUTHORITY, Collections.emptyList(), 0, 1, tenantId)
      .compose(recordCollection -> {
        if (recordCollection.getRecords().isEmpty()) {
          log.debug("handle:: No records found [externalId: '{}', tenantId: '{}']", authorityId, tenantId);
          return Future.succeededFuture();
        }
        var matchedId = recordCollection.getRecords().get(0).getMatchedId();

        return recordService.updateRecordsState(matchedId, RecordState.DELETED, RecordType.MARC_AUTHORITY, tenantId);
      }).map(authorityId);
  }

  private Future<String> performHardDelete(String authorityId, Map<String, String> okapiHeaders) {
    return recordService.deleteRecordsByExternalId(authorityId, IdType.RECORD, okapiHeaders).map(authorityId);
  }

  private void logError(String authorityId, EventSubType subType, String tenantId) {
    log.error("handle:: Failed to {} records [externalId: '{}', tenantId: '{}']", subType.getValueReadable(),
      authorityId, tenantId);
  }

  private void logInput(String authorityId, EventSubType subType, String tenantId) {
    log.info("handle:: Trying to {} records [externalId: '{}', tenantId '{}']",
      subType.getValueReadable(), authorityId, tenantId);
  }

  private boolean isUnexpectedDomainEvent(KafkaConsumerRecord<String, String> consumerRecord) {
    return !KafkaUtil.headerExists(DOMAIN_EVENT_TYPE_HEADER, DELETE_DOMAIN_EVENT_TYPE, consumerRecord.headers());
  }

  public enum EventSubType {

    SOFT_DELETE("soft-delete"),
    HARD_DELETE("hard-delete");

    private final String valueReadable;

    EventSubType(String valueReadable) {
      this.valueReadable = valueReadable;
    }

    public String getValueReadable() {
      return valueReadable;
    }
  }

}
