package org.folio.rest.impl;

import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.folio.rest.tools.utils.TenantTool.tenantId;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import java.util.Date;
import java.util.Map;
import javax.ws.rs.core.Response;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.RecordStorageKafkaTopic;
import org.folio.kafka.services.KafkaAdminClientService;
import org.folio.liquibase.LiquibaseUtil;
import org.folio.rest.annotations.Validate;
import org.folio.rest.jaxrs.model.Parameter;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.rest.jaxrs.model.Snapshot.Status;
import org.folio.rest.jaxrs.model.TenantAttributes;
import org.folio.rest.tools.utils.TenantTool;
import org.folio.services.SRSKafkaTopicService;
import org.folio.services.SnapshotService;
import org.folio.spring.SpringContextUtil;
import org.springframework.beans.factory.annotation.Autowired;

public class ModTenantAPI extends TenantAPI {

  private static final Logger LOGGER = LogManager.getLogger();

  static final String LOAD_SAMPLE_PARAMETER = "loadSample";

  static final Snapshot STUB_SNAPSHOT = new Snapshot()
    .withJobExecutionId("00000000-0000-0000-0000-000000000000")
    .withStatus(Status.COMMITTED)
    .withProcessingStartedDate(new Date(1546351314000L));

  @Autowired
  private SnapshotService snapshotService;

  @Autowired
  private SRSKafkaTopicService srsKafkaTopicService;

  private final String tenantId;

  public ModTenantAPI(Vertx vertx, String tenantId) { //NOSONAR
    SpringContextUtil.autowireDependencies(this, Vertx.currentContext());
    this.tenantId = TenantTool.calculateTenantId(tenantId);
  }

  @Validate
  @Override
  Future<Integer> loadData(TenantAttributes attributes, String tenantId,
                           Map<String, String> headers, Context context) {
    // create topics before loading data
    Vertx vertx = context.owner();

    return super.loadData(attributes, tenantId, headers, context)
      .compose(num -> vertx.executeBlocking(() -> {
            LiquibaseUtil.initializeSchemaForTenant(vertx, tenantId);
            return null;
          })
          .compose(ar -> setLoadSampleParameter(attributes, context))
          .compose(v -> createStubSnapshot(attributes))
          .map(num)
      );
  }

  @Validate
  @Override
  public void postTenant(TenantAttributes tenantAttributes, Map<String, String> headers,
                         Handler<AsyncResult<Response>> handler, Context context) {
    // delete Kafka topics if tenant purged
    Future<Void> result = tenantAttributes.getPurge() != null && tenantAttributes.getPurge()
      ? new KafkaAdminClientService(context.owner()).deleteKafkaTopics(RecordStorageKafkaTopic.values(), tenantId(headers))
      : Future.succeededFuture();
    result.onComplete(x -> super.postTenant(tenantAttributes, headers, ar -> {
      if (ar.succeeded()) {
        Vertx vertx = context.owner();
        var kafkaAdminClientService = new KafkaAdminClientService(vertx);
        kafkaAdminClientService.createKafkaTopics(srsKafkaTopicService.createTopicObjects(), tenantId);
        handler.handle(Future.succeededFuture(ar.result()));
      } else {
        handler.handle(Future.failedFuture(ar.cause()));
      }
    }, context));
  }

  private Future<Void> setLoadSampleParameter(TenantAttributes attributes, Context context) {
    String loadSampleParam = getTenantAttributesParameter(attributes);
    context.put(LOAD_SAMPLE_PARAMETER, Boolean.parseBoolean(loadSampleParam));
    return Future.succeededFuture();
  }

  private Future<Void> createStubSnapshot(TenantAttributes attributes) {
    String loadSampleParam = getTenantAttributesParameter(attributes);
    if (!Boolean.parseBoolean(loadSampleParam)) {
      LOGGER.info("createStubSnapshot:: Module is being deployed in production mode");
      return Future.succeededFuture();
    }

    LOGGER.info("createStubSnapshot:: Module is being deployed in test mode, stub snapshot will be created. Check the server log for details.");
    return snapshotService.saveSnapshot(STUB_SNAPSHOT, tenantId)
        .mapEmpty();
  }

  private String getTenantAttributesParameter(TenantAttributes attributes) {
    if (attributes == null) {
      return EMPTY;
    }
    return attributes.getParameters().stream()
      .filter(p -> p.getKey().equals(LOAD_SAMPLE_PARAMETER))
      .findFirst()
      .map(Parameter::getValue)
      .orElse(EMPTY);
  }
}
