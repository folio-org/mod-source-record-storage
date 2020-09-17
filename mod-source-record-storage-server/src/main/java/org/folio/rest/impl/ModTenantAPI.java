package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.folio.liquibase.LiquibaseUtil;
import org.folio.processing.events.EventManager;
import org.folio.processing.mapping.MappingManager;
import org.folio.processing.mapping.mapper.reader.record.MarcBibReaderFactory;
import org.folio.rest.annotations.Validate;
import org.folio.rest.jaxrs.model.Parameter;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.rest.jaxrs.model.Snapshot.Status;
import org.folio.rest.jaxrs.model.TenantAttributes;
import org.folio.rest.tools.utils.TenantTool;
import org.folio.rest.util.OkapiConnectionParams;
import org.folio.services.RecordService;
import org.folio.services.SnapshotService;
import org.folio.services.handlers.InstancePostProcessingEventHandler;
import org.folio.services.handlers.MarcRecordWriterFactory;
import org.folio.services.handlers.actions.ModifyRecordEventHandler;
import org.folio.spring.SpringContextUtil;
import org.folio.util.pubsub.PubSubClientUtils;
import org.springframework.beans.factory.annotation.Autowired;

import javax.ws.rs.core.Response;
import java.util.Date;
import java.util.Map;

import static org.apache.commons.lang3.StringUtils.EMPTY;

@SuppressWarnings("squid:CallToDeprecatedMethod")
public class ModTenantAPI extends TenantAPI {

  private static final Logger LOGGER = LoggerFactory.getLogger(ModTenantAPI.class);

  private static final String TEST_MODE_PARAMETER = "testMode";

  static final String LOAD_SAMPLE_PARAMETER = "loadSample";

  static final Snapshot STUB_SNAPSHOT = new Snapshot()
    .withJobExecutionId("00000000-0000-0000-0000-000000000000")
    .withStatus(Status.COMMITTED)
    .withProcessingStartedDate(new Date(1546351314000L));

  @Autowired
  private RecordService recordService;

  @Autowired
  private SnapshotService snapshotService;

  @Autowired
  private InstancePostProcessingEventHandler instancePostProcessingEventHandler;

  @Autowired
  private ModifyRecordEventHandler modifyRecordEventHandler;

  private String tenantId;

  public ModTenantAPI(Vertx vertx, String tenantId) { //NOSONAR
    SpringContextUtil.autowireDependencies(this, Vertx.currentContext());
    this.tenantId = TenantTool.calculateTenantId(tenantId);
    MappingManager.registerReaderFactory(new MarcBibReaderFactory());
    MappingManager.registerWriterFactory(new MarcRecordWriterFactory());
    EventManager.registerEventHandler(instancePostProcessingEventHandler);
    EventManager.registerEventHandler(modifyRecordEventHandler);
  }

  @Validate
  @Override
  public void postTenant(TenantAttributes entity, Map<String, String> headers, Handler<AsyncResult<Response>> handlers, Context context) {
    super.postTenant(entity, headers, ar -> {
      if (ar.failed()) {
        handlers.handle(ar);
      } else {
        Vertx vertx = context.owner();
        vertx.executeBlocking(
          blockingFuture -> {
            LiquibaseUtil.initializeSchemaForTenant(vertx, tenantId);
            blockingFuture.complete();
          },
          // so far, postTenant result doesn't depend on module registration till data import flow uses mod-pubsub as transport
          result -> setLoadSampleParameter(entity, context)
            .compose(v -> createStubSnapshot(entity))
            .compose(v -> registerModuleToPubsub(entity, headers, context.owner()))
            .onComplete(event -> handlers.handle(ar))
        );
      }
    }, context);
  }

  private Future<Void> setLoadSampleParameter(TenantAttributes attributes, Context context) {
    String loadSampleParam = getTenantAttributesParameter(attributes, LOAD_SAMPLE_PARAMETER);
    context.put(LOAD_SAMPLE_PARAMETER, Boolean.parseBoolean(loadSampleParam));
    return Future.succeededFuture();
  }

  private Future<Void> createStubSnapshot(TenantAttributes attributes) {
    String loadSampleParam = getTenantAttributesParameter(attributes, LOAD_SAMPLE_PARAMETER);
    if (!Boolean.parseBoolean(loadSampleParam)) {
      LOGGER.info("Module is being deployed in production mode");
      return Future.succeededFuture();
    }

    Promise<Void> promise = Promise.promise();
    snapshotService.saveSnapshot(STUB_SNAPSHOT, tenantId).onComplete(save -> {
      if (save.failed()) {
        promise.fail(save.cause());
      }
      promise.complete();
    });
    LOGGER.info("Module is being deployed in test mode, stub snapshot will be created. Check the server log for details.");

    return promise.future();
  }

  private String getTenantAttributesParameter(TenantAttributes attributes, String parameterName) {
    if (attributes == null) {
      return EMPTY;
    }
    return attributes.getParameters().stream()
      .filter(p -> p.getKey().equals(parameterName))
      .findFirst()
      .map(Parameter::getValue)
      .orElseGet(() -> EMPTY);
  }

  private Future<Void> registerModuleToPubsub(TenantAttributes attributes, Map<String, String> headers, Vertx vertx) {
    if (Boolean.parseBoolean(getTenantAttributesParameter(attributes, TEST_MODE_PARAMETER))) {
      return Future.succeededFuture();
    }

    Promise<Void> promise = Promise.promise();
    PubSubClientUtils.registerModule(new OkapiConnectionParams(headers, vertx))
      .whenComplete((registrationAr, throwable) -> {
        if (throwable == null) {
          LOGGER.info("Module was successfully registered as publisher/subscriber in mod-pubsub");
          promise.complete();
        } else {
          LOGGER.error("Error during module registration in mod-pubsub", throwable);
          promise.fail(throwable);
        }
      });
    return promise.future();
  }
}
