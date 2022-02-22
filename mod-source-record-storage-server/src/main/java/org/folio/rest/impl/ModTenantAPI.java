package org.folio.rest.impl;

import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.liquibase.LiquibaseUtil;
import org.folio.rest.jaxrs.model.Parameter;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.rest.jaxrs.model.Snapshot.Status;
import org.folio.rest.jaxrs.model.TenantAttributes;
import org.folio.rest.tools.utils.TenantTool;
import org.folio.services.RecordCleanupService;
import org.folio.services.SnapshotService;
import org.folio.spring.SpringContextUtil;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Date;
import java.util.Map;

import static org.apache.commons.lang3.StringUtils.EMPTY;

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
  private RecordCleanupService recordCleanupService;

  private String tenantId;

  public ModTenantAPI(Vertx vertx, String tenantId) { //NOSONAR
    SpringContextUtil.autowireDependencies(this, Vertx.currentContext());
    this.tenantId = TenantTool.calculateTenantId(tenantId);
  }

  @Override
  Future<Integer> loadData(TenantAttributes attributes, String tenantId,
                           Map<String, String> headers, Context context) {
    return super.loadData(attributes, tenantId, headers, context)
      .compose(num -> {
        Vertx vertx = context.owner();
        LiquibaseUtil.initializeSchemaForTenant(vertx, tenantId);
        recordCleanupService.initialize(vertx, tenantId);
        return setLoadSampleParameter(attributes, context)
          .compose(v -> createStubSnapshot(attributes)).map(num);
      });
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
      .orElse(EMPTY);
  }
}
