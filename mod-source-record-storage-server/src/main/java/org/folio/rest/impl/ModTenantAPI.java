package org.folio.rest.impl;

import static org.apache.commons.lang3.StringUtils.EMPTY;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.Response;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.folio.liquibase.LiquibaseUtil;
import org.folio.rest.annotations.Validate;
import org.folio.rest.jaxrs.model.ExternalIdsHolder;
import org.folio.rest.jaxrs.model.Parameter;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.RawRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.rest.jaxrs.model.Snapshot.Status;
import org.folio.rest.jaxrs.model.TenantAttributes;
import org.folio.rest.tools.utils.TenantTool;
import org.folio.rest.util.OkapiConnectionParams;
import org.folio.services.LbRecordService;
import org.folio.services.LbSnapshotService;
import org.folio.services.RecordService;
import org.folio.services.SnapshotService;
import org.folio.spring.SpringContextUtil;
import org.folio.util.pubsub.PubSubClientUtils;
import org.springframework.beans.factory.annotation.Autowired;

import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

@SuppressWarnings("squid:CallToDeprecatedMethod")
public class ModTenantAPI extends TenantAPI {

  private static final Logger LOGGER = LoggerFactory.getLogger(ModTenantAPI.class);

  private static final String TEST_MODE_PARAMETER = "testMode";
  private static final String SAMPLE_DATA = "sampledata/sampleMarcRecords.json";

  static final String LOAD_SAMPLE_PARAMETER = "loadSample";

  static final Snapshot STUB_SNAPSHOT = new Snapshot()
    .withJobExecutionId("00000000-0000-0000-0000-000000000000")
    .withStatus(Status.COMMITTED)
    .withProcessingStartedDate(new Date(1546351314000L));

  @Autowired
  private LbRecordService recordService;

  @Autowired
  private LbSnapshotService snapshotService;

  @Autowired
  private RecordService legacyRecordService;

  @Autowired
  private SnapshotService legacySnapshotService;

  private String tenantId;

  public ModTenantAPI(Vertx vertx, String tenantId) { //NOSONAR
    SpringContextUtil.autowireDependencies(this, Vertx.currentContext());
    this.tenantId = TenantTool.calculateTenantId(tenantId);
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
            .compose(v -> createStubSnapshot(context, entity))
            .compose(v -> createStubData(entity))
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

  private Future<Void> createStubSnapshot(Context context, TenantAttributes attributes) {
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
      // temporary to keep legacy tests passing
      legacySnapshotService.saveSnapshot(STUB_SNAPSHOT, tenantId).onComplete(legacy -> {
        if (legacy.failed()) {
          promise.fail(legacy.cause());
        }
        promise.complete();
      });
    });
    LOGGER.info("Module is being deployed in test mode, stub snapshot will be created. Check the server log for details.");

    return promise.future();
  }

  private Future<Void> createStubData(TenantAttributes attributes) {
    Promise<Void> promise = Promise.promise();
    if (Boolean.parseBoolean(getTenantAttributesParameter(attributes, LOAD_SAMPLE_PARAMETER))) {
      try {
        InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream(SAMPLE_DATA);
        if (inputStream == null) {
          LOGGER.info("Module is being deployed in test mode, but stub data was not populated: no resources found: {}", SAMPLE_DATA);
          return Future.succeededFuture();
        }
        String sampleData = IOUtils.toString(inputStream, StandardCharsets.UTF_8.name());
        if (StringUtils.isBlank(sampleData)) {
          return Future.succeededFuture();
        }
        JsonArray marcRecords = new JsonArray(sampleData);
        @SuppressWarnings("squid:S3740")
        List<Future> futures = new ArrayList<>(marcRecords.size());
        marcRecords.forEach(jsonRecord -> {
          Record record = new Record();
          JsonObject marcRecordJson = JsonObject.mapFrom(jsonRecord);
          String recordUUID = marcRecordJson.getString("id");
          String instanceUUID = marcRecordJson.getString("instanceId");
          JsonObject content = marcRecordJson.getJsonObject("content");
          record
            .withId(recordUUID)
            .withMatchedId(recordUUID)
            .withSnapshotId("00000000-0000-0000-0000-000000000000")
            .withRecordType(Record.RecordType.MARC)
            .withState(Record.State.ACTUAL)
            .withGeneration(0)
            .withParsedRecord(new ParsedRecord().withId(recordUUID).withContent(content))
            .withRawRecord(new RawRecord().withId(recordUUID).withContent(content.toString()))
            .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(instanceUUID));
          Promise<Void> helperPromise = Promise.promise();

          recordService.saveRecord(record, tenantId).onComplete(h -> {
            if (h.succeeded()) {
              LOGGER.info("Sample Source Record was successfully saved. Record ID: {}", record.getId());
            } else {
              LOGGER.error("Error during saving Sample Source Record with ID: " + record.getId(), h.cause());
            }
            // temporary to keep legacy tests passing
            legacyRecordService.saveRecord(record, tenantId).onComplete(h -> {
              if (h.succeeded()) {
                LOGGER.info("Sample Source Record was successfully saved. Record ID: {}", record.getId());
              } else {
                LOGGER.error("Error during saving Sample Source Record with ID: " + record.getId(), h.cause());
              }
              helperPromise.complete();
            });
          });

          futures.add(helperPromise.future());
        });
        CompositeFuture.all(futures).onComplete(r -> promise.complete());
      } catch (Exception e) {
        LOGGER.error("Error during loading sample source records", e);
        promise.fail(e);
      }
    } else {
      promise.complete();
    }
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
