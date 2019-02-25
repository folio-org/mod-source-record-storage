package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.folio.rest.annotations.Validate;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.RawRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.TenantAttributes;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.tools.utils.TenantTool;
import org.folio.services.RecordService;
import org.folio.services.RecordServiceImpl;

import javax.ws.rs.core.Response;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

public class ModTenantAPI extends TenantAPI {

  private static final Logger LOGGER = LoggerFactory.getLogger(ModTenantAPI.class);
  static final String LOAD_SAMPLE_PARAMETER = "loadSample";
  private static final String CREATE_STUB_SNAPSHOT_SQL = "templates/db_scripts/create_stub_snapshot.sql";
  private static final String TENANT_PLACEHOLDER = "${myuniversity}";
  private static final String MODULE_PLACEHOLDER = "${mymodule}";
  private static final String JSON_EXTENSION = ".json";

  private RecordService recordService;

  public ModTenantAPI(Vertx vertx, String tenantId) {
    String calculatedTenantId = TenantTool.calculateTenantId(tenantId);
    this.recordService = new RecordServiceImpl(vertx, calculatedTenantId);
  }

  @Validate
  @Override
  public void postTenant(TenantAttributes entity, Map<String, String> headers, Handler<AsyncResult<Response>> handlers, Context context) {
    super.postTenant(entity, headers, ar -> {
      if (ar.failed()) {
        handlers.handle(ar);
      } else {
        setLoadSampleParameter(entity, context)
          .compose(v -> createStubSnapshot(headers, context, entity))
          .compose(v -> createStubData(entity))
          .setHandler(event -> handlers.handle(ar));
      }
    }, context);
  }

  private Future<Void> setLoadSampleParameter(TenantAttributes attributes, Context context) {
    context.put(LOAD_SAMPLE_PARAMETER, isLoadSample(attributes));
    return Future.succeededFuture();
  }

  private Future<List<String>> createStubSnapshot(Map<String, String> headers, Context context, TenantAttributes attributes) {
    try {
      if (!isLoadSample(attributes)) {
        LOGGER.info("Module is being deployed in production mode");
        return Future.succeededFuture();
      }

      InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream(CREATE_STUB_SNAPSHOT_SQL);

      if (inputStream == null) {
        LOGGER.info("Module is being deployed in test mode, but stub snapshot was not created: no resources found: {}", CREATE_STUB_SNAPSHOT_SQL);
        return Future.succeededFuture();
      }

      String sqlScript = IOUtils.toString(inputStream, StandardCharsets.UTF_8.name());
      if (StringUtils.isBlank(sqlScript)) {
        return Future.succeededFuture();
      }

      String tenantId = TenantTool.calculateTenantId((String) headers.get("x-okapi-tenant"));
      String moduleName = PostgresClient.getModuleName();

      sqlScript = sqlScript.replace(TENANT_PLACEHOLDER, tenantId).replace(MODULE_PLACEHOLDER, moduleName);

      Future<List<String>> future = Future.future();
      PostgresClient.getInstance(context.owner()).runSQLFile(sqlScript, false, future);

      LOGGER.info("Module is being deployed in test mode, stub snapshot will be created. Check the server log for details.");

      return future;
    } catch (IOException e) {
      return Future.failedFuture(e);
    }
  }

  private Future<Void> createStubData(TenantAttributes attributes) { //NOSONAR
    Future<Void> future = Future.future();
    if (isLoadSample(attributes)) {
      try {
        ClassLoader classLoader = getClass().getClassLoader();
        File sampleDir = new File(Objects.requireNonNull(classLoader.getResource("sampledata")).getFile());
        List<Future> futures = new ArrayList<>();
        Files.walk(sampleDir.toPath()).forEach(file -> { //NOSONAR
          if (file.toFile().getName().endsWith(JSON_EXTENSION)) {
            Record record = new Record();
            record.setId(file.toFile().getName().split(JSON_EXTENSION)[0]);
            try {
              String content = new String(Files.readAllBytes(file), StandardCharsets.UTF_8);
              record.setParsedRecord(new ParsedRecord().withId(UUID.randomUUID().toString()).withContent(content));
              record.setRawRecord(new RawRecord().withId(UUID.randomUUID().toString()).withContent(content));
              record.setRecordType(Record.RecordType.MARC);
              record.setSnapshotId("00000000-0000-0000-0000-000000000000");
              record.setGeneration("0");
              futures.add(recordService.saveRecord(record).setHandler(h -> {
                if (h.succeeded()) {
                  LOGGER.info("Sample Source Record was successfully saved. Record ID: {}", record.getId());
                } else {
                  LOGGER.error("Error during saving Sample Source Record with ID: " + record.getId(), h.cause());
                }
              }));
            } catch (IOException e) {
              LOGGER.error("Error during reading sample source records", e);
            }
          }
        });
        CompositeFuture.all(futures).setHandler(r -> future.complete());
      } catch (Exception e) {
        LOGGER.error("Error during loading sample source records", e);
        future.fail(e);
      }
    } else {
      future.complete();
    }
    return future;
  }

  private boolean isLoadSample(TenantAttributes attributes) {
    if (attributes == null) {
      return false;
    }
    return attributes.getParameters()
      .stream()
      .anyMatch(p -> p.getKey().equals(LOAD_SAMPLE_PARAMETER)
        && p.getValue().equals(Boolean.TRUE.toString()));
  }

}
