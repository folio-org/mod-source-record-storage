package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.folio.dataimport.util.ExceptionHelper;
import org.folio.rest.jaxrs.model.AsyncMigrationJobInitRq;
import org.folio.rest.jaxrs.resource.SourceStorageMigrationsJobs;
import org.folio.services.migrations.AsyncMigrationJobService;
import org.folio.spring.SpringContextUtil;
import org.springframework.beans.factory.annotation.Autowired;

import javax.ws.rs.core.Response;
import java.util.Map;

import static java.lang.String.format;

public class SourceStorageMigrationsJobsImpl implements SourceStorageMigrationsJobs {

  private static final String NOT_FOUND_MSG = "Async migration job with id '%s' was not found";

  private final String tenantId;

  @Autowired
  private AsyncMigrationJobService asyncMigrationJobService;

  public SourceStorageMigrationsJobsImpl(Vertx vertx, String tenantId) { //NOSONAR
    SpringContextUtil.autowireDependencies(this, Vertx.currentContext());
    this.tenantId = tenantId;
  }

  @Override
  public void postSourceStorageMigrationsJobs(AsyncMigrationJobInitRq entity, Map<String, String> okapiHeaders,
                                              Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    asyncMigrationJobService.runAsyncMigration(entity, tenantId)
      .map(PostSourceStorageMigrationsJobsResponse::respond202WithApplicationJson)
      .map(Response.class::cast)
      .otherwise(ExceptionHelper::mapExceptionToResponse)
      .onComplete(asyncResultHandler);
  }

  @Override
  public void getSourceStorageMigrationsJobsById(String id, Map<String, String> okapiHeaders,
                                                 Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    asyncMigrationJobService.getById(id, tenantId)
      .map(migrationJobOptional -> migrationJobOptional
        .map(GetSourceStorageMigrationsJobsByIdResponse::respond200WithApplicationJson)
        .orElseGet(() -> GetSourceStorageMigrationsJobsByIdResponse.respond404WithTextPlain(format(NOT_FOUND_MSG, id))))
      .map(Response.class::cast)
      .otherwise(ExceptionHelper::mapExceptionToResponse)
      .onComplete(asyncResultHandler);
  }
}
