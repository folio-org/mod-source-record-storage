package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import org.folio.rest.jaxrs.model.AsyncMigrationJobInitRq;
import org.folio.rest.jaxrs.resource.SourceStorageMigrationsJobs;

import javax.ws.rs.core.Response;
import java.util.Map;

public class SourceStorageMigrationsJobsImpl implements SourceStorageMigrationsJobs {


  @Override
  public void postSourceStorageMigrationsJobs(AsyncMigrationJobInitRq entity, Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {

  }

  @Override
  public void getSourceStorageMigrationsJobsById(String id, Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {

  }
}
