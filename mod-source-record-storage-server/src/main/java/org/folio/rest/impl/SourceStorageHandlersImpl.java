package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.folio.DataImportEventPayload;
import org.folio.processing.events.EventManager;
import org.folio.processing.events.utils.ZIPArchiver;
import org.folio.processing.mapping.MappingManager;
import org.folio.processing.mapping.mapper.reader.record.MarcBibReaderFactory;
import org.folio.rest.jaxrs.resource.SourceStorageHandlers;
import org.folio.rest.util.OkapiConnectionParams;
import org.folio.services.handlers.InstancePostProcessingEventHandler;
import org.folio.services.UpdateRecordEventHandlingService;
import org.folio.services.handlers.MarcBibliographicMatchEventHandler;
import org.folio.services.handlers.actions.ModifyRecordEventHandler;
import org.folio.spring.SpringContextUtil;
import org.springframework.beans.factory.annotation.Autowired;

import javax.ws.rs.core.Response;
import java.util.Map;

public class SourceStorageHandlersImpl implements SourceStorageHandlers {

  private static final Logger LOG = LoggerFactory.getLogger(SourceStorageHandlersImpl.class);

  @Autowired
  private InstancePostProcessingEventHandler instancePostProcessingEventHandler;
  @Autowired
  private ModifyRecordEventHandler modifyRecordEventHandler;
  @Autowired
  private UpdateRecordEventHandlingService updateRecordEventHandlingService;
  @Autowired
  private MarcBibliographicMatchEventHandler marcBibliographicMatchEventHandler;

  public SourceStorageHandlersImpl(Vertx vertx, String tenantId) { //NOSONAR
    SpringContextUtil.autowireDependencies(this, Vertx.currentContext());

    MappingManager.registerReaderFactory(new MarcBibReaderFactory());
    EventManager.registerEventHandler(instancePostProcessingEventHandler);
    EventManager.registerEventHandler(modifyRecordEventHandler);
    EventManager.registerEventHandler(marcBibliographicMatchEventHandler);
  }

  @Override
  public void postSourceStorageHandlersDataImport(String entity, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        LOG.debug("Data import process event was received: {}", entity);
        asyncResultHandler.handle(Future.succeededFuture(PostSourceStorageHandlersDataImportResponse.respond204()));
        // response status doesn't depend on event handling result
        DataImportEventPayload eventPayload = new JsonObject(ZIPArchiver.unzip(entity)).mapTo(DataImportEventPayload.class);
        EventManager.handleEvent(eventPayload);
      } catch (Exception e) {
        LOG.error("Error of data import event handling");
      }
    });
  }

  @Override
  public void postSourceStorageHandlersUpdatedRecord(String entity, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      LOG.debug("Received QM_RECORD_UPDATED event: {}", entity);
      asyncResultHandler.handle(Future.succeededFuture(PostSourceStorageHandlersUpdatedRecordResponse.respond204()));
      // response status doesn't depend on event handling result
      updateRecordEventHandlingService.handleEvent(entity, new OkapiConnectionParams(okapiHeaders, vertxContext.owner()));
    });
  }
}
