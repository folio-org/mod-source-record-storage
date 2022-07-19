package org.folio.services.handlers.actions;

import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_HOLDINGS_RECORD_MODIFIED_READY_FOR_POST_PROCESSING;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_HOLDINGS_RECORD_UPDATED;
import static org.folio.rest.jaxrs.model.EntityType.MARC_HOLDINGS;

import io.vertx.core.Vertx;
import org.springframework.beans.factory.annotation.Autowired;

import org.folio.rest.jaxrs.model.EntityType;
import org.folio.services.RecordService;
import org.folio.services.caches.MappingParametersSnapshotCache;

public class MarcHoldingsUpdateModifyEventHandler extends AbstractUpdateModifyEventHandler {

  @Autowired
  public MarcHoldingsUpdateModifyEventHandler(RecordService recordService,
                                              MappingParametersSnapshotCache mappingParametersCache,
                                              Vertx vertx) {
    super(recordService, mappingParametersCache, vertx);
  }

  @Override
  public boolean isPostProcessingNeeded() {
    return true;
  }

  @Override
  protected boolean isHridFillingNeeded() {
    return true;
  }

  @Override
  public String getPostProcessingInitializationEventType() {
    return DI_SRS_MARC_HOLDINGS_RECORD_MODIFIED_READY_FOR_POST_PROCESSING.value();
  }

  @Override
  protected String getNextEventType() {
    return DI_SRS_MARC_HOLDINGS_RECORD_UPDATED.value();
  }

  @Override
  protected EntityType modifiedEntityType() {
    return MARC_HOLDINGS;
  }
}
