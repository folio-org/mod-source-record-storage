package org.folio.services.handlers.actions;

import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_MODIFIED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_MODIFIED_READY_FOR_POST_PROCESSING;
import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;

import io.vertx.core.Vertx;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.folio.rest.jaxrs.model.EntityType;
import org.folio.services.RecordService;
import org.folio.services.caches.MappingParametersSnapshotCache;

@Component
public class MarcBibUpdateModifyEventHandler extends AbstractUpdateModifyEventHandler {

  @Autowired
  public MarcBibUpdateModifyEventHandler(RecordService recordService, MappingParametersSnapshotCache mappingParametersCache,
                                         Vertx vertx) {
    super(recordService, mappingParametersCache, vertx);
  }

  @Override
  public boolean isPostProcessingNeeded() {
    return true;
  }

  @Override
  public String getPostProcessingInitializationEventType() {
    return DI_SRS_MARC_BIB_RECORD_MODIFIED_READY_FOR_POST_PROCESSING.value();
  }

  @Override
  protected String getNextEventType() {
    return DI_SRS_MARC_BIB_RECORD_MODIFIED.value();
  }

  @Override
  protected EntityType modifiedEntityType() {
    return MARC_BIBLIOGRAPHIC;
  }
}
