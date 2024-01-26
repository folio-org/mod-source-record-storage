package org.folio.services.handlers.actions;

import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_AUTHORITY_RECORD_MODIFIED_READY_FOR_POST_PROCESSING;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_AUTHORITY_RECORD_UPDATED;
import static org.folio.rest.jaxrs.model.EntityType.MARC_AUTHORITY;

import io.vertx.core.Vertx;

import org.folio.services.SnapshotService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.folio.rest.jaxrs.model.EntityType;
import org.folio.services.RecordService;
import org.folio.services.caches.MappingParametersSnapshotCache;

@Component
public class MarcAuthorityUpdateModifyEventHandler extends AbstractUpdateModifyEventHandler {

  @Autowired
  public MarcAuthorityUpdateModifyEventHandler(RecordService recordService,
                                               SnapshotService snapshotService,
                                               MappingParametersSnapshotCache mappingParametersCache,
                                               Vertx vertx) {
    super(recordService, snapshotService, mappingParametersCache, vertx);
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
    return DI_SRS_MARC_AUTHORITY_RECORD_MODIFIED_READY_FOR_POST_PROCESSING.value();
  }

  @Override
  protected String getUpdateEventType() {
    return DI_SRS_MARC_AUTHORITY_RECORD_UPDATED.value();
  }

  @Override
  protected EntityType modifiedEntityType() {
    return MARC_AUTHORITY;
  }
}
