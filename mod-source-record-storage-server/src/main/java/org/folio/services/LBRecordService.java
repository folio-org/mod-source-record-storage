package org.folio.services;

import org.folio.dao.query.RecordQuery;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.RecordCollection;
import org.folio.rest.jaxrs.model.RecordsBatchResponse;

import io.vertx.core.Future;

/**
 * {@link Record} service
 */
public interface LBRecordService extends EntityService<Record, RecordCollection, RecordQuery> {

  /**
   * Saves collection of records
   *
   * @param recordCollection records to save
   * @param tenantId         tenant id
   * @return future with response containing list of successfully saved records and error messages for records that were not saved
   */
  public Future<RecordsBatchResponse> saveRecords(RecordCollection recordCollection, String tenantId);

  /**
   * Searches for Record either by SRS id or external relation id
   *
   * @param externalIdIdentifier specifies of external relation id type
   * @param id                   either SRS id or external relation id
   * @param tenantId             tenant id
   * @return future with {@link Record}
   */
  public Future<Record> getFormattedRecord(String externalIdIdentifier, String id, String tenantId);

}