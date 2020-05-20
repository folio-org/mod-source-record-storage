package org.folio.services;

import org.folio.dao.query.ParsedRecordQuery;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.ParsedRecordCollection;
import org.folio.rest.jaxrs.model.ParsedRecordsBatchResponse;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.RecordCollection;

import io.vertx.core.Future;

/**
 * {@link ParsedRecord} service
 */
public interface ParsedRecordService extends EntityService<ParsedRecord, ParsedRecordCollection, ParsedRecordQuery> {

  /**
   * Update parsed records from collection of records and external relations ids
   * in one transaction
   *
   * @param recordCollection collection of records from which parsed records will
   *                         be updated
   * @param tenantId         tenant id
   * @return future with response containing list of successfully updated records
   *         and error messages for records that were not updated
   */
  public Future<ParsedRecordsBatchResponse> updateParsedRecords(RecordCollection recordCollection, String tenantId);

  /**
   * Updates {@link ParsedRecord} in the db
   *
   * @param record   record dto from which {@link ParsedRecord} will be updated
   * @param tenantId tenant id
   * @return future with updated ParsedRecord
   */
  public Future<ParsedRecord> updateParsedRecord(Record record, String tenantId);

}