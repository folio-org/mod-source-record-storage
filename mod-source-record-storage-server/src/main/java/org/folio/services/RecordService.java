package org.folio.services;

import io.vertx.core.Future;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.RecordCollection;
import org.folio.rest.jaxrs.model.SourceRecordCollection;

import java.util.Optional;

/**
 * Record Service
 */
public interface RecordService {

  /**
   * Searches for records
   *
   * @param query  query from URL
   * @param offset starting index in a list of results
   * @param limit  limit of records for pagination
   * @param tenantId tenant id
   * @return future with {@link RecordCollection}
   */
  Future<RecordCollection> getRecords(String query, int offset, int limit, String tenantId);

  /**
   * Searches for record by id
   *
   * @param id Record id
   * @param tenantId tenant id
   * @return future with optional {@link Record}
   */
  Future<Optional<Record>> getRecordById(String id, String tenantId);

  /**
   * Saves record
   *
   * @param record Record to save
   * @param tenantId tenant id
   * @return future with true if succeeded
   */
  Future<Boolean> saveRecord(Record record, String tenantId);

  /**
   * Updates record with given id
   *
   * @param record Record to update
   * @param tenantId tenant id
   * @return future with true if succeeded
   */
  Future<Boolean> updateRecord(Record record, String tenantId);

  /**
   * Searches for source records
   *
   * @param query  query from URL
   * @param offset starting index in a list of results
   * @param limit  limit of records for pagination
   * @param deletedRecords indicates to return records marked as deleted or not
   * @param tenantId tenant id
   * @return future with {@link SourceRecordCollection}
   */
  Future<SourceRecordCollection> getSourceRecords(String query, int offset, int limit, boolean deletedRecords, String tenantId);
}
