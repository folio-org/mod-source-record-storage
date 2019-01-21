package org.folio.services;

import io.vertx.core.Future;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.RecordCollection;
import org.folio.rest.jaxrs.model.ResultCollection;

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
   * @return future with {@link RecordCollection}
   */
  Future<RecordCollection> getRecords(String query, int offset, int limit);

  /**
   * Searches for record by id
   *
   * @param id Record id
   * @return future with optional {@link Record}
   */
  Future<Optional<Record>> getRecordById(String id);

  /**
   * Saves record
   *
   * @param record Record to save
   * @return future with true if succeeded
   */
  Future<Boolean> saveRecord(Record record);

  /**
   * Updates record with given id
   *
   * @param record Record to update
   * @return future with true if succeeded
   */
  Future<Boolean> updateRecord(Record record);

  /**
   * Deletes record by id
   *
   * @param id record id
   * @return future with true if succeeded
   */
  Future<Boolean> deleteRecord(String id);

  /**
   * Searches for results
   *
   * @param query  query from URL
   * @param offset starting index in a list of results
   * @param limit  limit of records for pagination
   * @return future with {@link ResultCollection}
   */
  Future<ResultCollection> getResults(String query, int offset, int limit);
}
