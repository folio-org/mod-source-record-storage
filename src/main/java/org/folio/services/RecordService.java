package org.folio.services;

import io.vertx.core.Future;

import java.util.List;
import java.util.Optional;
import org.folio.rest.jaxrs.model.Record;

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
   * @return future with list of {@link Record}
   */
  Future<List<Record>> getRecords(String query, int offset, int limit);

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
}
