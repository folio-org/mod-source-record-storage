package org.folio.dao;

import io.vertx.core.Future;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.Result;

import java.util.List;
import java.util.Optional;

/**
 * Data access object for {@link Record}
 */
public interface RecordDao {

  /**
   * Searches for {@link Record} in the db view
   *
   * @param query  query string to filter records based on matching criteria in fields
   * @param offset starting index in a list of results
   * @param limit  maximum number of results to return
   * @return future with list of {@link Record}
   */
  Future<List<Record>> getRecords(String query, int offset, int limit);

  /**
   * Searches for {@link Record} by id
   *
   * @param id Record id
   * @return future with optional {@link Record}
   */
  Future<Optional<Record>> getRecordById(String id);

  /**
   * Saves {@link Record} to the db
   *
   * @param record {@link Record} to save
   * @return future with true if succeeded
   */
  Future<Boolean> saveRecord(Record record);

  /**
   * Updates {{@link Record} in the db
   *
   * @param record {@link Record} to update
   * @return future with true if succeeded
   */
  Future<Boolean> updateRecord(Record record);

  /**
   * Deletes {@link Record} from the db
   *
   * @param id id of the {@link Record} to delete
   * @return future with true if succeeded
   */
  Future<Boolean> deleteRecord(String id);

  /**
   * Searches for {@link Result} in the db view
   *
   * @param query  query string to filter results based on matching criteria in fields
   * @param offset starting index in a list of results
   * @param limit  maximum number of results to return
   * @return future with list of {@link Result}
   */
  Future<List<Result>> getResults(String query, int offset, int limit);

}
