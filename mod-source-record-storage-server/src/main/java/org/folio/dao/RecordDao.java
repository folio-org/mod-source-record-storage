package org.folio.dao;

import io.vertx.core.Future;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.ParsedRecordCollection;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.RecordCollection;
import org.folio.rest.jaxrs.model.SourceRecord;
import org.folio.rest.jaxrs.model.SourceRecordCollection;

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
   * @param tenantId tenant id
   * @return future with {@link RecordCollection}
   */
  Future<RecordCollection> getRecords(String query, int offset, int limit, String tenantId);

  /**
   * Searches for {@link Record} by id
   *
   * @param id Record id
   * @param tenantId tenant id
   * @return future with optional {@link Record}
   */
  Future<Optional<Record>> getRecordById(String id, String tenantId);

  /**
   * Saves {@link Record} to the db
   *
   * @param record {@link Record} to save
   * @param tenantId tenant id
   * @return future with true if succeeded
   */
  Future<Boolean> saveRecord(Record record, String tenantId);

  /**
   * Updates {{@link Record} in the db
   *
   * @param record {@link Record} to update
   * @param tenantId tenant id
   * @return future with true if succeeded
   */
  Future<Boolean> updateRecord(Record record, String tenantId);

  /**
   * Searches for {@link SourceRecord} in the db view
   *
   * @param query  query string to filter results based on matching criteria in fields
   * @param offset starting index in a list of results
   * @param limit  maximum number of results to return
   * @param deletedRecords indicates to return records marked as deleted or not
   * @param tenantId tenant id
   * @return future with {@link SourceRecordCollection}
   */
  Future<SourceRecordCollection> getSourceRecords(String query, int offset, int limit, boolean deletedRecords, String tenantId);

  /**
   * Increments generation in case a record with the same matchedId exists
   * and the snapshot it is linked to is COMMITTED before the processing of the current one started
   *
   * @param record   - record
   * @param tenantId - tenant id
   * @return future with generation
   */
  Future<Integer> calculateGeneration(Record record, String tenantId);

  /**
   * Updates {@link ParsedRecord} in the db
   *
   * @param parsedRecord {@link ParsedRecord} to update
   * @param recordType type of ParsedRecord
   * @param tenantId tenant id
   * @return future with true if succeeded
   */
  Future<Boolean> updateParsedRecord(ParsedRecord parsedRecord, ParsedRecordCollection.RecordType recordType, String tenantId);

  /**
   * Searches for {@link Record} by instance id
   *
   * @param instanceId Instance id
   * @param tenantId tenant id
   * @return future with optional {@link Record}
   */
  Future<Optional<Record>> getRecordByInstanceId(String instanceId, String tenantId);

}
