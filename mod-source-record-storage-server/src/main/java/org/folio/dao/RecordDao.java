package org.folio.dao;

import io.vertx.core.Future;
import org.folio.dao.util.ExternalIdType;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.RecordCollection;
import org.folio.rest.jaxrs.model.SourceRecord;
import org.folio.rest.jaxrs.model.SourceRecordCollection;
import org.folio.rest.jaxrs.model.SuppressFromDiscoveryDto;

import java.util.Optional;

/**
 * Data access object for {@link Record}
 */
public interface RecordDao {

  /**
   * Searches for {@link Record} in the db view
   *
   * @param query    query string to filter records based on matching criteria in fields
   * @param offset   starting index in a list of results
   * @param limit    maximum number of results to return
   * @param tenantId tenant id
   * @return future with {@link RecordCollection}
   */
  Future<RecordCollection> getRecords(String query, int offset, int limit, String tenantId);

  /**
   * Searches for {@link Record} by id
   *
   * @param id       Record id
   * @param tenantId tenant id
   * @return future with optional {@link Record}
   */
  Future<Optional<Record>> getRecordById(String id, String tenantId);

  /**
   * Saves {@link Record} to the db
   *
   * @param record   {@link Record} to save
   * @param tenantId tenant id
   * @return future with saved Record
   */
  Future<Record> saveRecord(Record record, String tenantId);

  /**
   * Updates {{@link Record} in the db
   *
   * @param record   {@link Record} to update
   * @param tenantId tenant id
   * @return future with updated Record
   */
  Future<Record> updateRecord(Record record, String tenantId);

  /**
   * Searches for {@link SourceRecord} in the db view
   *
   * @param query          query string to filter results based on matching criteria in fields
   * @param offset         starting index in a list of results
   * @param limit          maximum number of results to return
   * @param deletedRecords indicates to return records marked as deleted or not
   * @param tenantId       tenant id
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
   * @param record record dto from which {@link ParsedRecord} will be updated
   * @param tenantId     tenant id
   * @return future with updated ParsedRecord
   */
  Future<ParsedRecord> updateParsedRecord(Record record, String tenantId);

  /**
   * Searches for {@link Record} by id of external entity which was created from desired record
   *
   * @param externalId external relation id
   * @param externalIdType external id type
   * @param tenantId   tenant id
   * @return future with optional {@link Record}
   */
  Future<Optional<Record>> getRecordByExternalId(String externalId, ExternalIdType externalIdType, String tenantId);

  /**
   * Searches for {@link SourceRecord} by id if externalIdType is empty. If no, searches by id "externalIdsHolder"-field for externalIdType name
   * @param id - searching id
   * @param externalIdType - field name for "externalIdsHolder"
   * @param tenantId - tenant id
   * @return future with optional {@link SourceRecord}
   */
  Future<Optional<SourceRecord>> getSourceRecord(String id, ExternalIdType externalIdType, String tenantId);

  /**
   * Change suppress from discovery flag for record by external relation id
   *
   * @param suppressFromDiscoveryDto - dto that contains new value and id
   * @param tenantId                 - tenant id
   * @return - future with true if succeeded
   */
  Future<Boolean> updateSuppressFromDiscoveryForRecord(SuppressFromDiscoveryDto suppressFromDiscoveryDto, String tenantId);

  /**
   * Deletes in transaction all records associated with specified snapshot and snapshot itself
   *
   * @param snapshotId snapshot id
   * @param tenantId   tenant id
   * @return - future with true if succeeded
   */
  Future<Boolean> deleteRecordsBySnapshotId(String snapshotId, String tenantId);

}
