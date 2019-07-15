package org.folio.services;

import io.vertx.core.Future;
import org.folio.rest.jaxrs.model.*;

import java.util.Optional;

/**
 * Record Service
 */
public interface RecordService {

    /**
     * Searches for records
     *
     * @param query    query from URL
     * @param offset   starting index in a list of results
     * @param limit    limit of records for pagination
     * @param tenantId tenant id
     * @return future with {@link RecordCollection}
     */
    Future<RecordCollection> getRecords(String query, int offset, int limit, String tenantId);

    /**
     * Searches for record by id
     *
     * @param id       Record id
     * @param tenantId tenant id
     * @return future with optional {@link Record}
     */
    Future<Optional<Record>> getRecordById(String id, String tenantId);

    /**
     * Saves record
     *
     * @param record   Record to save
     * @param tenantId tenant id
     * @return future with true if succeeded
     */
    Future<Boolean> saveRecord(Record record, String tenantId);

  /**
   * Saves collection of records
   *
   * @param recordBatch Records to save
   * @param tenantId    tenant id
   * @return future with collection both saved records and errors messages if any records were not saved.
   */
  Future<RecordBatch> saveRecords(RecordBatch recordBatch, String tenantId);

    /**
     * Updates record with given id
     *
     * @param record   Record to update
     * @param tenantId tenant id
     * @return future with true if succeeded
     */
    Future<Boolean> updateRecord(Record record, String tenantId);

    /**
     * Searches for source records
     *
     * @param query          query from URL
     * @param offset         starting index in a list of results
     * @param limit          limit of records for pagination
     * @param deletedRecords indicates to return records marked as deleted or not
     * @param tenantId       tenant id
     * @return future with {@link SourceRecordCollection}
     */
    Future<SourceRecordCollection> getSourceRecords(String query, int offset, int limit, boolean deletedRecords, String tenantId);

    /**
     * Update parsed records
     *
     * @param parsedRecordCollection collection of parsed records to update
     * @param tenantId               tenant id
     * @return future with collection both updated records and errors messages if any records were not saved.
     */
    Future<ParsedRecordCollection> updateParsedRecords(ParsedRecordCollection parsedRecordCollection, String tenantId);

    /**
     * Searches for Record either by SRS id or Instance id
     *
     * @param identifier specifies whether search should be performed by SRS or Instance id
     * @param id         either SRS id or Instance id
     * @param tenantId   tenant id
     * @return future with {@link Record}
     */
    Future<Record> getFormattedRecord(SourceStorageFormattedRecordsIdGetIdentifier identifier, String id, String tenantId);

}
