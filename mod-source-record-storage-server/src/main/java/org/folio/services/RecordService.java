package org.folio.services;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

import io.vertx.sqlclient.Row;
import org.folio.dao.util.ExternalIdType;
import org.folio.dao.util.RecordType;
import org.folio.rest.jaxrs.model.ParsedRecordDto;
import org.folio.rest.jaxrs.model.ParsedRecordsBatchResponse;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.RecordCollection;
import org.folio.rest.jaxrs.model.RecordsBatchResponse;
import org.folio.rest.jaxrs.model.SourceRecord;
import org.folio.rest.jaxrs.model.SourceRecordCollection;
import org.jooq.Condition;
import org.jooq.OrderField;

import io.reactivex.Flowable;
import io.vertx.core.Future;

public interface RecordService {

  /**
   * Searches for {@link Record} by {@link Condition} and ordered by collection of {@link OrderField} with offset and limit
   *
   * @param condition   query where condition
   * @param recordType  record type
   * @param orderFields fields to order by
   * @param offset      starting index in a list of results
   * @param limit       limit of records for pagination
   * @param tenantId    tenant id
   * @return {@link Future} of {@link RecordCollection}
   */
  Future<RecordCollection> getRecords(Condition condition, RecordType recordType, Collection<OrderField<?>> orderFields, int offset, int limit, String tenantId);

  /**
   * Stream {@link Record} by {@link Condition} and ordered by collection of {@link OrderField} with offset and limit
   *
   * @param condition   query where condition
   * @param recordType  record type
   * @param orderFields fields to order by
   * @param offset      starting index in a list of results
   * @param limit       limit of records for pagination
   * @param tenantId    tenant id
   * @return {@link Flowable} of {@link Record}
   */
  Flowable<Record> streamRecords(Condition condition, RecordType recordType, Collection<OrderField<?>> orderFields, int offset, int limit, String tenantId);

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
   * @param record   record to save
   * @param tenantId tenant id
   * @return future with saved Record
   */
  Future<Record> saveRecord(Record record, String tenantId);

  /**
   * Saves collection of records
   *
   * @param recordsCollection records to save
   * @param tenantId          tenant id
   * @return future with response containing list of successfully saved records and error messages for records that were not saved
   */
  Future<RecordsBatchResponse> saveRecords(RecordCollection recordsCollection, String tenantId);

  /**
   * Updates record with given id
   *
   * @param record   record to update
   * @param tenantId tenant id
   * @return future with updated Record
   */
  Future<Record> updateRecord(Record record, String tenantId);

  /**
   * Searches for {@link SourceRecord} by {@link Condition} and ordered by order fields with offset and limit
   *
   * @param condition   query where condition
   * @param recordType  record type
   * @param orderFields fields to order by
   * @param offset      starting index in a list of results
   * @param limit       limit of records for pagination
   * @param tenantId    tenant id
   * @return future with {@link SourceRecordCollection}
   */
  Future<SourceRecordCollection> getSourceRecords(Condition condition, RecordType recordType, Collection<OrderField<?>> orderFields, int offset, int limit, String tenantId);

  /**
   * Stream {@link SourceRecord} by {@link Condition} and ordered by order fields with offset and limit
   *
   * @param condition   query where condition
   * @param recordType  record type
   * @param orderFields fields to order by
   * @param offset      starting index in a list of results
   * @param limit       limit of records for pagination
   * @param tenantId    tenant id
   * @return {@link Flowable} of {@link SourceRecord}
   */
  Flowable<SourceRecord> streamSourceRecords(Condition condition, RecordType recordType, Collection<OrderField<?>> orderFields, int offset, int limit, String tenantId);

  /**
   * Stream [instanceId, totalCount]  of the marc record by search expressions with offset and limit
   *
   * @param leaderExpression    expression to search by the leader of marc records
   * @param fieldsExpression    expression to search by the marc fields of marc records
   * @param deleted             deleted
   * @param suppress            suppress from discovery
   * @param offset              starting index in a list of results
   * @param limit               limit of records for pagination
   * @param tenantId            tenant id
   * @return {@link Flowable} of {@link Record id}
   */
  Flowable<Row> streamMarcRecordIds(String leaderExpression, String fieldsExpression, Boolean deleted, Boolean suppress, Integer offset, Integer limit, String tenantId);
  /**
   * Searches for {@link SourceRecord} where id in a list of ids defined by id type. i.e. INSTANCE or RECORD
   *
   * @param ids            list of ids
   * @param externalIdType id type
   * @param recordType     record type
   * @param deleted        filter by state DELETED or leader record status d, s, or x
   * @param tenantId       tenant id
   * @return future with {@link SourceRecordCollection}
   */
  Future<SourceRecordCollection> getSourceRecords(List<String> ids, ExternalIdType externalIdType, RecordType recordType, Boolean deleted, String tenantId);

  /**
   * Searches for source record by id via specific id type
   *
   * @param id             for searching
   * @param externalIdType search type
   * @param tenantId       tenant id
   * @return future with optional source record
   */
  Future<Optional<SourceRecord>> getSourceRecordById(String id, ExternalIdType externalIdType, String tenantId);

  /**
   * Update parsed records from collection of records and external relations ids in one transaction
   *
   * @param recordCollection collection of records from which parsed records will be updated
   * @param tenantId         tenant id
   * @return future with response containing list of successfully updated records and error messages for records that were not updated
   */
  Future<ParsedRecordsBatchResponse> updateParsedRecords(RecordCollection recordCollection, String tenantId);

  /**
   * Searches for Record either by SRS id or external relation id
   *
   * @param id             either SRS id or external relation id
   * @param externalIdType specifies of external relation id type
   * @param tenantId       tenant id
   * @return future with {@link Record}
   */
  Future<Record> getFormattedRecord(String id, ExternalIdType externalIdType, String tenantId);

  /**
   * Change suppress from discovery flag for record by external relation id
   *
   * @param id             id
   * @param externalIdType external id type
   * @param suppress       suppress from discovery
   * @param tenantId       tenant id
   * @return future with true if succeeded
   */
  Future<Boolean> updateSuppressFromDiscoveryForRecord(String id, ExternalIdType externalIdType, Boolean suppress, String tenantId);

  /**
   * Deletes records associated with specified snapshot and snapshot itself
   *
   * @param snapshotId snapshot id
   * @param tenantId   tenant id
   * @return future with true if succeeded
   */
  Future<Boolean> deleteRecordsBySnapshotId(String snapshotId, String tenantId);

  /**
   * Creates new updated Record with incremented generation linked to a new Snapshot, and sets OLD status to the "old" Record,
   * no data is deleted as a result of the update
   *
   * @param parsedRecordDto parsed record DTO containing updates to parsed record
   * @param snapshotId      snapshot id to which new Record should be linked
   * @param tenantId        tenant id
   * @return future with updated Record
   */
  Future<Record> updateSourceRecord(ParsedRecordDto parsedRecordDto, String snapshotId, String tenantId);

}
