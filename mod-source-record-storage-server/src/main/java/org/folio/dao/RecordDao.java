package org.folio.dao;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import org.folio.dao.util.ExternalIdType;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.RecordCollection;
import org.folio.rest.jaxrs.model.SourceRecord;
import org.folio.rest.jaxrs.model.SourceRecordCollection;
import org.jooq.Condition;
import org.jooq.OrderField;

import io.github.jklingsporn.vertx.jooq.classic.reactivepg.ReactiveClassicGenericQueryExecutor;
import io.vertx.core.Future;

/**
 * Data access object for {@link Record}
 */
public interface RecordDao {

  /**
   * Searches for {@link Record} by {@link Condition} and ordered by collection of {@link OrderField} with offset and limit
   *
   * @param condition   query where condition
   * @param orderFields fields to order by
   * @param offset      starting index in a list of results
   * @param limit       limit of records for pagination
   * @param tenantId    tenant id
   * @return future with {@link RecordCollection}
   */
  Future<RecordCollection> getRecords(Condition condition, Collection<OrderField<?>> orderFields, int offset, int limit, String tenantId);

  /**
   * Searches for {@link Record} by id
   *
   * @param id       Record id
   * @param tenantId tenant id
   * @return future with optional {@link Record}
   */
  Future<Optional<Record>> getRecordById(String id, String tenantId);

  /**
   * Searches for {@link Record} by id using {@link ReactiveClassicGenericQueryExecutor}
   *
   * @param txQE query execution
   * @param id   Record id
   * @return future with optional {@link Record}
   */
  Future<Optional<Record>> getRecordById(ReactiveClassicGenericQueryExecutor txQE, String id);

  /**
   * Searches for {@link Record} by matchedId
   *
   * @param matchedId       Record matchedId
   * @param tenantId tenant id
   * @return future with optional {@link Record}
   */
  Future<Optional<Record>> getRecordByMatchedId(String matchedId, String tenantId);

  /**
   * Searches for {@link Record} by matchedId using {@link ReactiveClassicGenericQueryExecutor}
   *
   * @param txQE query execution
   * @param matchedId   Record matchedId
   * @return future with optional {@link Record}
   */
  Future<Optional<Record>> getRecordByMatchedId(ReactiveClassicGenericQueryExecutor txQE, String matchedId);

  /**
   * Searches for {@link Record} by condition
   *
   * @param condition query where condition
   * @param tenantId  tenant id
   * @return future with optional {@link Record}
   */
  Future<Optional<Record>> getRecordByCondition(Condition condition, String tenantId);

  /**
   * Searches for {@link Record} by {@link Condition} using {@link ReactiveClassicGenericQueryExecutor}
   *
   * @param txQE      query executor
   * @param condition query where condition
   * @return future with optional {@link Record}
   */
  Future<Optional<Record>> getRecordByCondition(ReactiveClassicGenericQueryExecutor txQE, Condition condition);

  /**
   * Saves {@link Record} to the db
   *
   * @param record   Record to save
   * @param tenantId tenant id
   * @return future with saved Record
   */
  Future<Record> saveRecord(Record record, String tenantId);

  /**
   * Saves {@link Record} to the db using {@link ReactiveClassicGenericQueryExecutor}
   *
   * @param txQE   query executor
   * @param record Record to save
   * @return future with saved Record
   */
  Future<Record> saveRecord(ReactiveClassicGenericQueryExecutor txQE, Record record);

  /**
   * Updates {{@link Record} in the db
   *
   * @param record   Record to update
   * @param tenantId tenant id
   * @return future with updated Record
   */
  Future<Record> updateRecord(Record record, String tenantId);

  /**
   * Increments generation in case a record with the same matchedId exists
   * and the snapshot it is linked to is COMMITTED before the processing of the current one started
   *
   * @param txQE   query execution
   * @param record Record
   * @return future with generation
   */
  Future<Integer> calculateGeneration(ReactiveClassicGenericQueryExecutor txQE, Record record);

  /**
   * Updates {@link ParsedRecord} in the db
   *
   * @param record   record dto from which {@link ParsedRecord} will be updated
   * @param tenantId tenant id
   * @return future with updated ParsedRecord
   */
  Future<ParsedRecord> updateParsedRecord(Record record, String tenantId);

  /**
   * Searches for {@link Record} by id of external entity which was created from desired record
   *
   * @param externalId     external relation id
   * @param externalIdType external id type
   * @param tenantId       tenant id
   * @return future with optional {@link Record}
   */
  Future<Optional<Record>> getRecordByExternalId(String externalId, ExternalIdType externalIdType, String tenantId);

  /**
   * Searches for {@link Record} by id of external entity which was created from desired record
   *
   * @param txQE           query execution
   * @param externalId     external relation id
   * @param externalIdType external id type
   * @return future with optional {@link Record}
   */
  Future<Optional<Record>> getRecordByExternalId(ReactiveClassicGenericQueryExecutor txQE, String externalId, ExternalIdType externalIdType);

  /**
   * Searches for {@link SourceRecord} by {@link Condition} and ordered by order fields with offset and limit
   *
   * @param condition   query where condition
   * @param orderFields fields to order by
   * @param offset      starting index in a list of results
   * @param limit       limit of records for pagination
   * @param tenantId    tenant id
   * @return future with {@link SourceRecordCollection}
   */
  Future<SourceRecordCollection> getSourceRecords(Condition condition, Collection<OrderField<?>> orderFields, int offset, int limit, String tenantId);

  /**
   * Searches for {@link SourceRecord} where id in a list of ids defined by external id type. i.e. INSTANCE or RECORD
   *
   * @param ids            list of ids
   * @param externalIdType external id type on which source record will be searched
   * @param deleted        filter by state DELETED or leader record status d, s, or x
   * @param tenantId       tenant id
   * @return future with {@link SourceRecordCollection}
   */
  Future<SourceRecordCollection> getSourceRecords(List<String> ids, ExternalIdType externalIdType, Boolean deleted, String tenantId);

  /**
   * Searches for {@link SourceRecord} by {@link Condition}
   *
   * @param condition query where condition
   * @param tenantId  tenant id
   * @return return future with optional {@link SourceRecord}
   */
  Future<Optional<SourceRecord>> getSourceRecordByCondition(Condition condition, String tenantId);

  /**
   * Searches for {@link SourceRecord} by external entity which was created from desired record by specific type.
   *
   * @param id             id
   * @param externalIdType external id type on which source record will be searched
   * @param tenantId       tenant id
   * @return return future with optional {@link SourceRecord}
   */
  Future<Optional<SourceRecord>> getSourceRecordByExternalId(String id, ExternalIdType externalIdType, String tenantId);

  /**
   * Deletes in transaction all records associated with specified snapshot and snapshot itself
   *
   * @param snapshotId snapshot id
   * @param tenantId   tenant id
   * @return future with true if succeeded
   */
  Future<Boolean> deleteRecordsBySnapshotId(String snapshotId, String tenantId);

  /**
   * Creates new Record and updates status of the "old" one,
   * no data is overwritten as a result of update
   *
   * @param newRecord new Record to create
   * @param oldRecord old Record that has to be marked as "old"
   * @param tenantId  tenant id
   * @return future with new "updated" Record
   */
  Future<Record> saveUpdatedRecord(Record newRecord, Record oldRecord, String tenantId);

  /**
   * Creates new Record and updates status of the "old" one,
   * no data is overwritten as a result of update. Creates
   * new snapshot.
   *
   * @param txQE      query execution
   * @param newRecord new Record to create
   * @param oldRecord old Record that has to be marked as "old"
   * @return future with new "updated" Record
   */
  Future<Record> saveUpdatedRecord(ReactiveClassicGenericQueryExecutor txQE, Record newRecord, Record oldRecord);

  /**
   * Change suppress from discovery flag for record by external relation id
   *
   * @param id       id
   * @param idType   external id type
   * @param suppress suppress from discovery
   * @param tenantId tenant id
   * @return future with true if succeeded
   */
  Future<Boolean> updateSuppressFromDiscoveryForRecord(String id, String idType, Boolean suppress, String tenantId);

  /**
   * Execute action within transaction.
   *
   * @param <T>      future generic type
   * @param action   action
   * @param tenantId tenant id
   * @return future with generic type
   */
  <T> Future<T> executeInTransaction(Function<ReactiveClassicGenericQueryExecutor, Future<T>> action, String tenantId);

}
