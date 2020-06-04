package org.folio.dao;

import java.util.Collection;
import java.util.Optional;
import java.util.function.Function;

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
public interface LBRecordDao extends RecordDao {

  /**
   * {@inheritDoc}
   * @deprecated
   */
  @Override
  @Deprecated
  default Future<RecordCollection> getRecords(String query, int offset, int limit, String tenantId) {
    throw new UnsupportedOperationException("Lookup records by CQL is no longer supported");
  }

  /**
   * {@inheritDoc}
   * @deprecated
   */
  @Override
  @Deprecated
  default Future<SourceRecordCollection> getSourceRecords(String query, int offset, int limit, boolean deletedRecords, String tenantId) {
    throw new UnsupportedOperationException("Lookup source records by CQL is no longer supported");
  }

  /**
   * Searches for {@link Record} by id using {@link ReactiveClassicGenericQueryExecutor}
   * 
   * @param txQE query execution
   * @param id   Record id
   * @return future with optional {@link Record}
   */
  Future<Optional<Record>> getRecordById(ReactiveClassicGenericQueryExecutor txQE, String id);

  /**
   * Saves {@link Record} to the db using {@link ReactiveClassicGenericQueryExecutor}
   * 
   * @param txQE   query executor
   * @param record {@link Record} to save
   * @return future with saved Record
   */
  Future<Record> saveRecord(ReactiveClassicGenericQueryExecutor txQE, Record record);

  /**
   * Searches for {@link Record} by condition
   * 
   * @param condition condition
   * @param tenantId  tenant id
   * @return future with optional {@link Record}
   */
  Future<Optional<Record>> getRecordByCondition(Condition condition, String tenantId);

  /**
   * Searches for {@link Record} by {@link Condition} using {@link ReactiveClassicGenericQueryExecutor}
   * 
   * @param txQE      query executor
   * @param condition condition
   * @return future with optional {@link Record}
   */
  Future<Optional<Record>> getRecordByCondition(ReactiveClassicGenericQueryExecutor txQE, Condition condition);

  /**
   * Searches for {@link Record} by {@link Condition} and ordered by collection of {@link OrderField} with offset and limit
   * 
   * @param condition   condition
   * @param orderFields fields to order by
   * @param offset      offset
   * @param limit       limit
   * @param tenantId    tenant id
   * @return future with {@link RecordCollection}
   */
  Future<RecordCollection> getRecords(Condition condition, Collection<OrderField<?>> orderFields, int offset, int limit, String tenantId);

  /**
   * Searches for {@link SourceRecord} by {@link Condition} and ordered by order fields with offset and limit
   * 
   * @param condition   condition
   * @param orderFields fields to order by
   * @param offset      offset
   * @param limit       limit
   * @param tenantId    tenant id
   * @return future with {@link SourceRecordCollection}
   */
  Future<SourceRecordCollection> getSourceRecords(Condition condition, Collection<OrderField<?>> orderFields, int offset, int limit, String tenantId);

  /**
   * Searches for {@link SourceRecord} by {@link Condition}
   * 
   * @param condition condition
   * @param tenantId  tenant id
   * @return - return future with optional {@link SourceRecord}
   */
  Future<Optional<SourceRecord>> getSourceRecordByCondition(Condition condition, String tenantId);

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
   * Execute action within transaction.
   * 
   * @param <T>      future generic type
   * @param action   action
   * @param tenantId tenant id
   * @return future with generic type
   */
  <T> Future<T> executeInTransaction(Function<ReactiveClassicGenericQueryExecutor, Future<T>> action, String tenantId);

}