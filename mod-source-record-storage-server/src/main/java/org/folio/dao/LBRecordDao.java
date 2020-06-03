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

public interface LBRecordDao extends RecordDao {

  /**
   * @deprecated
   */
  @Override
  @Deprecated
  default Future<RecordCollection> getRecords(String query, int offset, int limit, String tenantId) {
    throw new UnsupportedOperationException("Lookup records by CQL is no longer supported");
  }

  /**
   * @deprecated
   */
  @Override
  @Deprecated
  default Future<SourceRecordCollection> getSourceRecords(String query, int offset, int limit, boolean deletedRecords, String tenantId) {
    throw new UnsupportedOperationException("Lookup source records by CQL is no longer supported");
  }

  Future<Optional<Record>> getRecordById(ReactiveClassicGenericQueryExecutor txQE, String matchedId);

  Future<Record> saveRecord(ReactiveClassicGenericQueryExecutor txQE, Record record);

  Future<Optional<Record>> getRecordByCondition(Condition condition, String tenantId);

  Future<Optional<Record>> getRecordByCondition(ReactiveClassicGenericQueryExecutor txQE, Condition condition);

  Future<RecordCollection> getRecords(Condition condition, Collection<OrderField<?>> orderFields, int offset, int limit, String tenantId);

  Future<SourceRecordCollection> getSourceRecords(Condition condition, Collection<OrderField<?>> orderFields, int offset, int limit, String tenantId);

  Future<Optional<SourceRecord>> getSourceRecordByCondition(Condition condition, String tenantId);

  Future<Integer> calculateGeneration(ReactiveClassicGenericQueryExecutor txQE, Record record);

  Future<Record> saveUpdatedRecord(ReactiveClassicGenericQueryExecutor txQE, Record newRecord, Record oldRecord);

  <T> Future<T> executeInTransaction(Function<ReactiveClassicGenericQueryExecutor, Future<T>> action, String tenantId);

}