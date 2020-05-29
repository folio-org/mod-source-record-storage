package org.folio.services;

import java.util.Collection;
import java.util.Optional;

import org.folio.dao.PostgresClientFactory;
import org.folio.rest.jaxrs.model.ParsedRecordDto;
import org.folio.rest.jaxrs.model.ParsedRecordsBatchResponse;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.RecordCollection;
import org.folio.rest.jaxrs.model.RecordsBatchResponse;
import org.folio.rest.jaxrs.model.SourceRecord;
import org.folio.rest.jaxrs.model.SourceRecordCollection;
import org.folio.rest.jaxrs.model.SuppressFromDiscoveryDto;
import org.jooq.Condition;
import org.jooq.OrderField;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import io.vertx.core.Future;

@Service
@ConditionalOnProperty(prefix = "jooq", name = "services.record", havingValue = "true")
public class LBRecordServiceImpl implements LBRecordService {

  private final PostgresClientFactory postgresClientFactory;

  @Autowired
  public LBRecordServiceImpl(final PostgresClientFactory postgresClientFactory) {
    this.postgresClientFactory = postgresClientFactory;
  }

  @Override
  public Future<Optional<Record>> getRecordById(String id, String tenantId) {
    return null;
  }

  @Override
  public Future<Record> saveRecord(Record record, String tenantId) {
    return null;
  }

  @Override
  public Future<RecordsBatchResponse> saveRecords(RecordCollection recordsCollection, String tenantId) {
    return null;
  }

  @Override
  public Future<Record> updateRecord(Record record, String tenantId) {
    return null;
  }

  @Override
  public Future<Optional<SourceRecord>> getSourceRecordById(String id, String idType, String tenantId) {
    return null;
  }

  @Override
  public Future<ParsedRecordsBatchResponse> updateParsedRecords(RecordCollection recordCollection, String tenantId) {
    return null;
  }

  @Override
  public Future<Record> getFormattedRecord(String externalIdIdentifier, String id, String tenantId) {
    return null;
  }

  @Override
  public Future<Boolean> updateSuppressFromDiscoveryForRecord(SuppressFromDiscoveryDto suppressFromDiscoveryDto,
      String tenantId) {
    return null;
  }

  @Override
  public Future<Boolean> deleteRecordsBySnapshotId(String snapshotId, String tenantId) {
    return null;
  }

  @Override
  public Future<Record> updateSourceRecord(ParsedRecordDto parsedRecordDto, String snapshotId, String tenantId) {
    return null;
  }

  @Override
  public Future<RecordCollection> getRecords(Condition condition, Collection<OrderField<?>> orderFields, int offset,
      int limit, String tenantId) {
    return null;
  }

  @Override
  public Future<SourceRecordCollection> getSourceRecords(Condition condition, Collection<OrderField<?>> orderFields,
      int offset, int limit, String tenantId) {
    return null;
  }

}