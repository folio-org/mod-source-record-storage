package org.folio.dao;

import java.util.Date;
import java.util.Optional;

import org.folio.rest.jaxrs.model.SourceRecord;
import org.folio.rest.jaxrs.model.SourceRecordCollection;

import io.vertx.core.Future;

public interface SourceRecordDao {

  public Future<Optional<SourceRecord>> getSourceMarcRecordById(String id, String tenantId);

  public Future<Optional<SourceRecord>> getSourceMarcRecordByIdAlt(String id, String tenantId);

  public Future<Optional<SourceRecord>> getSourceMarcRecordByInstanceId(String instanceId, String tenantId);

  public Future<Optional<SourceRecord>> getSourceMarcRecordByInstanceIdAlt(String instanceId, String tenantId);

  public Future<SourceRecordCollection> getSourceMarcRecords(Integer offset, Integer limit, String tenantId);

  public Future<SourceRecordCollection> getSourceMarcRecordsAlt(Integer offset, Integer limit, String tenantId);

  public Future<SourceRecordCollection> getSourceMarcRecordsForPeriod(Date from, Date to, Integer offset, Integer limit, String tenantId);

  public Future<SourceRecordCollection> getSourceMarcRecordsForPeriodAlt(Date from, Date to, Integer offset, Integer limit, String tenantId);

}