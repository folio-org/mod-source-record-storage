package org.folio.services;

import java.util.Date;
import java.util.Optional;

import org.folio.dao.query.RecordQuery;
import org.folio.dao.util.SourceRecordContent;
import org.folio.rest.jaxrs.model.ExternalIdsHolder;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.RawRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.SourceRecord;
import org.folio.rest.jaxrs.model.SourceRecordCollection;
import org.folio.rest.jaxrs.model.SuppressFromDiscoveryDto.IncomingIdType;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;

/**
 * {@link SourceRecord} service interface. Lookup {@link Record} and enhance
 * with related source {@link RawRecord} and {@link ParsedRecord}.
 *
 */
public interface SourceRecordService {

  /**
   * Searches for {@link SourceRecord} by matched id of {@link Record}
   * 
   * @param id       record id
   * @param tenantId tenant id
   * @return future with optional {@link SourceRecord} with {@link ParsedRecord} only
   */
  Future<Optional<SourceRecord>> getSourceMarcRecordById(String id, String tenantId);

  /**
   * Searches for {@link SourceRecord} by matched id of {@link Record} with latest generation
   * 
   * @param id       record id
   * @param tenantId tenant id
   * @return future with optional {@link SourceRecord} with {@link ParsedRecord} only
   */
  Future<Optional<SourceRecord>> getSourceMarcRecordByIdAlt(String id, String tenantId);

  /**
   * Searches for {@link SourceRecord} by {@link ExternalIdsHolder} instance id of {@link Record}
   * 
   * @param instanceId record instance id
   * @param tenantId   tenant id
   * @return future with optional {@link SourceRecord} with {@link ParsedRecord} only
   */
  Future<Optional<SourceRecord>> getSourceMarcRecordByInstanceId(String instanceId,
      String tenantId);

  /**
   * Searches for {@link SourceRecord} by {@link ExternalIdsHolder} instance id of {@link Record}
   * with latest generation
   * 
   * @param instanceId record instance id
   * @param tenantId   tenant id
   * @return future with optional {@link SourceRecord} with {@link ParsedRecord} only
   */
  Future<Optional<SourceRecord>> getSourceMarcRecordByInstanceIdAlt(String instanceId,
      String tenantId);

  /**
   * Searches for collection of {@link SourceRecord} with {@link Record.State} of 'ACTUAL'
   * 
   * @param offset   starting index in a list of results
   * @param limit    maximum number of results to return
   * @param tenantId tenant id
   * @return future with {@link SourceRecordCollection} with list of {@link ParsedRecord} only
   */
  Future<SourceRecordCollection> getSourceMarcRecords(Integer offset, Integer limit,
      String tenantId);

  /**
   * Searches for collection of {@link SourceRecord} with {@link Record.State} of 'ACTUAL'
   * and latest generation
   * 
   * @param offset   starting index in a list of results
   * @param limit    maximum number of results to return
   * @param tenantId tenant id
   * @return future with {@link SourceRecordCollection} with list of {@link ParsedRecord} only
   */
  Future<SourceRecordCollection> getSourceMarcRecordsAlt(Integer offset, Integer limit,
      String tenantId);

  /**
   * Searches for collection of {@link SourceRecord} for a given period with {@link Record.State}
   * of 'ACTUAL'
   * 
   * @param from     starting updated date
   * @param till     ending updated date
   * @param offset   starting index in a list of results
   * @param limit    maximum number of results to return
   * @param tenantId tenant id
   * @return future with {@link SourceRecordCollection} with list of {@link ParsedRecord} only
   */
  Future<SourceRecordCollection> getSourceMarcRecordsForPeriod(Date from, Date till,
      Integer offset, Integer limit, String tenantId);

  /**
   * Searches for collection of {@link SourceRecord} for a given period with {@link Record.State}
   * of 'ACTUAL' and latest generation
   * 
   * @param from     starting updated date
   * @param till     ending updated date
   * @param offset   starting index in a list of results
   * @param limit    maximum number of results to return
   * @param tenantId tenant id
   * @return future with {@link SourceRecordCollection} with list of {@link ParsedRecord} only
   */
  Future<SourceRecordCollection> getSourceMarcRecordsForPeriodAlt(Date from, Date till,
      Integer offset, Integer limit, String tenantId);

  /**
   * Searches for {@link SourceRecord} by id of {@link Record} and enhanced with {@link RawRecord},
   * {@link ParsedRecord}, or both.
   * 
   * @param content  {@link SourceRecordContent} to specify how to enhance
   * @param id       record id
   * @param tenantId tenant id
   * @return future with optional {@link SourceRecord} with specified {@link SourceRecordContent}
   */
  Future<Optional<SourceRecord>> getSourceMarcRecordById(SourceRecordContent content,
      String id, String tenantId);

  /**
   * Searches for {@link SourceRecord} by matched id of {@link Record} and enhanced with
   * {@link RawRecord}, {@link ParsedRecord}, or both.
   * 
   * @param content  {@link SourceRecordContent} to specify how to enhance
   * @param id       Record id
   * @param tenantId tenant id
   * @return future with optional {@link SourceRecord} with specified {@link SourceRecordContent}
   */
  Future<Optional<SourceRecord>> getSourceMarcRecordByMatchedId(SourceRecordContent content,
      String matchedId, String tenantId);

  /**
   * Searches for {@link SourceRecord} by {@link ExternalIdsHolder} instance id of {@link Record}
   * and enhanced with {@link RawRecord}, {@link ParsedRecord}, or both.
   * 
   * @param content  {@link SourceRecordContent} to specify how to enhance
   * @param id       Record id
   * @param tenantId tenant id
   * @return future with optional {@link SourceRecord} with specified {@link SourceRecordContent}
   */
  Future<Optional<SourceRecord>> getSourceMarcRecordByInstanceId(SourceRecordContent content,
      String instanceId, String tenantId);

  /**
   * Searches for collection of {@link SourceRecord} by {@link RecordQuery} enhanced with
   * {@link RawRecord}, {@link ParsedRecord}, or both.
   * 
   * @param content  {@link SourceRecordContent} to specify how to enhance
   * @param query    {@link RecordQuery} to prepare WHERE and ORDER BY clause
   * @param offset   starting index in a list of results
   * @param limit    maximum number of results to return
   * @param tenantId tenant id
   * @return future with {@link SourceRecordCollection} with specified {@link SourceRecordContent}
   */
  Future<SourceRecordCollection> getSourceMarcRecordsByQuery(SourceRecordContent content,
      RecordQuery query, Integer offset, Integer limit, String tenantId);

  /**
   * Searches for and streams collection of {@link SourceRecord} by {@link RecordQuery} enhanced
   * with {@link RawRecord}, {@link ParsedRecord}, or both.
   * 
   * @param content             {@link SourceRecordContent} to specify how to enhance
   * @param query               {@link RecordQuery} to prepare WHERE and ORDER BY clause
   * @param offset              starting index in a list of results
   * @param limit               maximum number of results to return
   * @param tenantId            tenant id
   * @param sourceRecordHandler handler for each {@link SourceRecord}
   * @param endHandler          handler for when stream is finished
   */
  void getSourceMarcRecordsByQuery(SourceRecordContent content, RecordQuery query,
      Integer offset, Integer limit, String tenantId, Handler<SourceRecord> sourceRecordHandler,
      Handler<AsyncResult<Void>> endHandler);

  /**
   * Searches for source record by id via specific {@link IncomingIdType}
   *
   * @param id       - for searching
   * @param idType   - search type
   * @param tenantId - tenant id
   * @return future with optional source record
   */
  Future<Optional<SourceRecord>> getSourceRecordById(String id, IncomingIdType idType, String tenantId);

  /**
   * Convert {@link Record} to {@link SourceRecord}
   * 
   * @param record record
   * @return source record
   */
  SourceRecord toSourceRecord(Record record);

}