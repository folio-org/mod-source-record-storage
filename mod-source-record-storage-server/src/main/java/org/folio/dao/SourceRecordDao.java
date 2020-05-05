package org.folio.dao;

import java.util.Date;
import java.util.Optional;

import org.folio.rest.jaxrs.model.SourceRecord;
import org.folio.rest.jaxrs.model.SourceRecordCollection;

import io.vertx.core.Future;

/**
 * Data access object for {@link SourceRecord}
 */
public interface SourceRecordDao {

  /**
   * Searches for {@link SourceRecord} by matched id of {@link Record}
   * 
   * @param id       Record id
   * @param tenantId tenant id
   * @return future with optional {@link SourceRecord} with {@link ParsedRecord} only
   */
  public Future<Optional<SourceRecord>> getSourceMarcRecordById(String id, String tenantId);

  /**
   * Searches for {@link SourceRecord} by matched id of {@link Record} with latest generation
   * 
   * @param id       Record id
   * @param tenantId tenant id
   * @return future with optional {@link SourceRecord} with {@link ParsedRecord} only
   */
  public Future<Optional<SourceRecord>> getSourceMarcRecordByIdAlt(String id, String tenantId);

  /**
   * Searches for {@link SourceRecord} by {@link ExternalIdsHolder} instanceId of {@link Record}
   * 
   * @param id       Record instance id
   * @param tenantId tenant id
   * @return future with optional {@link SourceRecord} with {@link ParsedRecord} only
   */
  public Future<Optional<SourceRecord>> getSourceMarcRecordByInstanceId(String instanceId, String tenantId);

  /**
   * Searches for {@link SourceRecord} by {@link ExternalIdsHolder} instanceId of {@link Record} with latest generation
   * 
   * @param id       Record instance id
   * @param tenantId tenant id
   * @return future with optional {@link SourceRecord} with {@link ParsedRecord} only
   */
  public Future<Optional<SourceRecord>> getSourceMarcRecordByInstanceIdAlt(String instanceId, String tenantId);

  /**
   * Searches for collection of {@link SourceRecord} with {@link Record.State} of 'ACTUAL'
   * 
   * @param offset   starting index in a list of results
   * @param limit    maximum number of results to return
   * @param tenantId tenant id
   * @return future with {@link SourceRecordCollection} with list of {@link ParsedRecord} only
   */
  public Future<SourceRecordCollection> getSourceMarcRecords(Integer offset, Integer limit, String tenantId);

  /**
   * Searches for collection of {@link SourceRecord} with {@link Record.State} of 'ACTUAL' and latest generation
   * 
   * @param offset   starting index in a list of results
   * @param limit    maximum number of results to return
   * @param tenantId tenant id
   * @return future with {@link SourceRecordCollection} with list of {@link ParsedRecord} only
   */
  public Future<SourceRecordCollection> getSourceMarcRecordsAlt(Integer offset, Integer limit, String tenantId);

  /**
   * Searches for collection of {@link SourceRecord} for a given period with {@link Record.State} of 'ACTUAL'
   * 
   * @param from     starting updated date
   * @param till     ending updated date
   * @param offset   starting index in a list of results
   * @param limit    maximum number of results to return
   * @param tenantId tenant id
   * @return future with {@link SourceRecordCollection} with list of {@link ParsedRecord} only
   */
  public Future<SourceRecordCollection> getSourceMarcRecordsForPeriod(Date from, Date till, Integer offset, Integer limit, String tenantId);

  /**
   * Searches for collection of {@link SourceRecord} for a given period with {@link Record.State} of 'ACTUAL' and latest generation
   * 
   * @param from     starting updated date
   * @param till     ending updated date
   * @param offset   starting index in a list of results
   * @param limit    maximum number of results to return
   * @param tenantId tenant id
   * @return future with {@link SourceRecordCollection} with list of {@link ParsedRecord} only
   */
  public Future<SourceRecordCollection> getSourceMarcRecordsForPeriodAlt(Date from, Date till, Integer offset, Integer limit, String tenantId);

}