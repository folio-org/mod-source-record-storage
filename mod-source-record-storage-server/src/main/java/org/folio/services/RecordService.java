package org.folio.services;

import io.reactivex.Flowable;
import io.vertx.core.Future;
import io.vertx.sqlclient.Row;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.folio.dao.util.IdType;
import org.folio.dao.util.RecordType;
import org.folio.rest.jaxrs.model.FetchParsedRecordsBatchRequest;
import org.folio.rest.jaxrs.model.MarcBibCollection;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.ParsedRecordDto;
import org.folio.rest.jaxrs.model.ParsedRecordsBatchResponse;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.RecordCollection;
import org.folio.rest.jaxrs.model.RecordMatchingDto;
import org.folio.rest.jaxrs.model.RecordsBatchResponse;
import org.folio.rest.jaxrs.model.RecordsIdentifiersCollection;
import org.folio.rest.jaxrs.model.SourceRecord;
import org.folio.rest.jaxrs.model.SourceRecordCollection;
import org.folio.rest.jaxrs.model.StrippedParsedRecordCollection;
import org.folio.rest.jooq.enums.RecordState;
import org.folio.services.entities.RecordsModifierOperator;
import org.jooq.Condition;
import org.jooq.OrderField;

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
   * @param okapiHeaders okapi headers
   * @return future with saved Record
   */
  Future<Record> saveRecord(Record record, Map<String, String> okapiHeaders);

  /**
   * Saves collection of records
   *
   * @param recordsCollection records to save
   * @param okapiHeaders okapi headers
   * @return future with response containing list of successfully saved records and error messages for records that were not saved
   */
  Future<RecordsBatchResponse> saveRecords(RecordCollection recordsCollection, Map<String, String> okapiHeaders);

  /**
   * Saves collection of records.
   *
   * @param externalIds external relation ids
   * @param recordType  record type
   * @param recordsModifier records collection modifier operator
   * @param okapiHeaders okapi headers
   * @param maxSaveRetryCount max count of retries to save records
   * @return future with response containing list of successfully saved records and error messages for records that were not saved
   */
  Future<RecordsBatchResponse> saveRecordsByExternalIds(List<String> externalIds, RecordType recordType,
                                                        RecordsModifierOperator recordsModifier,
                                                        Map<String, String> okapiHeaders,
                                                        int maxSaveRetryCount);

  /**
   * Updates record with given id
   *
   * @param record   record to update
   * @param okapiHeaders okapi headers
   * @return future with updated Record
   */
  Future<Record> updateRecord(Record record, Map<String, String> okapiHeaders);

  /**
   * Updates record generation with given matched id
   *
   * @param matchedId   matched id
   * @param record   record to update
   * @param okapiHeaders okapi headers
   * @return future with updated Record generation
   */
  Future<Record> updateRecordGeneration(String matchedId, Record record, Map<String, String> okapiHeaders);

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
   * @param searchParameters params needed for search
   * @return {@link Flowable} of {@link Record id}
   */
  Flowable<Row> streamMarcRecordIds(RecordSearchParameters searchParameters, String tenantId);

  /**
   * Searches for {@link SourceRecord} where id in a list of ids defined by id type. i.e. INSTANCE or RECORD
   *
   * @param ids            list of ids
   * @param idType         id type
   * @param recordType     record type
   * @param deleted        filter by state DELETED or leader record status d, s, or x
   * @param tenantId       tenant id
   * @return future with {@link SourceRecordCollection}
   */
  Future<SourceRecordCollection> getSourceRecords(List<String> ids, IdType idType, RecordType recordType, Boolean deleted, String tenantId);

  /**
   * Searches for source record by id via specific id type
   *
   * @param id             for searching
   * @param idType         search type
   * @param state          search record state
   * @param tenantId       tenant id
   * @return future with optional source record
   */
  Future<Optional<SourceRecord>> getSourceRecordById(String id, IdType idType, RecordState state, String tenantId);

  /**
   * Searches for {@link Record} by condition specified in {@link RecordMatchingDto}
   * and returns {@link RecordsIdentifiersCollection} representing list of pairs of recordId and externalId
   *
   * @param recordMatchingDto record matching request that describes matching criteria
   * @param tenantId          tenant id
   * @return {@link Future} of {@link RecordsIdentifiersCollection}
   */
  Future<RecordsIdentifiersCollection> getMatchedRecordsIdentifiers(RecordMatchingDto recordMatchingDto, String tenantId);

  /**
   * Updates {@link ParsedRecord} in the db
   *
   * @param record   record dto from which {@link ParsedRecord} will be updated
   * @param okapiHeaders okapi headers
   * @return future with updated ParsedRecord
   */
  Future<ParsedRecord> updateParsedRecord(Record record, Map<String, String> okapiHeaders);

  /**
   * Update parsed records from collection of records and external relations ids in one transaction
   *
   * @param recordCollection collection of records from which parsed records will be updated
   * @param okapiHeaders okapi headers
   * @return future with response containing list of successfully updated records and error messages for records that were not updated
   */
  Future<ParsedRecordsBatchResponse> updateParsedRecords(RecordCollection recordCollection, Map<String, String> okapiHeaders);

  /**
   * Fetch stripped parsed records by ids and filter marc fields by provided range of fields
   *
   * @param fetchRequest     fetch request
   * @param tenantId         tenant id
   * @return {@link Future} of {@link StrippedParsedRecordCollection}
   */
  Future<StrippedParsedRecordCollection> fetchStrippedParsedRecords(FetchParsedRecordsBatchRequest fetchRequest, String tenantId);

  /**
   * Searches for Record either by SRS id or external relation id
   *
   * @param id             either SRS id or external relation id
   * @param idType         specifies of external relation id type
   * @param tenantId       tenant id
   * @return future with {@link Record}
   */
  Future<Record> getFormattedRecord(String id, IdType idType, String tenantId);

  /**
   * Change suppress from discovery flag for record by external relation id
   *
   * @param id             id
   * @param idType         external id type
   * @param suppress       suppress from discovery
   * @param tenantId       tenant id
   * @return future with true if succeeded
   */
  Future<Boolean> updateSuppressFromDiscoveryForRecord(String id, IdType idType, Boolean suppress, String tenantId);

  /**
   * Deletes records associated with specified snapshot and snapshot itself
   *
   * @param snapshotId snapshot id
   * @param tenantId   tenant id
   * @return future with true if succeeded
   */
  Future<Boolean> deleteRecordsBySnapshotId(String snapshotId, String tenantId);

  /**
   * Deletes records by external id
   *
   * @param externalId external id
   * @param okapiHeaders okapi headers
   * @return future with true if succeeded
   */
  Future<Void> deleteRecordsByExternalId(String externalId, Map<String, String> okapiHeaders);

  /**
   * Creates new updated Record with incremented generation linked to a new Snapshot, and sets OLD status to the "old" Record,
   * no data is deleted as a result of the update
   *
   * @param parsedRecordDto parsed record DTO containing updates to parsed record
   * @param snapshotId      snapshot id to which new Record should be linked
   * @param okapiHeaders okapi headers
   * @return future with updated Record
   */
  Future<Record> updateSourceRecord(ParsedRecordDto parsedRecordDto, String snapshotId, Map<String, String> okapiHeaders);

  /**
   * Find marc bib ids by incoming arrays from SRM and exclude all valid marc bib and return only marc bib ids,
   * which does not exists in the system
   *
   * @param marcBibIds list of marc bib ids (max size - 32767)
   * @param tenantId tenant id
   * @return future with list of invalid marc bib ids
   */
  Future<MarcBibCollection> verifyMarcBibRecords(List<String> marcBibIds, String tenantId);

  /**
   * Updates a multiple records that have same matched id. It's usually the actual record,
   * and related records of an older generations.
   *
   * @param matchedId matched id
   * @param state record state
   * @param recordType record type
   * @param tenantId  tenant id
   * @return void future
   */
  Future<Void> updateRecordsState(String matchedId, RecordState state, RecordType recordType, String tenantId);

  Future<Void> deleteRecordById(String id, IdType idType, Map<String, String> okapiHeaders);

  Future<Void> unDeleteRecordById(String id, IdType idType, Map<String, String> okapiHeaders);
}
