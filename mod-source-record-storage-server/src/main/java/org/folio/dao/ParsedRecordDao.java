package org.folio.dao;

import org.folio.dao.query.ParsedRecordQuery;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.ParsedRecordCollection;
import org.folio.rest.jaxrs.model.Record;

import io.vertx.core.Future;

/**
 * Data access object for {@link ParsedRecord}
 */
public interface ParsedRecordDao extends EntityDao<ParsedRecord, ParsedRecordCollection, ParsedRecordQuery> {

  /**
   * Updates {@link ParsedRecord} in the db
   *
   * @param record   record dto from which {@link ParsedRecord} will be updated
   * @param tenantId tenant id
   * @return future with updated ParsedRecord
   */
  public Future<ParsedRecord> updateParsedRecord(Record record, String tenantId);

}