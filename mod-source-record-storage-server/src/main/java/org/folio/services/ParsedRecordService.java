package org.folio.services;

import org.folio.dao.ParsedRecordDao;
import org.folio.dao.query.ParsedRecordQuery;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.ParsedRecordCollection;

/**
 * {@link ParsedRecord} service
 */
public interface ParsedRecordService extends EntityService<ParsedRecord, ParsedRecordCollection, ParsedRecordQuery, ParsedRecordDao> {

}