package org.folio.dao;

import org.folio.dao.query.ParsedRecordQuery;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.ParsedRecordCollection;

/**
 * Data access object for {@link ParsedRecord}
 */
public interface ParsedRecordDao extends EntityDao<ParsedRecord, ParsedRecordCollection, ParsedRecordQuery> {

}