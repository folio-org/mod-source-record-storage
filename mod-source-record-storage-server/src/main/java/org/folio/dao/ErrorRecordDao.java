package org.folio.dao;

import org.folio.dao.query.ErrorRecordQuery;
import org.folio.rest.jaxrs.model.ErrorRecord;
import org.folio.rest.jaxrs.model.ErrorRecordCollection;

/**
 * Data access object for {@link ErrorRecord}
 */
public interface ErrorRecordDao extends EntityDao<ErrorRecord, ErrorRecordCollection, ErrorRecordQuery> {

}