package org.folio.services;

import org.folio.dao.ErrorRecordDao;
import org.folio.dao.query.ErrorRecordQuery;
import org.folio.rest.jaxrs.model.ErrorRecord;
import org.folio.rest.jaxrs.model.ErrorRecordCollection;

/**
 * {@link ErrorRecord} service
 */
public interface ErrorRecordService extends EntityService<ErrorRecord, ErrorRecordCollection, ErrorRecordQuery, ErrorRecordDao> {

}