package org.folio.dao;

import org.folio.dao.filter.ErrorRecordFilter;
import org.folio.rest.jaxrs.model.ErrorRecord;
import org.folio.rest.jaxrs.model.ErrorRecordCollection;

/**
 * Data access object for {@link ErrorRecord}
 */
public interface ErrorRecordDao extends BeanDao<ErrorRecord, ErrorRecordCollection, ErrorRecordFilter> {

}