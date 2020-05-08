package org.folio.services;

import org.folio.dao.LBRecordDao;
import org.folio.dao.filter.RecordFilter;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.RecordCollection;

/**
 * {@link Record} service
 */
public interface LBRecordService extends EntityService<Record, RecordCollection, RecordFilter, LBRecordDao> {

}