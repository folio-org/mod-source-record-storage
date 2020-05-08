package org.folio.services;

import org.folio.dao.RawRecordDao;
import org.folio.dao.filter.RawRecordFilter;
import org.folio.rest.jaxrs.model.RawRecord;
import org.folio.rest.jaxrs.model.RawRecordCollection;

/**
 * {@link RawRecord} service
 */
public interface RawRecordService extends EntityService<RawRecord, RawRecordCollection, RawRecordFilter, RawRecordDao> {

}