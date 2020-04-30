package org.folio.dao;

import org.folio.dao.filter.RawRecordFilter;
import org.folio.rest.jaxrs.model.RawRecord;
import org.folio.rest.jaxrs.model.RawRecordCollection;

public interface RawRecordDao extends BeanDao<RawRecord, RawRecordCollection, RawRecordFilter> {

}