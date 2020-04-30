package org.folio.dao;

import org.folio.dao.filter.RecordFilter;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.RecordCollection;

public interface LBRecordDao extends BeanDao<Record, RecordCollection, RecordFilter> {

}