package org.folio.dao;

import org.folio.dao.filter.SourceRecordFilter;
import org.folio.rest.jaxrs.model.SourceRecord;
import org.folio.rest.jaxrs.model.SourceRecordCollection;

public interface SourceRecordDao extends BeanDao<SourceRecord, SourceRecordCollection, SourceRecordFilter> {

}