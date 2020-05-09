package org.folio.services;

import org.folio.dao.query.RawRecordQuery;
import org.folio.rest.jaxrs.model.RawRecord;
import org.folio.rest.jaxrs.model.RawRecordCollection;

/**
 * {@link RawRecord} service
 */
public interface RawRecordService extends EntityService<RawRecord, RawRecordCollection, RawRecordQuery> {

}