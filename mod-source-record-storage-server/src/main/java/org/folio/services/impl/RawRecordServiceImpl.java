package org.folio.services.impl;

import org.folio.dao.RawRecordDao;
import org.folio.dao.filter.RawRecordFilter;
import org.folio.rest.jaxrs.model.RawRecord;
import org.folio.rest.jaxrs.model.RawRecordCollection;
import org.folio.services.AbstractEntityService;
import org.folio.services.RawRecordService;
import org.springframework.stereotype.Service;

@Service
public class RawRecordServiceImpl extends AbstractEntityService<RawRecord, RawRecordCollection, RawRecordFilter, RawRecordDao>
    implements RawRecordService {

}