package org.folio.services.impl;

import org.folio.dao.LBRecordDao;
import org.folio.dao.filter.RecordFilter;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.RecordCollection;
import org.folio.services.AbstractEntityService;
import org.folio.services.LBRecordService;
import org.springframework.stereotype.Service;

@Service
public class LBRecordServiceImpl extends AbstractEntityService<Record, RecordCollection, RecordFilter, LBRecordDao>
    implements LBRecordService {

}