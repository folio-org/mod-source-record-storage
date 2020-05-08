package org.folio.services.impl;

import org.folio.dao.ErrorRecordDao;
import org.folio.dao.filter.ErrorRecordFilter;
import org.folio.rest.jaxrs.model.ErrorRecord;
import org.folio.rest.jaxrs.model.ErrorRecordCollection;
import org.folio.services.AbstractEntityService;
import org.folio.services.ErrorRecordService;
import org.springframework.stereotype.Service;

@Service
public class ErrorRecordServiceImpl extends AbstractEntityService<ErrorRecord, ErrorRecordCollection, ErrorRecordFilter, ErrorRecordDao>
    implements ErrorRecordService {

}