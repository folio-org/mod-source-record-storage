package org.folio.services.impl;

import org.folio.dao.ParsedRecordDao;
import org.folio.dao.filter.ParsedRecordFilter;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.ParsedRecordCollection;
import org.folio.services.AbstractEntityService;
import org.folio.services.ParsedRecordService;
import org.springframework.stereotype.Service;

@Service
public class ParsedRecordServiceImpl extends AbstractEntityService<ParsedRecord, ParsedRecordCollection, ParsedRecordFilter, ParsedRecordDao>
    implements ParsedRecordService {

}