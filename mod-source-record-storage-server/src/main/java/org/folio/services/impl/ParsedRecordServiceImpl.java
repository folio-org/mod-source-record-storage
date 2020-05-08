package org.folio.services.impl;

import org.folio.dao.ParsedRecordDao;
import org.folio.dao.query.ParsedRecordQuery;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.ParsedRecordCollection;
import org.folio.services.AbstractEntityService;
import org.folio.services.ParsedRecordService;
import org.springframework.stereotype.Service;

@Service
public class ParsedRecordServiceImpl extends AbstractEntityService<ParsedRecord, ParsedRecordCollection, ParsedRecordQuery, ParsedRecordDao>
    implements ParsedRecordService {

}