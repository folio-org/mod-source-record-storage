package org.folio.dao.util;

import java.util.UUID;

import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.Record;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.JSONB;
import org.jooq.LoaderOptionsStep;
import org.jooq.Record2;

public interface ParsedRecordType {

  void formatRecord(Record record) throws FormatRecordException;

  Condition getRecordImplicitCondition();

  Condition getSourceRecordImplicitCondition();

  Record2<UUID, JSONB> toDatabaseRecord2(ParsedRecord parsedRecord);

  LoaderOptionsStep toLoaderOptionsStep(DSLContext dsl);

}
