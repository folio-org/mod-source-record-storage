package org.folio.dao.util;

import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.Record;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.LoaderOptionsStep;

/**
 * Interface for operations with separate parsed record tables.
 */
public interface ParsedRecordType {

  void formatRecord(Record record) throws FormatRecordException;

  Condition getRecordImplicitCondition();

  Condition getSourceRecordImplicitCondition();

  org.jooq.Record toDatabaseRecord2(ParsedRecord parsedRecord);

  @SuppressWarnings("squid:S1452")
  LoaderOptionsStep<?> toLoaderOptionsStep(DSLContext dsl);

}
