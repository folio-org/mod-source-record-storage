package org.folio.dao.query;

import static org.folio.dao.util.DaoUtil.CONTENT_COLUMN_NAME;
import static org.folio.dao.util.DaoUtil.ID_COLUMN_NAME;
import static org.folio.dao.util.DaoUtil.PARSED_RECORDS_TABLE_NAME;

import org.folio.dao.query.Metamodel.Property;
import org.folio.rest.jaxrs.model.ParsedRecord;

@Metamodel(
  entity = ParsedRecord.class,
  table = PARSED_RECORDS_TABLE_NAME,
  properties = {
    @Property(path = "id", column = ID_COLUMN_NAME),
    @Property(path = "content", column = CONTENT_COLUMN_NAME)
  }
)
public class ParsedRecordQuery extends AbstractEntityQuery<ParsedRecordQuery> {

  public static ParsedRecordQuery query() {
    return new ParsedRecordQuery();
  }

}