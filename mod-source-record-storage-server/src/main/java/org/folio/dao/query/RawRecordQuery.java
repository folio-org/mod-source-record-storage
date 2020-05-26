package org.folio.dao.query;

import static org.folio.dao.util.DaoUtil.CONTENT_COLUMN_NAME;
import static org.folio.dao.util.DaoUtil.ID_COLUMN_NAME;
import static org.folio.dao.util.DaoUtil.RAW_RECORDS_TABLE_NAME;

import org.folio.dao.query.Metamodel.Property;
import org.folio.rest.jaxrs.model.RawRecord;

@Metamodel(
  entity = RawRecord.class,
  table = RAW_RECORDS_TABLE_NAME,
  properties = {
    @Property(path = "id", column = ID_COLUMN_NAME),
    @Property(path = "content", column = CONTENT_COLUMN_NAME)
  }
)
public class RawRecordQuery extends AbstractEntityQuery<RawRecordQuery> {

  public static RawRecordQuery query() {
    return new RawRecordQuery();
  }

}