package org.folio.dao.query;

import static org.folio.dao.impl.ErrorRecordDaoImpl.DESCRIPTION_COLUMN_NAME;
import static org.folio.dao.util.DaoUtil.CONTENT_COLUMN_NAME;
import static org.folio.dao.util.DaoUtil.ERROR_RECORDS_TABLE_NAME;
import static org.folio.dao.util.DaoUtil.ID_COLUMN_NAME;

import org.folio.dao.query.Metamodel.Property;
import org.folio.rest.jaxrs.model.ErrorRecord;

@Metamodel(
  entity = ErrorRecord.class,
  table = ERROR_RECORDS_TABLE_NAME,
  properties = {
    @Property(path = "id", column = ID_COLUMN_NAME),
    @Property(path = "content", column = CONTENT_COLUMN_NAME),
    @Property(path = "description", column = DESCRIPTION_COLUMN_NAME)
  }
)
public class ErrorRecordQuery extends AbstractEntityQuery<ErrorRecordQuery> {

  public static ErrorRecordQuery query() {
    return new ErrorRecordQuery();
  }

}