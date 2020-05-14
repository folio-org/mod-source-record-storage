package org.folio.dao.query;

import static org.folio.dao.impl.ErrorRecordDaoImpl.DESCRIPTION_COLUMN_NAME;

import java.util.Map;

import com.google.common.collect.ImmutableMap;

import org.folio.dao.util.DaoUtil;
import org.folio.rest.jaxrs.model.ErrorRecord;

public class ErrorRecordQuery extends AbstractEntityQuery {

  private static final Map<String, String> ptc = DaoUtil.getBasicContentPropertyToColumnMap();
  
  static {
    ptc.put("description", DESCRIPTION_COLUMN_NAME);
  }

  private ErrorRecordQuery() {
    super(ImmutableMap.copyOf(ptc), ErrorRecord.class);
  }

  public static ErrorRecordQuery query() {
    return new ErrorRecordQuery();
  }

}