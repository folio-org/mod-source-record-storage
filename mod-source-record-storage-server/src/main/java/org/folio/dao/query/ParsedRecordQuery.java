package org.folio.dao.query;

import java.util.Map;

import com.google.common.collect.ImmutableMap;

import org.folio.dao.util.DaoUtil;
import org.folio.rest.jaxrs.model.ParsedRecord;

public class ParsedRecordQuery extends AbstractEntityQuery {

  private static final Map<String, String> ptc = DaoUtil.getImmutableContentPropertyToColumnMap();

  private ParsedRecordQuery() {
    super(ImmutableMap.copyOf(ptc), ParsedRecord.class);
  }

  public static ParsedRecordQuery query() {
    return new ParsedRecordQuery();
  }

}