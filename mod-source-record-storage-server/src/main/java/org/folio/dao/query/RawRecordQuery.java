package org.folio.dao.query;

import java.util.Map;

import com.google.common.collect.ImmutableMap;

import org.folio.dao.util.DaoUtil;
import org.folio.rest.jaxrs.model.RawRecord;

public class RawRecordQuery extends AbstractEntityQuery {

  private static final Map<String, String> ptc = DaoUtil.getImmutableContentPropertyToColumnMap();

  private RawRecordQuery() {
    super(ImmutableMap.copyOf(ptc), RawRecord.class);
  }

  public static RawRecordQuery query() {
    return new RawRecordQuery();
  }

}