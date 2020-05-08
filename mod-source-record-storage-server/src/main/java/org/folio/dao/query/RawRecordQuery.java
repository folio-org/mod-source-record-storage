package org.folio.dao.query;

import static org.folio.dao.util.DaoUtil.CONTENT_COLUMN_NAME;
import static org.folio.dao.util.DaoUtil.ID_COLUMN_NAME;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.lang3.StringUtils;
import org.folio.dao.util.WhereClauseBuilder;
import org.folio.rest.jaxrs.model.RawRecord;

public class RawRecordQuery extends RawRecord implements EntityQuery {

  private final Map<String, String> propertyToColumn = new HashMap<>();

  public RawRecordQuery() {
    propertyToColumn.put("id", ID_COLUMN_NAME);
    propertyToColumn.put("content", CONTENT_COLUMN_NAME);
  }

  @Override
  public String toWhereClause() {
    return WhereClauseBuilder.of()
      .append(getId(), ID_COLUMN_NAME)
      .build();
  }

  @Override
  public String toOrderByClause() {
    return StringUtils.EMPTY;
  }

  @Override
  public Optional<String> getPropertyColumnName(String property) {
    return Optional.ofNullable(propertyToColumn.get(property));
  }

}