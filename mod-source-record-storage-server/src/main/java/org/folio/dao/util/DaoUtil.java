package org.folio.dao.util;

import java.time.Instant;
import java.util.Date;
import java.util.Objects;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.FastDateFormat;
import org.folio.dao.query.EntityQuery;
import org.folio.dao.query.OrderBy;
import org.folio.rest.jaxrs.model.Metadata;

import io.vertx.core.json.JsonArray;
import io.vertx.ext.sql.ResultSet;

/**
 * Utility class for hosting DAO constants
 */
public class DaoUtil {

  public static final FastDateFormat DATE_FORMATTER = DateFormatUtils.ISO_8601_EXTENDED_DATETIME_TIME_ZONE_FORMAT;

  public static final String GET_BY_ID_SQL_TEMPLATE = "SELECT %s FROM %s WHERE id = '%s';";
  public static final String GET_BY_WHERE_SQL_TEMPLATE = "SELECT %s FROM %s WHERE %s = '%s';";
  public static final String GET_BY_QUERY_SQL_TEMPLATE = "SELECT %s FROM %s %s %s OFFSET %s LIMIT %s;";
  public static final String GET_BY_QUERY_WITH_TOTAL_SQL_TEMPLATE = "WITH cte AS (SELECT %s FROM %s %s) " +
    "SELECT * FROM (TABLE cte %s OFFSET %s LIMIT %s) sub " +
    "RIGHT JOIN (SELECT count(*) FROM cte) c(total_count) ON true;";

  public static final String SAVE_SQL_TEMPLATE = "INSERT INTO %s (%s) VALUES (%s);";
  public static final String UPDATE_SQL_TEMPLATE = "UPDATE %s SET (%s) = (%s) WHERE id = '%s';";
  public static final String DELETE_SQL_TEMPLATE = "DELETE FROM %s WHERE id = '%s';";

  public static final String SNAPSHOTS_TABLE_NAME = "snapshots_lb";
  public static final String RECORDS_TABLE_NAME = "records_lb";
  public static final String RAW_RECORDS_TABLE_NAME = "raw_records_lb";
  public static final String PARSED_RECORDS_TABLE_NAME = "marc_records_lb";
  public static final String ERROR_RECORDS_TABLE_NAME = "error_records_lb";

  public static final String ID_COLUMN_NAME = "id";
  public static final String JSON_COLUMN_NAME = "jsonb";
  public static final String CONTENT_COLUMN_NAME = "content";

  public static final String TOTAL_COUNT_COLUMN_NAME = "total_count";

  public static final String COMMA = ",";
  public static final String QUESTION_MARK = "?";

  public static final String WRAPPED_TEMPLATE = "'%s'";
  public static final String UNWRAPPED_TEMPLATE = "%s";

  public static final String SPACE = " ";
  public static final String SPACED_AND = " AND ";
  public static final String ORDER_BY_TEMPLATE = "ORDER BY %s";
  public static final String WHERE_TEMPLATE = "WHERE %s";
  public static final String COLUMN_EQUALS_TEMPLATE = "%s = ";

  private DaoUtil() { }

  public static Metadata metadataFromJsonArray(JsonArray row, int[] positions) {
    Metadata metadata = new Metadata();
    String createdByUserId = row.getString(positions[0]);
    if (StringUtils.isNotEmpty(createdByUserId)) {
      metadata.setCreatedByUserId(createdByUserId);
    }
    Instant createdDate = row.getInstant(positions[1]);
    if (Objects.nonNull(createdDate)) {
      metadata.setCreatedDate(Date.from(createdDate));
    }
    String updatedByUserId = row.getString(positions[2]);
    if (StringUtils.isNotEmpty(updatedByUserId)) {
      metadata.setUpdatedByUserId(updatedByUserId);
    }
    Instant updatedDate = row.getInstant(positions[3]);
    if (Objects.nonNull(updatedDate)) {
      metadata.setUpdatedDate(Date.from(updatedDate));
    }
    return metadata;
  }

  public static boolean hasRecords(ResultSet resultSet) {
    return resultSet.getNumRows() >= 1 && Objects.nonNull(resultSet.getRows().get(0).getString(ID_COLUMN_NAME));
  }

  public static Integer getTotalRecords(ResultSet resultSet) {
    if (resultSet.getNumRows() > 0) {
      return resultSet.getRows().get(0).getInteger(TOTAL_COUNT_COLUMN_NAME);
    }
    // returning -1 to indicate unknown total count
    return -1; // this should not occur
  }

  public static boolean equals(Set<OrderBy> sort1, Set<OrderBy> sort2) {
    return sort1.size() == sort2.size() && sort1.containsAll(sort2);
  }

  public static boolean equals(EntityQuery entityQuery, Object other) {
    return Objects.nonNull(other) && DaoUtil.equals(entityQuery.getSort(), ((EntityQuery) other).getSort());
  }

}