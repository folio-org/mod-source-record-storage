package org.folio.dao.util;

import java.time.OffsetDateTime;
import java.util.Date;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.FastDateFormat;
import org.folio.dao.query.EntityQuery;
import org.folio.dao.query.OrderBy;
import org.folio.rest.jaxrs.model.Metadata;

import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;

/**
 * Utility class for hosting DAO constants
 */
public class DaoUtil {

  // NOTE: wherever this is used should be converted to sqlTemplate and Tuple to allow typing sql build
  public static final FastDateFormat DATE_FORMATTER = DateFormatUtils.ISO_8601_EXTENDED_DATETIME_TIME_ZONE_FORMAT;

  public static final String GET_BY_ID_SQL_TEMPLATE = "SELECT %s FROM %s WHERE id = '%s';";
  public static final String GET_BY_WHERE_SQL_TEMPLATE = "SELECT %s FROM %s WHERE %s = '%s';";
  public static final String GET_BY_FILTER_SQL_TEMPLATE = "SELECT %s, count(*) OVER() total_count FROM %s %s %s OFFSET %s LIMIT %s;";
  public static final String SAVE_SQL_TEMPLATE = "INSERT INTO %s (%s) VALUES (%s);";
  public static final String UPDATE_SQL_TEMPLATE = "UPDATE %s SET (%s) = (%s) WHERE id = '%s';";
  public static final String DELETE_SQL_TEMPLATE = "DELETE FROM %s WHERE id = '%s';";

  public static final String SNAPSHOTS_TABLE_NAME = "snapshots_lb";
  public static final String RECORDS_TABLE_NAME = "records_lb";
  public static final String RAW_RECORDS_TABLE_NAME = "raw_records_lb";
  public static final String PARSED_RECORDS_TABLE_NAME = "marc_records_lb";
  public static final String ERROR_RECORDS_TABLE_NAME = "error_records_lb";

  public static final String ID_COLUMN_NAME = "id";
  public static final String JSONB_COLUMN_NAME = "jsonb";
  public static final String CONTENT_COLUMN_NAME = "content";

  public static final String TOTAL_COUNT_COLUMN_NAME = "total_count";

  public static final String COMMA = ",";
  public static final String VALUE_TEMPLATE_TEMPLATE = "$%s";

  public static final String SINGLE_QUOTE = "'";
  public static final String ESCAPED_SINGLE_QUOTE = "''";
  public static final String NEW_LINE = "\n";

  public static final String WRAPPED_TEMPLATE = "'%s'";
  public static final String UNWRAPPED_TEMPLATE = "%s";

  public static final String SPACE = " ";
  public static final String SPACED_AND = " AND ";
  public static final String ORDER_BY_TEMPLATE = "ORDER BY %s";
  public static final String WHERE_TEMPLATE = "WHERE %s";
  public static final String COLUMN_EQUALS_TEMPLATE = "%s = ";

  public static final Pattern BOOLEAN_PATTERN = Pattern.compile("true|false", Pattern.CASE_INSENSITIVE);

  private DaoUtil() { }

  /**
   * Creates {@link Metadata} from row and array of column names
   * 
   * @param row     result {@link Row}
   * @param columns array of column names, must be exactly 4
   * @return {@link Metadata}
   */
  public static Metadata metadataFromRow(Row row, String[] columns) {
    Metadata metadata = new Metadata();
    UUID createdByUserId = row.getUUID(columns[0]);
    if (Objects.nonNull(createdByUserId)) {
      metadata.setCreatedByUserId(createdByUserId.toString());
    }
    OffsetDateTime createdDate = row.getOffsetDateTime(columns[1]);
    if (Objects.nonNull(createdDate)) {
      metadata.setCreatedDate(Date.from(createdDate.toInstant()));
    }
    UUID updatedByUserId = row.getUUID(columns[2]);
    if (Objects.nonNull(updatedByUserId)) {
      metadata.setUpdatedByUserId(updatedByUserId.toString());
    }
    OffsetDateTime updatedDate = row.getOffsetDateTime(columns[3]);
    if (Objects.nonNull(updatedDate)) {
      metadata.setUpdatedDate(Date.from(updatedDate.toInstant()));
    }
    return metadata;
  }

  public static Integer getTotalRecords(RowSet<Row> rowSet) {
    if (rowSet.rowCount() > 0) {
      return rowSet.iterator().next().getInteger(TOTAL_COUNT_COLUMN_NAME);
    }
    // returning -1 to indicate unknown total count
    return -1;
  }

  public static boolean equals(Set<OrderBy> sort1, Set<OrderBy> sort2) {
    return sort1.size() == sort2.size() && sort1.containsAll(sort2);
  }

  public static boolean equals(EntityQuery entityQuery, Object other) {
    return Objects.nonNull(other) && DaoUtil.equals(entityQuery.getSort(), ((EntityQuery) other).getSort());
  }

}