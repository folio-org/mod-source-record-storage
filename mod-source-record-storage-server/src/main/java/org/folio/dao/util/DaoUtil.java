package org.folio.dao.util;

import static org.folio.dao.impl.LBRecordDaoImpl.CREATED_BY_USER_ID_COLUMN_NAME;
import static org.folio.dao.impl.LBRecordDaoImpl.CREATED_DATE_COLUMN_NAME;
import static org.folio.dao.impl.LBRecordDaoImpl.UPDATED_BY_USER_ID_COLUMN_NAME;
import static org.folio.dao.impl.LBRecordDaoImpl.UPDATED_DATE_COLUMN_NAME;

import java.time.OffsetDateTime;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;

import com.google.common.collect.ImmutableMap;

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
  public static final String GET_BY_QUERY_SQL_TEMPLATE = "SELECT %s, count(*) OVER() total_count FROM %s %s %s OFFSET %s LIMIT %s;";
  public static final String SAVE_SQL_TEMPLATE = "INSERT INTO %s (%s) VALUES (%s);";
  public static final String UPDATE_SQL_TEMPLATE = "UPDATE %s SET (%s) = (%s) WHERE id = '%s';";
  public static final String DELETE_SQL_TEMPLATE = "DELETE FROM %s WHERE id = '%s';";

  public static final String SNAPSHOTS_TABLE_NAME = "snapshots_lb";
  public static final String RECORDS_TABLE_NAME = "records_lb";
  public static final String RAW_RECORDS_TABLE_NAME = "raw_records_lb";
  public static final String PARSED_RECORDS_TABLE_NAME = "marc_records_lb";
  public static final String ERROR_RECORDS_TABLE_NAME = "error_records_lb";

  public static final String ID_PROPERTY_NAME = "id";
  public static final String CONTENT_PROPERTY_NAME = "content";

  public static final String ID_COLUMN_NAME = ID_PROPERTY_NAME;
  public static final String JSONB_COLUMN_NAME = "jsonb";
  public static final String CONTENT_COLUMN_NAME = CONTENT_PROPERTY_NAME;

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
   * Creates {@link Metadata} from row
   * 
   * @param row result {@link Row}
   * @return {@link Metadata}
   */
  public static Metadata metadataFromRow(Row row) {
    Metadata metadata = new Metadata();
    UUID createdByUserId = row.getUUID(CREATED_BY_USER_ID_COLUMN_NAME);
    if (Objects.nonNull(createdByUserId)) {
      metadata.setCreatedByUserId(createdByUserId.toString());
    }
    OffsetDateTime createdDate = row.getOffsetDateTime(CREATED_DATE_COLUMN_NAME);
    if (Objects.nonNull(createdDate)) {
      metadata.setCreatedDate(Date.from(createdDate.toInstant()));
    }
    UUID updatedByUserId = row.getUUID(UPDATED_BY_USER_ID_COLUMN_NAME);
    if (Objects.nonNull(updatedByUserId)) {
      metadata.setUpdatedByUserId(updatedByUserId.toString());
    }
    OffsetDateTime updatedDate = row.getOffsetDateTime(UPDATED_DATE_COLUMN_NAME);
    if (Objects.nonNull(updatedDate)) {
      metadata.setUpdatedDate(Date.from(updatedDate.toInstant()));
    }
    return metadata;
  }

  /**
   * Get total records from total count column 'total_count'
   * 
   * @param rowSet query results
   * @return integer number of total row beyond limit
   */
  public static Integer getTotalRecords(RowSet<Row> rowSet) {
    if (rowSet.rowCount() > 0) {
      return rowSet.iterator().next().getInteger(TOTAL_COUNT_COLUMN_NAME);
    }
    // returning -1 to indicate unknown total count
    return -1;
  }

  /**
   * Compare two set of {@link OrderBy}
   * 
   * @param sort1 first sort
   * @param sort2 second sort
   * @return true if sets are equal
   */
  public static boolean equals(Set<OrderBy> sort1, Set<OrderBy> sort2) {
    return sort1.size() == sort2.size() && sort1.containsAll(sort2);
  }

  /**
   * Initial compare of {@link EntityQuery} to another
   * 
   * @param entityQuery entity query
   * @param other       other untyped entity query
   * @return true if other not null and sorts equal
   */
  public static boolean equals(EntityQuery eq1, EntityQuery eq2) {
    return Objects.nonNull(eq2) && DaoUtil.equals(eq1.getSort(), eq2.getSort());
  }

  public static Map<String, String> getBasicContentPropertyToColumnMap() {
    Map<String, String> ptc = new HashMap<>();
    ptc.put(ID_PROPERTY_NAME, ID_COLUMN_NAME);
    ptc.put(CONTENT_PROPERTY_NAME, CONTENT_COLUMN_NAME);
    return ptc;
  }

  public static Map<String, String> getImmutableContentPropertyToColumnMap() {
    return ImmutableMap.copyOf(getBasicContentPropertyToColumnMap());
  }

}