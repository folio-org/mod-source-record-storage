package org.folio.dao.util;

import java.time.OffsetDateTime;
import java.util.Date;
import java.util.Objects;
import java.util.UUID;
import java.util.regex.Pattern;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.FastDateFormat;
import org.folio.rest.jaxrs.model.Metadata;

import io.vertx.sqlclient.Row;

/**
 * Utility class for hosting DAO constants
 */
public class DaoUtil {

  public static final FastDateFormat DATE_FORMATTER = DateFormatUtils.ISO_8601_EXTENDED_DATETIME_TIME_ZONE_FORMAT;

  public static final String GET_BY_ID_SQL_TEMPLATE = "SELECT %s FROM %s WHERE id = '%s';";
  public static final String GET_BY_WHERE_SQL_TEMPLATE = "SELECT %s FROM %s WHERE %s = '%s';";
  public static final String GET_BY_FILTER_SQL_TEMPLATE = "SELECT %s FROM %s %s OFFSET %s LIMIT %s;";
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

  public static final String COMMA = ",";
  public static final String VALUE_TEMPLATE_TEMPLATE = "$%s";

  public static final String SINGLE_QUOTE = "'";
  public static final String ESCAPED_SINGLE_QUOTE = "''";
  public static final String NEW_LINE = "\n";

  public static final String WRAPPED_TEMPLATE = "'%s'";
  public static final String UNWRAPPED_TEMPLATE = "%s";

  public static final String SPACED_AND = " AND ";
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

}