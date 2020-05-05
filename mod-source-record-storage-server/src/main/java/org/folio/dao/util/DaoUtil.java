package org.folio.dao.util;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.FastDateFormat;

/**
 * Utility class for hosting DAO constants
 */
public class DaoUtil {

  public static final FastDateFormat DATE_FORMATTER = DateFormatUtils.ISO_8601_EXTENDED_DATETIME_TIME_ZONE_FORMAT;

  public static final String GET_BY_ID_SQL_TEMPLATE = "SELECT * FROM %s WHERE id = '%s';";
  public static final String GET_BY_FILTER_SQL_TEMPLATE = "SELECT * FROM %s %s OFFSET %s LIMIT %s;";
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

  public static final String COMMA = ",";
  public static final String QUESTION_MARK = "?";

  public static final String WRAPPED_TEMPLATE = "'%s'";
  public static final String UNWRAPPED_TEMPLATE = "%s";

  public static final String SPACED_AND = " AND ";
  public static final String WHERE_TEMPLATE = "WHERE %s";
  public static final String COLUMN_EQUALS_TEMPLATE = "%s = ";

  private DaoUtil() { }

}