package org.folio.rest.util;

import static java.lang.String.format;

import javax.ws.rs.BadRequestException;

import org.codehaus.plexus.util.StringUtils;

import org.folio.dao.util.IdType;
import org.folio.dao.util.RecordType;
import org.folio.rest.jooq.enums.RecordState;

public final class QueryParamUtil {

  private QueryParamUtil() {}

  /**
   * Tries to convert string to {@link IdType}. Returns default RECORD if null or empty.
   *
   * @param externalIdType external id type as string
   * @return external id type
   */
  public static IdType toExternalIdType(String externalIdType) {
    if (StringUtils.isNotEmpty(externalIdType)) {
      try {
        return IdType.valueOf(externalIdType);
      } catch (Exception e) {
        throw new BadRequestException(format("Unknown id type %s", externalIdType));
      }
    }
    return IdType.RECORD;
  }

  /**
   * Tries to convert string to {@link RecordType}. Returns default MARC if null or empty.
   *
   * @param recordType record type as string
   * @return record type
   */
  public static RecordType toRecordType(String recordType) {
    if (StringUtils.isNotEmpty(recordType)) {
      try {
        return RecordType.valueOf(recordType);
      } catch (Exception e) {
        throw new BadRequestException(format("Unknown record type %s", recordType));
      }
    }
    return RecordType.MARC_BIB;
  }

  /**
   * Tries to convert string to {@link RecordState}. Returns default ACTUAL if null or empty.
   *
   * @param state record state as string
   * @return record state
   */
  public static RecordState toRecordState(String state) {
    if (StringUtils.isNotEmpty(state)) {
      try {
        return RecordState.valueOf(state);
      } catch (Exception e) {
        throw new BadRequestException(format("Unknown record state %s", state));
      }
    }
    return RecordState.ACTUAL;
  }

  /**
   * Returns the first value in the array which is not null.
   * If all the values are null or the array is null or empty then null is returned.
   *
   * @param values the values to test, may be null or empty
   * @return the first value from values which is not null, or null if there are no non-null values
   */
  public static String firstNonEmpty(final String... values) {
    if (values != null) {
      for (final String val : values) {
        if (org.apache.commons.lang3.StringUtils.isNotEmpty(val)) {
          return val;
        }
      }
    }
    return null;
  }

}
