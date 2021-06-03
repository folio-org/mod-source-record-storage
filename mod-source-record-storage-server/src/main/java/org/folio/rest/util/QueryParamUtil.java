package org.folio.rest.util;

import static java.lang.String.format;

import javax.ws.rs.BadRequestException;

import org.codehaus.plexus.util.StringUtils;
import org.folio.dao.util.ExternalIdType;
import org.folio.dao.util.RecordType;

public final class QueryParamUtil {

  private QueryParamUtil() { }

  /**
   * Tries to convert string to {@link ExternalIdType}. Returns default RECORD if null or empty.
   *
   * @param externalIdType external id type as string
   * @return external id type
   */
  public static ExternalIdType toExternalIdType(String externalIdType) {
    if (StringUtils.isNotEmpty(externalIdType)) {
      try {
        return ExternalIdType.valueOf(externalIdType);
      } catch (Exception e) {
        throw new BadRequestException(format("Unknown id type %s", externalIdType));
      }
    }
    return ExternalIdType.RECORD;
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

}
