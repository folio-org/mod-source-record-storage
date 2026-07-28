package org.folio.services.util;

import io.vertx.core.Vertx;
import org.apache.commons.lang3.StringUtils;
import org.folio.DataImportEventPayload;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.processing.exceptions.EventProcessingException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;
import static org.folio.dataimport.util.RestUtil.OKAPI_TENANT_HEADER;
import static org.folio.dataimport.util.RestUtil.OKAPI_TOKEN_HEADER;
import static org.folio.dataimport.util.RestUtil.OKAPI_URL_HEADER;
import static org.folio.services.util.EventHandlingUtil.OKAPI_REQUEST_HEADER;
import static org.folio.services.util.EventHandlingUtil.OKAPI_USER_HEADER;

public final class RestUtil {

  private RestUtil() {
  }

  public static Map<String, String> extractHeaders(DataImportEventPayload eventPayload) {
    Map<String, String> headers = new HashMap<>();
    List<String> missingHeaders = new ArrayList<>();

    putOrReportMissing(headers, missingHeaders, OKAPI_URL_HEADER, eventPayload.getOkapiUrl());
    putOrReportMissing(headers, missingHeaders, OKAPI_TENANT_HEADER, eventPayload.getTenant());

    if (!missingHeaders.isEmpty()) {
      throw new EventProcessingException(format(
        "retrieveOkapiConnectionParams:: Cannot build OkapiConnectionParams, missing required value(s): %s for eventType: '%s', jobExecutionId: '%s'",
        missingHeaders, eventPayload.getEventType(), eventPayload.getJobExecutionId()));
    }

    if (StringUtils.isNotBlank(eventPayload.getToken())) {
      headers.put(OKAPI_TOKEN_HEADER, eventPayload.getToken());
    }

    String userId = eventPayload.getContext().get(OKAPI_USER_HEADER);
    if (StringUtils.isNotBlank(userId)) {
      headers.put(OKAPI_USER_HEADER, userId);
    }
    String requestId = eventPayload.getContext().get(OKAPI_REQUEST_HEADER);
    if (StringUtils.isNotBlank(requestId)) {
      headers.put(OKAPI_REQUEST_HEADER, requestId);
    }
    return headers;
  }

  public static OkapiConnectionParams retrieveOkapiConnectionParams(DataImportEventPayload eventPayload, Vertx vertx) {
    var okapiHeaders = extractHeaders(eventPayload);
    return OkapiConnectionParams.createSystemUserConnectionParams(okapiHeaders, vertx);
  }

  private static void putOrReportMissing(Map<String, String> headers, List<String> missingHeaders, String headerName, String value) {
    if (StringUtils.isBlank(value)) {
      missingHeaders.add(headerName);
    } else {
      headers.put(headerName, value);
    }
  }
}
