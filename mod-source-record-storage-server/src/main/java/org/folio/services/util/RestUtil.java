package org.folio.services.util;

import io.vertx.core.Vertx;
import org.apache.commons.lang3.StringUtils;
import org.folio.DataImportEventPayload;
import org.folio.dataimport.util.OkapiConnectionParams;

import java.util.HashMap;
import java.util.Map;

import static org.folio.dataimport.util.RestUtil.OKAPI_TENANT_HEADER;
import static org.folio.dataimport.util.RestUtil.OKAPI_TOKEN_HEADER;
import static org.folio.dataimport.util.RestUtil.OKAPI_URL_HEADER;
import static org.folio.services.util.EventHandlingUtil.OKAPI_REQUEST_HEADER;
import static org.folio.services.util.EventHandlingUtil.OKAPI_USER_HEADER;

public final class RestUtil {

  private RestUtil() {
  }

  public static OkapiConnectionParams retrieveOkapiConnectionParams(DataImportEventPayload eventPayload, Vertx vertx) {
    Map<String, String> okapiHeaders = new HashMap<>(Map.of(
      OKAPI_URL_HEADER, eventPayload.getOkapiUrl(),
      OKAPI_TENANT_HEADER, eventPayload.getTenant(),
      OKAPI_TOKEN_HEADER, eventPayload.getToken()
    ));

    String userId = eventPayload.getContext().get(OKAPI_USER_HEADER);
    if (StringUtils.isNotBlank(userId)) {
      okapiHeaders.put(OKAPI_USER_HEADER, userId);
    }
    String requestId = eventPayload.getContext().get(OKAPI_REQUEST_HEADER);
    if (StringUtils.isNotBlank(requestId)) {
      okapiHeaders.put(OKAPI_REQUEST_HEADER, requestId);
    }
    return OkapiConnectionParams.createSystemUserConnectionParams(okapiHeaders, vertx);
  }
}
