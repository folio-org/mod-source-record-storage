package org.folio.services.util;

import org.apache.commons.lang3.StringUtils;
import org.folio.DataImportEventPayload;
import org.folio.dataimport.util.ConnectionParams;

import java.util.HashMap;
import java.util.Map;
import org.folio.okapi.common.XOkapiHeaders;

public final class RestUtil {

  private RestUtil() {
  }

  public static ConnectionParams retrieveOkapiConnectionParams(DataImportEventPayload eventPayload) {
    Map<String, String> okapiHeaders = new HashMap<>(Map.of(
      XOkapiHeaders.URL, eventPayload.getOkapiUrl(),
      XOkapiHeaders.TENANT, eventPayload.getTenant(),
      XOkapiHeaders.TOKEN, eventPayload.getToken()
    ));

    String userId = eventPayload.getContext().get(XOkapiHeaders.USER_ID);
    if (StringUtils.isNotBlank(userId)) {
      okapiHeaders.put(XOkapiHeaders.USER_ID, userId);
    }
    String requestId = eventPayload.getContext().get(XOkapiHeaders.REQUEST_ID);
    if (StringUtils.isNotBlank(requestId)) {
      okapiHeaders.put(XOkapiHeaders.REQUEST_ID, requestId);
    }
    return ConnectionParams.createSystemUserConnectionParams(okapiHeaders);
  }
}
