package org.folio.services.util;

import io.vertx.core.Vertx;
import org.folio.DataImportEventPayload;
import org.folio.dataimport.util.OkapiConnectionParams;

import java.util.Map;

import static org.folio.dataimport.util.RestUtil.OKAPI_TENANT_HEADER;
import static org.folio.dataimport.util.RestUtil.OKAPI_TOKEN_HEADER;
import static org.folio.dataimport.util.RestUtil.OKAPI_URL_HEADER;

public final class RestUtil {

  private RestUtil() {
  }

  public static OkapiConnectionParams retrieveOkapiConnectionParams(DataImportEventPayload eventPayload, Vertx vertx) {
    return OkapiConnectionParams.createSystemUserConnectionParams(Map.of(
      OKAPI_URL_HEADER, eventPayload.getOkapiUrl(),
      OKAPI_TENANT_HEADER, eventPayload.getTenant(),
      OKAPI_TOKEN_HEADER, eventPayload.getToken()
    ), vertx);
  }
}
