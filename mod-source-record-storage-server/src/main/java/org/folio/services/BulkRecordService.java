package org.folio.services;

import io.vertx.core.http.HttpServerResponse;
import org.folio.rest.jaxrs.model.SearchRecordRqBody;
import org.folio.rest.util.OkapiConnectionParams;

public interface BulkRecordService {

  void searchRecords(HttpServerResponse response, SearchRecordRqBody searchRecordBody, OkapiConnectionParams okapiParams);
}
