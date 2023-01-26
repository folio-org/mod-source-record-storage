package org.folio.client;

import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.Json;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.InstanceLinkDtoCollection;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.dataimport.util.RestUtil;
import org.folio.services.exceptions.InstanceLinksException;
import org.springframework.stereotype.Component;

@Component
public class InstanceLinkClient {
  private static final Logger LOGGER = LogManager.getLogger(InstanceLinkClient.class);

  public CompletableFuture<Optional<InstanceLinkDtoCollection>> getLinksByInstanceId(String instanceId, OkapiConnectionParams params) {
    LOGGER.trace("Trying to get InstanceLinkDtoCollection for okapi url: {}, tenantId: {}, instanceId: {}",
      params.getOkapiUrl(), params.getTenantId(), instanceId);
    return RestUtil.doRequest(params, "/links/instances/" + instanceId, HttpMethod.GET, null)
      .toCompletionStage()
      .toCompletableFuture()
      .thenCompose(httpResponse -> {
        if (httpResponse.getResponse().statusCode() == HttpStatus.SC_OK) {
          LOGGER.info("getLinksByInstanceId:: InstanceLinkDtoCollection was loaded by instanceId '{}'", instanceId);
          return CompletableFuture.completedFuture(Optional.of(Json.decodeValue(httpResponse.getBody(), InstanceLinkDtoCollection.class)));
        } else if (httpResponse.getResponse().statusCode() == HttpStatus.SC_NOT_FOUND) {
          LOGGER.warn("getLinksByInstanceId:: InstanceLinkDtoCollection was not found by instanceId '{}'", instanceId);
          return CompletableFuture.completedFuture(Optional.empty());
        } else {
          String message = String.format("getLinksByInstanceId:: Error loading InstanceLinkDtoCollection by instanceId: '%s', status code: %s, response message: %s",
            instanceId, httpResponse.getResponse().statusCode(), httpResponse.getBody());
          LOGGER.warn(message);
          return CompletableFuture.failedFuture(new InstanceLinksException(message));
        }
      });
  }

  public CompletableFuture<Void> updateInstanceLinks(String instanceId, InstanceLinkDtoCollection instanceLinkCollection, OkapiConnectionParams params) {
    LOGGER.trace("Trying to put InstanceLinkDtoCollection for okapi url: {}, tenantId: {}, instanceId: {}",
      params.getOkapiUrl(), params.getTenantId(), instanceId);
    return RestUtil.doRequest(params, "/links/instances/" + instanceId, HttpMethod.PUT, instanceLinkCollection)
      .toCompletionStage()
      .toCompletableFuture()
      .thenAccept(httpResponse -> {
        if (httpResponse.getResponse().statusCode() == HttpStatus.SC_ACCEPTED) {
          LOGGER.info("updateInstanceLinks:: InstanceLinkDtoCollection was updated successfully for instanceId '{}'", instanceId);
        } else {
          LOGGER.warn("updateInstanceLinks:: Error updating InstanceLinkDtoCollection by instanceId: '{}', status code:{}, response message: {}",
            instanceId, httpResponse.getResponse().statusCode(), httpResponse.getBody());
        }
      });
  }

}
