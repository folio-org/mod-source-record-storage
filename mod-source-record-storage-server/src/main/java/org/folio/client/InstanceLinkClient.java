package org.folio.client;

import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.Json;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.InstanceLinkDtoCollection;
import org.folio.LinkingRuleDto;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.dataimport.util.RestUtil;
import org.folio.services.exceptions.InstanceLinksException;
import org.springframework.stereotype.Component;

@Component
public class InstanceLinkClient {
  private static final Logger LOGGER = LogManager.getLogger(InstanceLinkClient.class);

  public CompletableFuture<Optional<InstanceLinkDtoCollection>> getLinksByInstanceId(String instanceId,
                                                                                     OkapiConnectionParams params) {
    LOGGER.trace("Trying to get InstanceLinkDtoCollection for okapi url: {}, tenantId: {}, instanceId: {}",
      params.getOkapiUrl(), params.getTenantId(), instanceId);
    return RestUtil.doRequestWithSystemUser(params, "/links/instances/" + instanceId, HttpMethod.GET, null)
      .toCompletionStage()
      .toCompletableFuture()
      .thenCompose(httpResponse -> {
        if (httpResponse.getResponse().statusCode() == HttpStatus.SC_OK) {
          LOGGER.info("getLinksByInstanceId:: InstanceLinkDtoCollection was loaded by instanceId '{}'", instanceId);
          return CompletableFuture.completedFuture(
            Optional.of(Json.decodeValue(httpResponse.getBody(), InstanceLinkDtoCollection.class)));
        } else if (httpResponse.getResponse().statusCode() == HttpStatus.SC_NOT_FOUND) {
          LOGGER.warn("getLinksByInstanceId:: InstanceLinkDtoCollection was not found by instanceId '{}'", instanceId);
          return CompletableFuture.completedFuture(Optional.empty());
        } else {
          String message = String.format(
            "getLinksByInstanceId:: Error loading InstanceLinkDtoCollection by instanceId: '%s', status code: %s, response message: %s",
            instanceId, httpResponse.getResponse().statusCode(), httpResponse.getBody());
          LOGGER.warn(message);
          return CompletableFuture.failedFuture(new InstanceLinksException(message));
        }
      });
  }

  public CompletableFuture<Optional<List<LinkingRuleDto>>> getLinkingRuleList(OkapiConnectionParams params) {
    LOGGER.trace("Trying to get list of LinkingRule for okapi url: {}, tenantId: {}",
      params.getOkapiUrl(), params.getTenantId());

    return RestUtil.doRequestWithSystemUser(params, "/linking-rules/instance-authority", HttpMethod.GET, null)
      .toCompletionStage()
      .toCompletableFuture()
      .thenCompose(httpResponse -> {
        if (HttpStatus.SC_OK == httpResponse.getResponse().statusCode()) {
          LOGGER.info("getLinkingRuleList:: LinkingRuleDto list was loaded '{}'", httpResponse.getResponse().statusCode());
          return CompletableFuture.completedFuture(
            Optional.of(Arrays.asList(Json.decodeValue(httpResponse.getBody(), LinkingRuleDto[].class))));
        } else if (httpResponse.getResponse().statusCode() == HttpStatus.SC_NOT_FOUND) {
          LOGGER.warn("getLinkingRuleList:: no LinkingRuleDto was found '{}'", httpResponse.getResponse().statusCode());
          return CompletableFuture.completedFuture(Optional.empty());
        } else {
          String message = String.format(
            "getLinkingRuleList:: Error loading LinkingRuleDto list status code: %s, response message: %s",
            httpResponse.getResponse().statusCode(), httpResponse.getBody());
          LOGGER.warn(message);
          return CompletableFuture.failedFuture(new InstanceLinksException(message));
        }
      });
  }

  public CompletableFuture<Void> updateInstanceLinks(String instanceId,
                                                     InstanceLinkDtoCollection instanceLinkCollection,
                                                     OkapiConnectionParams params) {
    LOGGER.trace("Trying to put InstanceLinkDtoCollection for okapi url: {}, tenantId: {}, instanceId: {}",
      params.getOkapiUrl(), params.getTenantId(), instanceId);
    return RestUtil.doRequestWithSystemUser(params, "/links/instances/" + instanceId, HttpMethod.PUT, instanceLinkCollection)
      .toCompletionStage()
      .toCompletableFuture()
      .thenAccept(httpResponse -> {
        if (httpResponse.getResponse().statusCode() == HttpStatus.SC_NO_CONTENT) {
          LOGGER.info("updateInstanceLinks:: InstanceLinkDtoCollection was updated successfully for instanceId '{}'",
            instanceId);
        } else {
          LOGGER.warn(
            "updateInstanceLinks:: Error updating InstanceLinkDtoCollection by instanceId: '{}', status code:{}, response message: {}",
            instanceId, httpResponse.getResponse().statusCode(), httpResponse.getBody());
        }
      });
  }

}
