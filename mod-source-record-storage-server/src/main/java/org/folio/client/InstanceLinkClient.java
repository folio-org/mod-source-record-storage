package org.folio.client;

import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.InstanceLinkDtoCollection;
import org.folio.client.support.Context;
import org.folio.client.support.OkapiHttpClient;
import org.folio.services.exceptions.InstanceLinksException;
import org.springframework.stereotype.Component;

@Component
public class InstanceLinkClient {
  private static final String PATH = "links/instances";
  private static final Logger LOGGER = LogManager.getLogger(InstanceLinkClient.class);

  private final Vertx vertx;
  private final Function<Context, OkapiHttpClient> okapiHttpClientCreator;


  public InstanceLinkClient(Vertx vertx) {
    this.vertx = vertx;
    this.okapiHttpClientCreator = this::createOkapiHttpClient;
  }

  public CompletableFuture<Optional<InstanceLinkDtoCollection>> getLinksByInstanceId(String instanceId, Context context) {
    LOGGER.trace("Trying to get InstanceLinkDtoCollection for okapi url: {}, tenantId: {}, instanceId: {}",
      context.getOkapiLocation(), context.getTenantId(), instanceId);
    OkapiHttpClient client = okapiHttpClientCreator.apply(context);

    return client.get(buildUrl(instanceId, context))
      .toCompletableFuture()
      .thenCompose(httpResponse -> {
        if (httpResponse.getStatusCode() == HttpStatus.SC_OK) {
          LOGGER.debug("InstanceLinkDtoCollection was loaded for instanceId '{}'", instanceId);
          InstanceLinkDtoCollection instanceLinkDtoCollection = Json.decodeValue(httpResponse.getBody(), InstanceLinkDtoCollection.class);
          return CompletableFuture.completedFuture(Optional.ofNullable(instanceLinkDtoCollection));
        } else if (httpResponse.getStatusCode() == HttpStatus.SC_NOT_FOUND) {
          LOGGER.warn("InstanceLinkDtoCollection was not found by instanceId '{}'", instanceId);
          return CompletableFuture.completedFuture(Optional.empty());
        } else {
          String message = String.format(
            "Error loading InstanceLinkDtoCollection by instanceId: '%s', status code: %s, response message: %s",
            instanceId, httpResponse.getStatusCode(), httpResponse.getBody());
          LOGGER.warn(message);
          return CompletableFuture.failedFuture(new InstanceLinksException(message));
        }
      });
  }

  public CompletableFuture<Void> updateInstanceLinks(String instanceId, InstanceLinkDtoCollection instanceLinkCollection, Context context) {
    LOGGER.trace("Trying to put InstanceLinkDtoCollection for okapi url: {}, tenantId: {}, instanceId: {}",
      context.getOkapiLocation(), context.getTenantId(), instanceId);
    OkapiHttpClient client = okapiHttpClientCreator.apply(context);
    JsonObject requestBody = JsonObject.mapFrom(instanceLinkCollection);
    return client.put(buildUrl(instanceId, context), requestBody)
      .toCompletableFuture()
      .thenAccept(httpResponse -> {
        if (httpResponse.getStatusCode() == HttpStatus.SC_ACCEPTED) {
          LOGGER.debug("InstanceLinkDtoCollection was updated successfully for instanceId '{}'", instanceId);
        } else {
          String message = String.format(
            "Error updating InstanceLinkDtoCollection by instanceId: '%s', status code: %s, response message: %s",
            instanceId, httpResponse.getStatusCode(), httpResponse.getBody());
          LOGGER.warn(message);
        }
      });
  }

  private OkapiHttpClient createOkapiHttpClient(Context context) {
    try {
      return new OkapiHttpClient(vertx, new URL(context.getOkapiLocation()),
        context.getTenantId(), context.getToken(), null, null, null);
    } catch (MalformedURLException e) {
      LOGGER.error("Invalid Okapi URL: '{}'", context.getOkapiLocation());
      throw new InstanceLinksException("Invalid Okapi URL");
    }
  }

  private String buildUrl(String instanceId, Context context) {
    return String.format("%s/%s/%s", context.getOkapiLocation(), PATH, instanceId);
  }
}
