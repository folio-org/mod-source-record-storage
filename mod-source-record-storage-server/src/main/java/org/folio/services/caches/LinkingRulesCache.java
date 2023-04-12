package org.folio.services.caches;

import com.github.benmanes.caffeine.cache.AsyncCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.LinkingRuleDto;
import org.folio.client.InstanceLinkClient;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class LinkingRulesCache {
  private static final Logger LOGGER = LogManager.getLogger();

  @Value("${srs.linking-rules-cache.expiration.time.hours:12}")
  private long cacheExpirationTime;

  private final InstanceLinkClient instanceLinkClient;
  private final AsyncCache<String, Optional<List<LinkingRuleDto>>> cache;

  @Autowired
  public LinkingRulesCache(InstanceLinkClient instanceLinkClient, Vertx vertx) {
    this.instanceLinkClient = instanceLinkClient;
    cache = Caffeine.newBuilder()
      .expireAfterWrite(cacheExpirationTime, TimeUnit.HOURS)
      .executor(task -> vertx.runOnContext(v -> task.run()))
      .buildAsync();
  }

  public Future<Optional<List<LinkingRuleDto>>> get(OkapiConnectionParams params) {
    try {
      return Future.fromCompletionStage(cache.get(params.getTenantId(), (key, executor) -> instanceLinkClient.getLinkingRuleList(params)));
    } catch (Exception e) {
      LOGGER.warn("get:: Error loading Linking rules", e);
      return Future.failedFuture(e);
    }
  }
}
