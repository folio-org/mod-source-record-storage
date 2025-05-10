package org.folio.services.caches;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

import io.vertx.core.json.Json;
import io.vertx.junit5.RunTestOnContext;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import java.util.Map;
import java.util.concurrent.CompletionException;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.rest.jaxrs.model.MappingMetadataDto;
import org.folio.services.exceptions.CacheLoadingException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

@ExtendWith(VertxExtension.class)
class MappingParametersSnapshotCacheTest {

  @RegisterExtension
  static RunTestOnContext runTestOnContext = new RunTestOnContext();

  static MappingParametersSnapshotCache cache;
  static OkapiConnectionParams params;

  @BeforeAll
  static void setUp(VertxTestContext vtc) {
    var vertx = runTestOnContext.vertx();
    cache = new MappingParametersSnapshotCache(vertx);

    vertx.createHttpServer()
    .requestHandler(req -> {
      switch(req.path()) {
        case "/mapping-metadata/200":
          var mappingParameters = new MappingParameters().withTenantConfigurationZone("ozone");
          var mappingMetadata = new MappingMetadataDto().withMappingParams(Json.encode(mappingParameters));
          req.response().setStatusCode(200).end(Json.encode(mappingMetadata));
          return;
        case "/mapping-metadata/404":
          req.response().setStatusCode(404).end("not found");
          return;
        default:
          req.response().setStatusCode(500).end("internal server error for path " + req.path());
          return;
      }
    })
    .listen(0)
    .onComplete(vtc.succeeding(server -> {
      Map<String, String> okapiHeaders = Map.of("x-okapi-url", "http://localhost:" + server.actualPort());
      params = new OkapiConnectionParams(okapiHeaders, vertx);
      vtc.completeNow();
    }));
  }

  @Test
  void success(VertxTestContext vtc) {
    cache.get("200", params)
    .onComplete(vtc.succeeding(result -> {
      var mappingParameters = result.get();
      assertThat(mappingParameters.getTenantConfigurationZone(), is("ozone"));
      vtc.completeNow();
    }));
  }

  @Test
  void notFound(VertxTestContext vtc) {
    cache.get("404", params)
    .onComplete(vtc.succeeding(result -> {
      assertThat(result.isEmpty(), is(true));
      vtc.completeNow();
    }));
  }

  @Test
  void exception(VertxTestContext vtc) {
    cache.get("999", params)
    .onComplete(vtc.failing(e -> {
      assertThat(e, is(instanceOf(CompletionException.class)));
      var cause = e.getCause();
      assertThat(cause, is(instanceOf(CacheLoadingException.class)));
      assertThat(cause.getMessage(), containsString("jobExecutionId: '999', status code: 500"));
      vtc.completeNow();
    }));
  }

  @Test
  void nullId(VertxTestContext vtc) {
    cache.get(null, params)
    .onComplete(vtc.failing(e -> {
      assertThat(e, is(instanceOf(NullPointerException.class)));
      vtc.completeNow();
    }));
  }

}
