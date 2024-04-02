package org.folio.rest.persist;

import io.vertx.core.json.JsonObject;
import org.junit.Test;

import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class PgConnectOptionsHelperTest {

  @Test
  public void testPgConnectOptions() {
    var host = "localhost";
    var port = 5555;
    var user = "user";
    var password = "password";
    var database = "database";

    var config = JsonObject.mapFrom(Map.of(
      "host", host,
      "port", port,
      "username", user,
      "password", password,
      "database", database
    ));

    var options = PgConnectOptionsHelper.createPgConnectOptions(config);
    assertThat(options.getHost(), is(host));
    assertThat(options.getPort(), is(port));
    assertThat(options.getUser(), is(user));
    assertThat(options.getPassword(), is(password));
    assertThat(options.getDatabase(), is(database));
  }
}
