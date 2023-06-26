package org.folio.services.util;


import org.junit.Test;

import static org.junit.Assert.assertEquals;

class EventHandlingUtilTest {

  private static final String ENV = "env";
  private static final String EVENT = "event";
  private static final String TENANT = "tenant";

  @Test
  void shouldCreateSubscriptionPattern() {
    var expected = String.format("%s\\.\\w{1,}\\.%s", ENV, EVENT);
    var actual = EventHandlingUtil.createSubscriptionPattern(ENV, EVENT);

    assertEquals(expected, actual);
  }
}
