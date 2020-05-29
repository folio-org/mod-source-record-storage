package org.folio.services;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class LBRecordServiceTest extends AbstractLBServiceTest {

  private LBRecordService recordService;

  @Before
  public void setUp(TestContext context) {
    recordService = new LBRecordServiceImpl(postgresClientFactory);
  }

  @Test
  public void shouldBeTrue(TestContext context) {
    context.assertTrue(true);
  }

}