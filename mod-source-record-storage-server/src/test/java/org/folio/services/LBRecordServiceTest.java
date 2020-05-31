package org.folio.services;

import org.folio.dao.LBRecordDao;
import org.folio.dao.LBRecordDaoImpl;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class LBRecordServiceTest extends AbstractLBServiceTest {

  private LBRecordDao recordDao;

  private LBRecordService recordService;

  @Before
  public void setUp(TestContext context) {
    recordDao = new LBRecordDaoImpl(postgresClientFactory);
    recordService = new LBRecordServiceImpl(recordDao);
  }

  @Test
  public void shouldBeTrue(TestContext context) {
    context.assertTrue(true);
  }

}