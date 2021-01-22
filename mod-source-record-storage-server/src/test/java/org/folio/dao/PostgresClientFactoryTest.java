package org.folio.dao;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

@RunWith(BlockJUnit4ClassRunner.class)
public class PostgresClientFactoryTest {

  @Test
  public void shouldSetConfigFilePath() {
    PostgresClientFactory.setConfigFilePath("/postgres-conf-local.json");
    assertEquals("/postgres-conf-local.json", PostgresClientFactory.getConfigFilePath());
  }

}
