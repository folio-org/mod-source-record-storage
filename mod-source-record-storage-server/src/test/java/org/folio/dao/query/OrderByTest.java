package org.folio.dao.query;

import static org.junit.Assert.assertEquals;

import org.folio.dao.query.OrderBy.Direction;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class OrderByTest {

  @Test
  public void shouldCreateOrderBy() {
    OrderBy orderBy = OrderBy.by("id");
    assertEquals("id", orderBy.getProperty());
    assertEquals(Direction.ASC, orderBy.getDirection());
  }

  @Test
  public void shouldCreateOrderByWithDirectory() {
    OrderBy orderBy = OrderBy.by("id", Direction.DESC);
    assertEquals("id", orderBy.getProperty());
    assertEquals(Direction.DESC, orderBy.getDirection());
  }

}