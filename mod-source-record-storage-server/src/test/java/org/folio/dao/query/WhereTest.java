package org.folio.dao.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.folio.dao.query.Where.Op;
import org.folio.rest.jaxrs.model.Record;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class WhereTest {

  @Test
  public void shouldCreateWhereEqual() {
    UUID id = UUID.randomUUID();
    Where where = Where.equal("id", id);
    assertEquals("id", where.getProperty());
    assertEquals(id, where.getValue());
    assertEquals(Op.EQUAL, where.getOp());
  }

  @Test
  public void shouldCreateWhereGreaterThen() {
    Date date = new Date(1589218979000l);
    Where where = Where.greaterThen("processingStartedDate", date);
    assertEquals("processingStartedDate", where.getProperty());
    assertEquals(date, where.getValue());
    assertEquals(Op.GREATER_THEN, where.getOp());
  }

  @Test
  public void shouldCreateWhereLessThen() {
    Date date = new Date(1589218979000l);
    Where where = Where.lessThen("processingStartedDate", date);
    assertEquals("processingStartedDate", where.getProperty());
    assertEquals(date, where.getValue());
    assertEquals(Op.LESS_THEN, where.getOp());
  }

  @Test
  public void shouldCreateWhereGreaterThenOrEqual() {
    Date date = new Date(1589218979000l);
    Where where = Where.greaterThenOrEqual("processingStartedDate", date);
    assertEquals("processingStartedDate", where.getProperty());
    assertEquals(date, where.getValue());
    assertEquals(Op.GREATER_THEN_OR_EQUAL, where.getOp());
  }

  @Test
  public void shouldCreateWhereLessThenOrEqual() {
    Date date = new Date(1589218979000l);
    Where where = Where.lessThenOrEqual("processingStartedDate", date);
    assertEquals("processingStartedDate", where.getProperty());
    assertEquals(date, where.getValue());
    assertEquals(Op.LESS_THEN_OR_EQUAL, where.getOp());
  }

  @Test
  public void shouldCreateWhereNotEqual() {
    Where where = Where.notEqual("state", Record.State.ACTUAL);
    assertEquals("state", where.getProperty());
    assertEquals(Record.State.ACTUAL, where.getValue());
    assertEquals(Op.NOT_EQUAL, where.getOp());
  }

  @Test
  public void shouldCreateWhereLike() {
    Where where = Where.like("description", "%test%");
    assertEquals("description", where.getProperty());
    assertEquals("%test%", where.getValue());
    assertEquals(Op.LIKE, where.getOp());
  }

  @Test
  public void shouldCreateWhereBetween() {
    Date from = new Date(1589218979000l);
    Date to = new Date(1589463489650l);
    Where where = Where.between("processingStartedDate", from, to);
    assertEquals("processingStartedDate", where.getProperty());
    assertEquals(from, ((Object[]) where.getValue())[0]);
    assertEquals(to, ((Object[]) where.getValue())[1]);
    assertEquals(Op.BETWEEN, where.getOp());
  }

  @Test
  public void shouldCreateWhereIn() {
    List<UUID> ids = new ArrayList<>();
    ids.add(UUID.randomUUID());
    ids.add(UUID.randomUUID());
    ids.add(UUID.randomUUID());
    ids.add(UUID.randomUUID());
    ids.add(UUID.randomUUID());
    Where where = Where.in("id", ids);
    assertEquals("id", where.getProperty());
    assertEquals(ids, where.getValue());
    assertEquals(Op.IN, where.getOp());
  }

  @Test
  public void shouldCreateWhereAnd() {
    Where where = Where.and();
    assertNull(where.getProperty());
    assertNull(where.getValue());
    assertEquals(Op.AND, where.getOp());
  }

  @Test
  public void shouldCreateWhereOr() {
    Where where = Where.or();
    assertNull(where.getProperty());
    assertNull(where.getValue());
    assertEquals(Op.OR, where.getOp());
  }

  @Test
  public void shouldCreateWhereStartExpression() {
    Where where = Where.startExpression();
    assertNull(where.getProperty());
    assertNull(where.getValue());
    assertEquals(Op.START_EXPRESSION, where.getOp());
  }

  @Test
  public void shouldCreateWhereEndExpression() {
    Where where = Where.endExpression();
    assertNull(where.getProperty());
    assertNull(where.getValue());
    assertEquals(Op.END_EXPRESSION, where.getOp());
  }

}