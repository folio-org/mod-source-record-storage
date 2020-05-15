package org.folio.dao.query;

import static org.folio.dao.util.DaoUtil.DATE_FORMATTER;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.drools.core.util.StringUtils;
import org.folio.dao.query.OrderBy.Direction;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.Record.RecordType;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class QueryBuilderTest {

  @Test
  public void shouldCreateDefaultRecordQueryBuilder() {
    RecordQuery query = RecordQuery.query();
    assertTrue(query.builder().query() instanceof RecordQuery);
    assertDefaultQueryBuilder(query.builder());
  }

  @Test
  public void shouldCreateDefaultSnapshotQueryBuilder() {
    SnapshotQuery query = SnapshotQuery.query();
    assertTrue(query.builder().query() instanceof SnapshotQuery);
    assertDefaultQueryBuilder(query.builder());
  }

  @Test
  public void shouldCreateDefaultRawRecordQueryBuilder() {
    RawRecordQuery query = RawRecordQuery.query();
    assertTrue(query.builder().query() instanceof RawRecordQuery);
    assertDefaultQueryBuilder(query.builder());
  }

  @Test
  public void shouldCreateDefaultParsedRecordQueryBuilder() {
    ParsedRecordQuery query = ParsedRecordQuery.query();
    assertTrue(query.builder().query() instanceof ParsedRecordQuery);
    assertDefaultQueryBuilder(query.builder());
  }

  @Test
  public void shouldCreateDefaultErrorRecordQueryBuilder() {
    ErrorRecordQuery query = ErrorRecordQuery.query();
    assertTrue(query.builder().query() instanceof ErrorRecordQuery);
    assertDefaultQueryBuilder(query.builder());
  }

  @Test
  public void shouldCreateSimpleRecordQueryBuilder() {
    RecordQuery query = RecordQuery.query();
    QueryBuilder builder = query.builder();
    builder
      .whereLessThenOrEqual("generation", 1)
        .and()
      .whereGreaterThen("order", 1)
      .orderBy("metadata.createdDate", Direction.DESC);
    assertNotNull(builder);
    assertNotNull(builder.query());
    assertTrue(builder.query() instanceof RecordQuery);
    String where = "WHERE generation <= 1 AND orderinfile > 1";
    assertEquals(where, builder.buildWhereClause());
    String orderBy = "ORDER BY createddate DESC";
    assertEquals(orderBy, builder.buildOrderByClause());
  }

  @Test
  public void shouldCreateAdvancedRecordQueryBuilder() {
    RecordQuery query = RecordQuery.query();
    QueryBuilder builder = query.builder();
    Date from = new Date(1483250400000L);
    Date to = new Date(1589481925565L);
    List<UUID> ids = new ArrayList<>();
    ids.add(UUID.fromString("20dabed7-6e2a-41f4-96f9-4b4581d85caa"));
    ids.add(UUID.fromString("bc44dfc8-c348-4edc-978e-c84e7333bdba"));
    ids.add(UUID.fromString("e1e38a1c-2c38-40be-afc9-2bc5e301abe1"));
    ids.add(UUID.fromString("52ae8394-1133-4675-8f9a-0f67d0b74db1"));
    ids.add(UUID.fromString("f00b0367-5267-43b3-8c4a-a5b2a31b0a8c"));
    builder
      .startExpression()
        .whereEqual("recordType", RecordType.MARC)
          .and()
        .whereGreaterThenOrEqual("generation", 1)
          .and()
        .whereNotEqual("state", Record.State.DELETED)
          .and()
        .whereBetween("metadata.createdDate", from, to)
      .endExpression()
        .or()
      .startExpression()
        .whereLessThen("order", 10)
          .or()
        .whereEqual("additionalInfo.suppressDiscovery", true)
      .endExpression()
        .or()
      .whereIn("id", ids)
      .orderBy("snapshotId")
      .orderBy("matchedId", Direction.ASC);
    String where = "WHERE ( recordtype = 'MARC' AND generation >= 1 AND state != 'DELETED' AND " +
      "createddate BETWEEN '" + DATE_FORMATTER.format(from) + "' AND '" + DATE_FORMATTER.format(to) + "' ) OR " + 
      "( orderinfile < 10 OR suppressdiscovery = true ) OR " +
      "id IN ('20dabed7-6e2a-41f4-96f9-4b4581d85caa','bc44dfc8-c348-4edc-978e-c84e7333bdba'," +
      "'e1e38a1c-2c38-40be-afc9-2bc5e301abe1','52ae8394-1133-4675-8f9a-0f67d0b74db1'," +
      "'f00b0367-5267-43b3-8c4a-a5b2a31b0a8c')";
    assertEquals(where, builder.buildWhereClause());
    String orderBy = "ORDER BY snapshotid ASC, matchedid ASC";
    assertEquals(orderBy, builder.buildOrderByClause());
  }

  @Test
  public void shouldCreateSimpleSnapshotQueryBuilder() {
    SnapshotQuery query = SnapshotQuery.query();
    QueryBuilder builder = query.builder();
    builder.orderBy("processingStartedDate", Direction.DESC);
    String where = StringUtils.EMPTY;
    assertEquals(where, builder.buildWhereClause());
    String orderBy = "ORDER BY processing_started_date DESC";
    assertEquals(orderBy, builder.buildOrderByClause());
  }

  @Test
  public void shouldCreateErrorRecordQueryBuilder() {
    ErrorRecordQuery query = ErrorRecordQuery.query();
    QueryBuilder builder = query.builder();
    builder.whereLike("description", "%test%");
    String where = "WHERE description LIKE '%test%'";
    assertEquals(where, builder.buildWhereClause());
    String orderBy = StringUtils.EMPTY;
    assertEquals(orderBy, builder.buildOrderByClause());
  }

  private void assertDefaultQueryBuilder(QueryBuilder builder) {
    assertNotNull(builder);
    assertNotNull(builder.query());
    assertEquals(StringUtils.EMPTY, builder.buildWhereClause());
    assertEquals(StringUtils.EMPTY, builder.buildOrderByClause());
  }

}