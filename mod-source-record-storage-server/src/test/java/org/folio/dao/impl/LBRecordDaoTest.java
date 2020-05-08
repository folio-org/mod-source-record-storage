package org.folio.dao.impl;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.folio.dao.LBRecordDao;
import org.folio.dao.LBSnapshotDao;
import org.folio.dao.query.RecordQuery;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.RecordCollection;
import org.folio.rest.persist.PostgresClient;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.BeanUtils;

import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class LBRecordDaoTest extends AbstractEntityDaoTest<Record, RecordCollection, RecordQuery, LBRecordDao> {

  private LBSnapshotDao snapshotDao;

  @Override
  public void createDao(TestContext context) throws IllegalAccessException {
    snapshotDao = new LBSnapshotDaoImpl();
    FieldUtils.writeField(snapshotDao, "postgresClientFactory", postgresClientFactory, true);
    dao = new LBRecordDaoImpl();
    FieldUtils.writeField(dao, "postgresClientFactory", postgresClientFactory, true);
  }

  @Override
  public void createDependentEntities(TestContext context) throws IllegalAccessException {
    Async async = context.async();
    snapshotDao.save(getSnapshots(), TENANT_ID).onComplete(save -> {
      if (save.failed()) {
        context.fail(save.cause());
      }
      async.complete();
    });
  }

  @Override
  public void clearTables(TestContext context) {
    Async async = context.async();
    PostgresClient pgClient = PostgresClient.getInstance(vertx, TENANT_ID);
    String sql = String.format(DELETE_SQL_TEMPLATE, dao.getTableName());
    pgClient.execute(sql, delete -> {
      if (delete.failed()) {
        context.fail(delete.cause());
      }
      String snapshotSql = String.format(DELETE_SQL_TEMPLATE, snapshotDao.getTableName());
      pgClient.execute(snapshotSql, snapshotDelete -> {
        if (snapshotDelete.failed()) {
          context.fail(snapshotDelete.cause());
        }
        async.complete();
      });
    });
  }

  @Test
  public void shouldGetByMatchedId(TestContext context) {
    Async async = context.async();
    dao.save(getMockEntity(), TENANT_ID).onComplete(save -> {
      if (save.failed()) {
        context.fail(save.cause());
      }
      dao.getByMatchedId(getMockEntity().getMatchedId(), TENANT_ID).onComplete(res -> {
        if (res.failed()) {
          context.fail(res.cause());
        }
        context.assertTrue(res.result().isPresent());
        compareEntities(context, getMockEntity(), res.result().get());
        async.complete();
      });
    });
  }

  @Test
  public void shouldGetByInstanceId(TestContext context) {
    Async async = context.async();
    dao.save(getMockEntity(), TENANT_ID).onComplete(save -> {
      if (save.failed()) {
        context.fail(save.cause());
      }
      dao.getByInstanceId(getMockEntity().getExternalIdsHolder().getInstanceId(), TENANT_ID).onComplete(res -> {
        if (res.failed()) {
          context.fail(res.cause());
        }
        context.assertTrue(res.result().isPresent());
        compareEntities(context, getMockEntity(), res.result().get());
        async.complete();
      });
    });
  }

  @Test
  public void shouldSaveGeneratingId(TestContext context) {
    Async async = context.async();
    dao.save(getMockEntityWithoutId(), TENANT_ID).onComplete(res -> {
      if (res.failed()) {
        context.fail(res.cause());
      }
      compareEntities(context, getMockEntityWithoutId(), res.result());
      async.complete();
    });
  }

  public Record getMockEntityWithoutId() {
    return new Record()
      .withSnapshotId(getSnapshot(1).getJobExecutionId())
      .withRecordType(Record.RecordType.MARC)
      .withMatchedProfileId("f9926e86-883b-4455-a807-fc5eeb9a951a")
      .withOrder(0)
      .withGeneration(1)
      .withState(Record.State.ACTUAL);
  }

  @Override
  public RecordQuery getNoopQuery() {
    return new RecordQuery();
  }

  @Override
  public RecordQuery getArbitruaryQuery() {
    RecordQuery snapshotQuery = new RecordQuery();
    snapshotQuery.setMatchedProfileId(getMockEntity().getMatchedProfileId());
    snapshotQuery.setState(Record.State.ACTUAL);
    return snapshotQuery;
  }

  @Override
  public RecordQuery getArbitruarySortedQuery() {
    return (RecordQuery) getArbitruaryQuery()
      .orderBy("matchedProfileId");
  }

  @Override
  public RecordQuery getCompleteQuery() {
    RecordQuery query = new RecordQuery();
    BeanUtils.copyProperties(getRecord("0f0fe962-d502-4a4f-9e74-7732bec94ee8").get(), query);
    query.withMetadata(query.getMetadata().withCreatedDate(null).withUpdatedDate(null));
    return query;
  }

  @Override
  public Record getMockEntity() {
    return getRecord(0);
  }

  @Override
  public Record getInvalidMockEntity() {
    String id = UUID.randomUUID().toString();
    return new Record()
      .withId(id)
      .withRecordType(Record.RecordType.MARC)
      .withOrder(0)
      .withGeneration(1)
      .withState(Record.State.ACTUAL);
  }

  @Override
  public Record getUpdatedMockEntity() {
    return new Record()
      .withId(getMockEntity().getId())
      .withMatchedId(getMockEntity().getMatchedId())
      .withMatchedProfileId(getMockEntity().getMatchedProfileId())
      .withSnapshotId(getMockEntity().getSnapshotId())
      .withGeneration(getMockEntity().getGeneration())
      .withRecordType(getMockEntity().getRecordType())
      .withAdditionalInfo(getMockEntity().getAdditionalInfo())
      .withExternalIdsHolder(getMockEntity().getExternalIdsHolder())
      .withMetadata(getMockEntity().getMetadata())
      .withState(Record.State.DRAFT)
      .withOrder(2);
  }

  @Override
  public List<Record> getMockEntities() {
    return getRecords();
  }

  @Override
  public void compareEntities(TestContext context, Record expected, Record actual) {
    if (StringUtils.isEmpty(expected.getId())) {
      context.assertNotNull(actual.getId());
    } else {
      context.assertEquals(expected.getId(), actual.getId());
    }
    if (StringUtils.isEmpty(expected.getMatchedId())) {
      context.assertNotNull(actual.getMatchedId());
    } else {
      context.assertEquals(expected.getMatchedId(), actual.getMatchedId());
    }
    context.assertEquals(expected.getSnapshotId(), actual.getSnapshotId());
    context.assertEquals(expected.getMatchedProfileId(), actual.getMatchedProfileId());
    context.assertEquals(expected.getGeneration(), actual.getGeneration());
    context.assertEquals(expected.getOrder(), actual.getOrder());
    context.assertEquals(expected.getState(), actual.getState());
    context.assertEquals(expected.getRecordType(), actual.getRecordType());
    if (Objects.nonNull(expected.getAdditionalInfo())) {
      context.assertEquals(expected.getAdditionalInfo().getSuppressDiscovery(), actual.getAdditionalInfo().getSuppressDiscovery());
    }
    if (Objects.nonNull(expected.getExternalIdsHolder())) {
      context.assertEquals(expected.getExternalIdsHolder().getInstanceId(), actual.getExternalIdsHolder().getInstanceId());
    }
    if (Objects.nonNull(expected.getMetadata())) {
      context.assertEquals(expected.getMetadata().getCreatedByUserId(), actual.getMetadata().getCreatedByUserId());
      context.assertNotNull(actual.getMetadata().getCreatedDate());
      context.assertEquals(expected.getMetadata().getUpdatedByUserId(), actual.getMetadata().getUpdatedByUserId());
      context.assertNotNull(actual.getMetadata().getUpdatedDate());
    }
  }

  @Override
  public void assertNoopQueryResults(TestContext context, RecordCollection actual) {
    List<Record> expected = getMockEntities();
    context.assertEquals(new Integer(expected.size()), actual.getTotalRecords());
    expected.forEach(expectedRecord -> context.assertTrue(actual.getRecords().stream()
      .anyMatch(actualRecord -> actualRecord.getId().equals(expectedRecord.getId()))));
  }

  @Override
  public void assertArbitruaryQueryResults(TestContext context, RecordCollection actual) {
    List<Record> expected = getMockEntities().stream()
      .filter(entity -> entity.getState().equals(getArbitruaryQuery().getState()) &&
        entity.getMatchedProfileId().equals(getArbitruaryQuery().getMatchedProfileId()))
      .collect(Collectors.toList());
    context.assertEquals(new Integer(expected.size()), actual.getTotalRecords());
    expected.forEach(expectedRecord -> context.assertTrue(actual.getRecords().stream()
      .anyMatch(actualRecord -> actualRecord.getId().equals(expectedRecord.getId()))));
  }

  @Override
  public void assertArbitruarySortedQueryResults(TestContext context, RecordCollection actual) {
    List<Record> expected = getMockEntities().stream()
      .filter(entity -> entity.getState().equals(getArbitruaryQuery().getState()) &&
        entity.getMatchedProfileId().equals(getArbitruaryQuery().getMatchedProfileId()))
      .collect(Collectors.toList());
    Collections.sort(expected, (r1, r2) -> r1.getMatchedProfileId().compareTo(r2.getMatchedProfileId()));
    context.assertEquals(new Integer(expected.size()), actual.getTotalRecords());
    expected.forEach(expectedRecord -> context.assertTrue(actual.getRecords().stream()
      .anyMatch(actualRecord -> actualRecord.getId().equals(expectedRecord.getId()))));
  }

  @Override
  public String getCompleteWhereClause() {
    return "WHERE id = '0f0fe962-d502-4a4f-9e74-7732bec94ee8'" +
      " AND matchedid = '0f0fe962-d502-4a4f-9e74-7732bec94ee8'" +
      " AND snapshotid = '7f939c0b-618c-4eab-8276-a14e0bfe5728'" +
      " AND matchedprofileid = '0731b68a-147e-4ad8-9de2-7eef7c1a5a99'" +
      " AND generation = 0" +
      " AND orderinfile = 1" +
      " AND recordtype = 'MARC'" +
      " AND state = 'ACTUAL'" +
      " AND instanceid = '6b4ae089-e1ee-431f-af83-e1133f8e3da0'" +
      " AND suppressdiscovery = false" +
      " AND createdbyuserid = '4547e8af-638a-4595-8af8-4d396d6a9f7a'" +
      " AND updatedbyuserid = '4547e8af-638a-4595-8af8-4d396d6a9f7a'";
  }

}