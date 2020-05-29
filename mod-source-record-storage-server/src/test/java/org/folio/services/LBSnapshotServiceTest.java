package org.folio.services;

import java.util.ArrayList;
import java.util.List;

import org.folio.TestMocks;
import org.folio.dao.LBSnapshotDao;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.rest.jaxrs.model.Snapshot.Status;
import org.folio.rest.jaxrs.model.SnapshotCollection;
import org.folio.rest.jooq.Tables;
import org.folio.rest.jooq.enums.JobExecutionStatus;
import org.jooq.Condition;
import org.jooq.OrderField;
import org.jooq.SortOrder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class LBSnapshotServiceTest extends AbstractLBServiceTest {

  private LBSnapshotService snapshotService;

  @Before
  public void setUp(TestContext context) {
    snapshotService = new LBSnapshotServiceImpl(postgresClientFactory);
  }

  @After
  public void cleanUp(TestContext context) {
    LBSnapshotDao.deleteAll(postgresClientFactory.getQueryExecutor(TENANT_ID)).onComplete(delete -> {
      if (delete.failed()) {
        context.fail(delete.cause());
      }
    });
  }

  @Test
  public void shouldGetSnapshotById(TestContext context) {
    Snapshot expected = TestMocks.getSnapshot(0);
    snapshotService.saveSnapshot(expected, TENANT_ID).onComplete(save -> {
      if (save.failed()) {
        context.fail(save.cause());
      }
      snapshotService.getSnapshotById(expected.getJobExecutionId(), TENANT_ID).onComplete(get -> {
        if (get.failed()) {
          context.fail(get.cause());
        }
        context.assertTrue(get.result().isPresent());
        compareSnapshots(context, expected, get.result().get());
      });
    });
  }

  @Test
  public void shouldNotGetSnapshotById(TestContext context) {
    Snapshot expected = TestMocks.getSnapshot(0);
    snapshotService.getSnapshotById(expected.getJobExecutionId(), TENANT_ID).onComplete(get -> {
      if (get.failed()) {
        context.fail(get.cause());
      }
      context.assertFalse(get.result().isPresent());
    });
  }

  @Test
  public void shouldSaveSnapshot(TestContext context) {
    Snapshot expected = TestMocks.getSnapshot(0);
    snapshotService.saveSnapshot(expected, TENANT_ID).onComplete(save -> {
      if (save.failed()) {
        context.fail(save.cause());
      }
      snapshotService.getSnapshotById(expected.getJobExecutionId(), TENANT_ID).onComplete(get -> {
        if (get.failed()) {
          context.fail(get.cause());
        }
        context.assertTrue(get.result().isPresent());
        compareSnapshots(context, expected, get.result().get());
      });
    });
  }

  @Test
  public void shouldFailToSaveSnapshot(TestContext context) {
    Snapshot valid = TestMocks.getSnapshot(0);
    Snapshot invalid = new Snapshot()
      .withJobExecutionId(valid.getJobExecutionId())
      .withProcessingStartedDate(valid.getProcessingStartedDate())
      .withMetadata(valid.getMetadata());
    snapshotService.saveSnapshot(invalid, TENANT_ID).onComplete(save -> {
      context.assertTrue(save.failed());
      String expected = "null value in column \"status\" violates not-null constraint";
      context.assertEquals(expected, save.cause().getMessage());
    });
  }

  @Test
  public void shouldUpdateSnapshot(TestContext context) {
    Snapshot original = TestMocks.getSnapshot(0);
    snapshotService.saveSnapshot(original, TENANT_ID).onComplete(save -> {
      if (save.failed()) {
        context.fail(save.cause());
      }
      Snapshot expected = new Snapshot()
        .withJobExecutionId(original.getJobExecutionId())
        .withStatus(Status.COMMITTED)
        .withProcessingStartedDate(original.getProcessingStartedDate())
        .withMetadata(original.getMetadata());
      snapshotService.updateSnapshot(expected, TENANT_ID).onComplete(update -> {
        if (update.failed()) {
          context.fail(update.cause());
        }
        compareSnapshots(context, expected, update.result());
      });
    });
  }

  @Test
  public void shouldFailToUpdateSnapshot(TestContext context) {
    Snapshot snapshot = TestMocks.getSnapshot(0);
    snapshotService.getSnapshotById(snapshot.getJobExecutionId(), TENANT_ID).onComplete(get -> {
      if (get.failed()) {
        context.fail(get.cause());
      }
      context.assertFalse(get.result().isPresent());
      snapshotService.updateSnapshot(snapshot, TENANT_ID).onComplete(update -> {
        context.assertTrue(update.failed());
        String expected = String.format("Snapshot with id '%s' was not found", snapshot.getJobExecutionId());
        context.assertEquals(expected, update.cause().getMessage());
      });
    });
  }

  @Test
  public void shouldDeleteSnapshot(TestContext context) {
    Snapshot snapshot = TestMocks.getSnapshot(0);
    snapshotService.saveSnapshot(snapshot, TENANT_ID).onComplete(save -> {
      if (save.failed()) {
        context.fail(save.cause());
      }
      snapshotService.deleteSnapshot(snapshot.getJobExecutionId(), TENANT_ID).onComplete(delete -> {
        if (delete.failed()) {
          context.fail(delete.cause());
        }
        context.assertTrue(delete.result());
        snapshotService.getSnapshotById(snapshot.getJobExecutionId(), TENANT_ID).onComplete(get -> {
          if (get.failed()) {
            context.fail(get.cause());
          }
          context.assertNull(get.result());
        });
      });
    });
  }

  @Test
  public void shouldNotDeleteSnapshot(TestContext context) {
    Snapshot snapshot = TestMocks.getSnapshot(0);
    snapshotService.deleteSnapshot(snapshot.getJobExecutionId(), TENANT_ID).onComplete(delete -> {
      if (delete.failed()) {
        context.fail(delete.cause());
      }
      context.assertFalse(delete.result());
    });
  }

  @Test
  public void shouldGetSnapshots(TestContext context) {
    LBSnapshotDao.save(postgresClientFactory.getQueryExecutor(TENANT_ID), TestMocks.getSnapshots()).onComplete(batch -> {
      if (batch.failed()) {
        context.fail(batch.cause());
      }
      Condition condition = Tables.SNAPSHOTS_LB.STATUS.eq(JobExecutionStatus.PROCESSING_IN_PROGRESS);
      List<OrderField<?>> orderFields = new ArrayList<>();
      orderFields.add(Tables.SNAPSHOTS_LB.PROCESSING_STARTED_DATE.sort(SortOrder.DESC));
      snapshotService.getSnapshots(condition, orderFields, 0, 2, TENANT_ID).onComplete(get -> {
        if (get.failed()) {
          context.fail(get.cause());
        }
        SnapshotCollection snapshotCollection = get.result();
        context.assertEquals(2, snapshotCollection.getTotalRecords());
        compareSnapshots(context, TestMocks.getSnapshot("d787a937-cc4b-49b3-85ef-35bcd643c689").get(), snapshotCollection.getSnapshots().get(0));
        compareSnapshots(context, TestMocks.getSnapshot("6681ef31-03fe-4abc-9596-23de06d575c5").get(), snapshotCollection.getSnapshots().get(1));
      });
    });
  }

  private void compareSnapshots(TestContext context, Snapshot expected, Snapshot actual) {
    context.assertEquals(expected.getJobExecutionId(), actual.getJobExecutionId());
    context.assertEquals(expected.getStatus(), actual.getStatus());
    context.assertEquals(expected.getProcessingStartedDate(), actual.getProcessingStartedDate());
    context.assertEquals(expected.getMetadata().getCreatedByUserId(), actual.getMetadata().getCreatedByUserId());
    context.assertNotNull(actual.getMetadata().getCreatedDate());
    context.assertEquals(expected.getMetadata().getUpdatedByUserId(), actual.getMetadata().getUpdatedByUserId());
    context.assertNotNull(actual.getMetadata().getUpdatedDate());
  }

}