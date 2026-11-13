package org.folio.services;

import static org.folio.rest.jooq.Tables.SNAPSHOTS_LB;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.vertx.core.Future;
import io.vertx.junit5.VertxTestContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.folio.TestMocks;
import org.folio.dao.SnapshotDao;
import org.folio.dao.SnapshotDaoImpl;
import org.folio.dao.util.SnapshotDaoUtil;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.rest.jaxrs.model.Snapshot.Status;
import org.folio.rest.jaxrs.model.SnapshotCollection;
import org.folio.rest.jooq.enums.JobExecutionStatus;
import org.jooq.Condition;
import org.jooq.OrderField;
import org.jooq.SortOrder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class SnapshotServiceTest extends AbstractLBServiceTest {

  private SnapshotDao snapshotDao;

  private SnapshotService snapshotService;

  @Mock
  private SnapshotDao mockedSnapshotDao;

  @InjectMocks
  private SnapshotServiceImpl snapshotServiceForMocks;

  @BeforeEach
  void setUp() {
    snapshotDao = new SnapshotDaoImpl(postgresClientFactory);
    snapshotService = new SnapshotServiceImpl(snapshotDao);
  }

  @AfterEach
  void cleanUp(VertxTestContext testContext) {
    SnapshotDaoUtil.deleteAll(postgresClientFactory.getQueryExecutor(TENANT_ID))
      .onComplete(testContext.succeedingThenComplete());
  }

  @Test
  void shouldGetSnapshots(VertxTestContext testContext) {
    SnapshotDaoUtil.save(postgresClientFactory.getQueryExecutor(TENANT_ID), TestMocks.getSnapshots()).onComplete(batch -> {
      if (batch.failed()) {
        testContext.failNow(batch.cause());
        return;
      }
      Condition condition = SNAPSHOTS_LB.STATUS.eq(JobExecutionStatus.PROCESSING_IN_PROGRESS);
      List<OrderField<?>> orderFields = new ArrayList<>();
      orderFields.add(SNAPSHOTS_LB.PROCESSING_STARTED_DATE.sort(SortOrder.DESC));
      snapshotService.getSnapshots(condition, orderFields, 0, 2, TENANT_ID).onComplete(get -> {
        if (get.failed()) {
          testContext.failNow(get.cause());
          return;
        }
        SnapshotCollection snapshotCollection = get.result();
        assertEquals(3, snapshotCollection.getTotalRecords());
        compareSnapshots(TestMocks.getSnapshot("d787a937-cc4b-49b3-85ef-35bcd643c689").get(), snapshotCollection.getSnapshots().get(0));
        compareSnapshots(TestMocks.getSnapshot("6681ef31-03fe-4abc-9596-23de06d575c5").get(), snapshotCollection.getSnapshots().get(1));
        testContext.completeNow();
      });
    });
  }

  @Test
  void shouldGetSnapshotById(VertxTestContext testContext) {
    Snapshot expected = TestMocks.getSnapshot(0);
    snapshotDao.saveSnapshot(expected, TENANT_ID).onComplete(save -> {
      if (save.failed()) {
        testContext.failNow(save.cause());
        return;
      }
      snapshotService.getSnapshotById(expected.getJobExecutionId(), TENANT_ID).onComplete(get -> {
        if (get.failed()) {
          testContext.failNow(get.cause());
          return;
        }
        assertTrue(get.result().isPresent());
        compareSnapshots(expected, get.result().get());
        testContext.completeNow();
      });
    });
  }

  @Test
  void shouldNotGetSnapshotById(VertxTestContext testContext) {
    Snapshot expected = TestMocks.getSnapshot(0);
    snapshotService.getSnapshotById(expected.getJobExecutionId(), TENANT_ID).onComplete(get -> {
      if (get.failed()) {
        testContext.failNow(get.cause());
        return;
      }
      assertFalse(get.result().isPresent());
      testContext.completeNow();
    });
  }

  @Test
  void shouldSaveSnapshot(VertxTestContext testContext) {
    Snapshot expected = TestMocks.getSnapshot(0);
    snapshotService.saveSnapshot(expected, TENANT_ID).onComplete(save -> {
      if (save.failed()) {
        testContext.failNow(save.cause());
        return;
      }
      snapshotDao.getSnapshotById(expected.getJobExecutionId(), TENANT_ID).onComplete(get -> {
        if (get.failed()) {
          testContext.failNow(get.cause());
          return;
        }
        assertTrue(get.result().isPresent());
        compareSnapshots(expected, get.result().get());
        testContext.completeNow();
      });
    });
  }

  @Test
  void shouldFailToSaveSnapshot(VertxTestContext testContext) {
    Snapshot valid = TestMocks.getSnapshot(0);
    Snapshot invalid = new Snapshot()
      .withJobExecutionId(valid.getJobExecutionId())
      .withProcessingStartedDate(valid.getProcessingStartedDate())
      .withMetadata(valid.getMetadata());
    snapshotService.saveSnapshot(invalid, TENANT_ID).onComplete(save -> {
      assertTrue(save.failed());
      String expected = "null value in column \"status\" of relation \"snapshots_lb\" violates not-null constraint";
      assertTrue(save.cause().getMessage().contains(expected));
      testContext.completeNow();
    });
  }

  @Test
  void shouldUpdateSnapshot(VertxTestContext testContext) {
    Snapshot original = TestMocks.getSnapshot(0);
    snapshotDao.saveSnapshot(original, TENANT_ID).onComplete(save -> {
      if (save.failed()) {
        testContext.failNow(save.cause());
        return;
      }
      Snapshot expected = new Snapshot()
        .withJobExecutionId(original.getJobExecutionId())
        .withStatus(Status.COMMITTED)
        .withProcessingStartedDate(original.getProcessingStartedDate())
        .withMetadata(original.getMetadata());
      snapshotService.updateSnapshot(expected, TENANT_ID).onComplete(update -> {
        if (update.failed()) {
          testContext.failNow(update.cause());
          return;
        }
        assertTrue(update.result().getMetadata().getUpdatedDate()
          .after(update.result().getMetadata().getCreatedDate()));
        compareSnapshots(expected, update.result());
        testContext.completeNow();
      });
    });
  }

  @Test
  void shouldFailToUpdateSnapshot(VertxTestContext testContext) {
    Snapshot snapshot = TestMocks.getSnapshot(0);
    snapshotDao.getSnapshotById(snapshot.getJobExecutionId(), TENANT_ID).onComplete(get -> {
      if (get.failed()) {
        testContext.failNow(get.cause());
        return;
      }
      assertFalse(get.result().isPresent());
      snapshotService.updateSnapshot(snapshot, TENANT_ID).onComplete(update -> {
        assertTrue(update.failed());
        String expected = String.format("Snapshot with id '%s' was not found", snapshot.getJobExecutionId());
        assertEquals(expected, update.cause().getMessage());
        testContext.completeNow();
      });
    });
  }

  @Test
  void shouldDeleteSnapshot(VertxTestContext testContext) {
    Snapshot snapshot = TestMocks.getSnapshot(0);
    snapshotDao.saveSnapshot(snapshot, TENANT_ID).onComplete(save -> {
      if (save.failed()) {
        testContext.failNow(save.cause());
        return;
      }
      snapshotService.deleteSnapshot(snapshot.getJobExecutionId(), TENANT_ID).onComplete(delete -> {
        if (delete.failed()) {
          testContext.failNow(delete.cause());
          return;
        }
        assertTrue(delete.result());
        snapshotDao.getSnapshotById(snapshot.getJobExecutionId(), TENANT_ID).onComplete(get -> {
          if (get.failed()) {
            testContext.failNow(get.cause());
            return;
          }
          assertFalse(get.result().isPresent());
          testContext.completeNow();
        });
      });
    });
  }

  @Test
  void shouldNotDeleteSnapshot(VertxTestContext testContext) {
    Snapshot snapshot = TestMocks.getSnapshot(0);
    snapshotService.deleteSnapshot(snapshot.getJobExecutionId(), TENANT_ID).onComplete(delete -> {
      if (delete.failed()) {
        testContext.failNow(delete.cause());
        return;
      }
      assertFalse(delete.result());
      testContext.completeNow();
    });
  }

  @Test
  void shouldCopySnapshotToAnotherTenant(VertxTestContext testContext) {
    Snapshot expected = TestMocks.getSnapshot(0);

    doAnswer(invocationOnMock -> Future.succeededFuture(Optional.of(expected))).when(mockedSnapshotDao).getSnapshotById(anyString(), anyString());

    doAnswer(invocationOnMock -> Future.succeededFuture(expected)).when(mockedSnapshotDao).saveSnapshot(any(), anyString());

    snapshotServiceForMocks.copySnapshotToOtherTenant(expected.getJobExecutionId(), TENANT_ID, "centralTenantId").onComplete(get -> {
      if (get.failed()) {
        testContext.failNow(get.cause());
        return;
      }
      compareSnapshots(expected, get.result());
      verify(mockedSnapshotDao, times(1)).saveSnapshot(any(Snapshot.class), eq("centralTenantId"));
      testContext.completeNow();
    });
  }

  private void compareSnapshots(Snapshot expected, Snapshot actual) {
    assertEquals(expected.getJobExecutionId(), actual.getJobExecutionId());
    assertEquals(expected.getStatus(), actual.getStatus());
    assertEquals(expected.getProcessingStartedDate(), actual.getProcessingStartedDate());
    if (Objects.nonNull(expected.getMetadata())) {
      compareMetadata(expected.getMetadata(), actual.getMetadata());
    } else {
      assertNull(actual.getMetadata());
    }
  }

}
