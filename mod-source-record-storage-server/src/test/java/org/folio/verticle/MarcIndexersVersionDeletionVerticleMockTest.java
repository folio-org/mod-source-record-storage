package org.folio.verticle;

import io.vertx.core.*;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.dao.RecordDao;
import org.folio.services.TenantDataProvider;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.*;

@RunWith(VertxUnitRunner.class)
public class MarcIndexersVersionDeletionVerticleMockTest {

  @Mock
  private RecordDao recordDao;
  @Mock
  private TenantDataProvider tenantDataProvider;
  private MarcIndexersVersionDeletionVerticle verticle;

  private Vertx vertx;
  private Promise<Void> promise;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    vertx = mock(Vertx.class);
    AtomicInteger counter = new AtomicInteger(0);
    when(vertx.setTimer(anyLong(), any())).thenAnswer(invocation -> {
      if (counter.getAndIncrement() < 10) {  // Ограничиваем количество вызовов
        Handler<Long> handler = invocation.getArgument(1);
        handler.handle(1L);
      }
      return 1L;
    });

    when(tenantDataProvider.getModuleTenants(anyString()))
      .thenReturn(Future.succeededFuture(Collections.emptyList()));

    verticle = spy(new MarcIndexersVersionDeletionVerticle(recordDao, tenantDataProvider));
    verticle.init(vertx, mock(Context.class));
    promise = Promise.promise();
  }

  @Test
  public void testStartWithPlannedTimeCallsTimedDeletion() throws IllegalAccessException, NoSuchFieldException {

    // Use reflection to set plannedTime to a non-blank value
    Field plannedTimeField = MarcIndexersVersionDeletionVerticle.class.getDeclaredField("plannedTime");
    plannedTimeField.setAccessible(true);
    plannedTimeField.set(verticle, "12:00,15:00");

    Field dirtyBatchSizeField = MarcIndexersVersionDeletionVerticle.class.getDeclaredField("dirtyBatchSize");
    dirtyBatchSizeField.setAccessible(true);
    dirtyBatchSizeField.set(verticle, 100);

    verticle.start(promise);

    // Verify that setupTimedDeletion is called with the correct parameters
    verify(verticle).setupTimedDeletion("12:00,15:00", 100);
    verify(verticle, never()).setupPeriodicDeletion(anyLong(), anyInt());
  }

  @Test
  public void testStartWithoutPlannedTimeCallsPeriodicDeletion() throws IllegalAccessException, NoSuchFieldException {

    // Use reflection to set intervalField to a non-blank value
    Field intervalField = MarcIndexersVersionDeletionVerticle.class.getDeclaredField("interval");
    intervalField.setAccessible(true);
    intervalField.set(verticle, 1800);

    Field dirtyBatchSizeField = MarcIndexersVersionDeletionVerticle.class.getDeclaredField("dirtyBatchSize");
    dirtyBatchSizeField.setAccessible(true);
    dirtyBatchSizeField.set(verticle, 100);

    verticle.start(promise);

    // Verify that setupPeriodicDeletion is called with the correct parameters
    verify(verticle).setupPeriodicDeletion(1800 * 1000L, 100);
    verify(verticle, never()).setupTimedDeletion(anyString(), anyInt());
  }

  @Test
  public void testStartWithInvalidPlannedTimeFallsBackToPeriodicDeletion() throws IllegalAccessException, NoSuchFieldException {
    // Use reflection to set incorrect value to plannedTime value
    Field plannedTimeField = MarcIndexersVersionDeletionVerticle.class.getDeclaredField("plannedTime");
    plannedTimeField.setAccessible(true);
    plannedTimeField.set(verticle, "invalid-time-format");

    Field dirtyBatchSizeField = MarcIndexersVersionDeletionVerticle.class.getDeclaredField("dirtyBatchSize");
    dirtyBatchSizeField.setAccessible(true);
    dirtyBatchSizeField.set(verticle, 100);

    verticle.start(promise);

    //Check that setupPeriodicDeletion should be executed
    verify(verticle).setupPeriodicDeletion(1800 * 1000L, 100);
    verify(verticle, times(1)).setupTimedDeletion("invalid-time-format", 100);
  }
}
