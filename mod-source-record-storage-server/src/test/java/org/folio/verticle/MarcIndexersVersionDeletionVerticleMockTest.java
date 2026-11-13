package org.folio.verticle;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import org.folio.dao.RecordDao;
import org.folio.services.TenantDataProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith({VertxExtension.class, MockitoExtension.class})
@MockitoSettings(strictness = Strictness.LENIENT)
public class MarcIndexersVersionDeletionVerticleMockTest {

  @Mock
  private RecordDao recordDao;
  @Mock
  private TenantDataProvider tenantDataProvider;
  private MarcIndexersVersionDeletionVerticle verticle;

  private Vertx vertx;
  private Promise<Void> promise;

  @BeforeEach
  void setUp() {
    vertx = mock(Vertx.class);
    AtomicInteger counter = new AtomicInteger(0);
    when(vertx.setTimer(anyLong(), any())).thenAnswer(invocation -> {
      if (counter.getAndIncrement() < 10) {
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
  void testStartWithPlannedTimeCallsTimedDeletion() throws IllegalAccessException, NoSuchFieldException {

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
  void testStartWithoutPlannedTimeCallsPeriodicDeletion() throws IllegalAccessException, NoSuchFieldException {

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
  void testStartWithInvalidPlannedTimeFallsBackToPeriodicDeletion() throws IllegalAccessException, NoSuchFieldException {
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
