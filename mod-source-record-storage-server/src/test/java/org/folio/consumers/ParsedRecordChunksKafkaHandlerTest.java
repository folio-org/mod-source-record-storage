package org.folio.consumers;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaHeader;
import org.folio.kafka.KafkaConfig;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.RecordCollection;
import org.folio.rest.jaxrs.model.RecordsBatchResponse;
import org.folio.services.RecordService;
import org.folio.services.caches.CancelledJobsIdsCache;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.List;
import java.util.UUID;

import static org.folio.consumers.ParsedRecordChunksKafkaHandler.JOB_EXECUTION_ID_HEADER;
import static org.folio.dataimport.util.RestUtil.OKAPI_TENANT_HEADER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ParsedRecordChunksKafkaHandlerTest {

  private static final String KAFKA_RECORD_KEY = "test-key";
  private static final String TENANT_ID = "diku";

  @Mock
  private RecordService recordService;
  @Mock
  private CancelledJobsIdsCache cancelledJobsIdsCache;
  @Mock
  private KafkaConfig kafkaConfig;
  private ParsedRecordChunksKafkaHandler handler;

  @Before
  public void setUp() {
    Vertx vertx = Vertx.vertx();
    handler = new ParsedRecordChunksKafkaHandler(recordService, cancelledJobsIdsCache, vertx, kafkaConfig);
  }

  @Test
  public void shouldSkipEventProcessingIfHeadersContainCancelledJobId() {
    // Given
    String cancelledJobId = UUID.randomUUID().toString();
    when(cancelledJobsIdsCache.contains(cancelledJobId)).thenReturn(true);

    KafkaConsumerRecord<String, byte[]> kafkaRecord = mock(KafkaConsumerRecord.class);
    when(kafkaRecord.key()).thenReturn(KAFKA_RECORD_KEY);
    when(kafkaRecord.headers()).thenReturn(List.of(
      KafkaHeader.header(JOB_EXECUTION_ID_HEADER, cancelledJobId)
    ));

    // When
    Future<String> result = handler.handle(kafkaRecord);

    // Then
    assertTrue(result.succeeded());
    assertEquals(KAFKA_RECORD_KEY, result.result());
    verify(recordService, never()).saveRecords(any(RecordCollection.class), any());
  }

  @Test
  public void shouldProcessEventWhenJobIsNotCancelled() {
    // Given
    String jobId = UUID.randomUUID().toString();
    RecordCollection recordCollection = new RecordCollection()
      .withRecords(List.of(new Record()))
      .withTotalRecords(1);
    Event event = new Event().withEventPayload(Json.encode(recordCollection));

    when(cancelledJobsIdsCache.contains(jobId)).thenReturn(false);
    when(recordService.saveRecords(any(RecordCollection.class), any()))
      .thenReturn(Future.succeededFuture(new RecordsBatchResponse().withTotalRecords(1)));

    KafkaConsumerRecord<String, byte[]> kafkaRecord = mock(KafkaConsumerRecord.class);
    when(kafkaRecord.key()).thenReturn(KAFKA_RECORD_KEY);
    when(kafkaRecord.headers()).thenReturn(List.of(
      KafkaHeader.header(JOB_EXECUTION_ID_HEADER, jobId),
      KafkaHeader.header(OKAPI_TENANT_HEADER, TENANT_ID)
    ));
    when(kafkaRecord.value()).thenReturn(Json.encode(event).getBytes());

    ParsedRecordChunksKafkaHandler spiedHandler = new ParsedRecordChunksKafkaHandler(recordService, cancelledJobsIdsCache, Vertx.vertx(), kafkaConfig) {
      @Override
      protected Future<String> sendBackRecordsBatchResponse(RecordsBatchResponse recordsBatchResponse, List<KafkaHeader> kafkaHeaders, String tenantId, int chunkNumber, String eventType, KafkaConsumerRecord<String, byte[]> commonRecord) {
        return Future.succeededFuture("fake-key");
      }
    };

    // When
    Future<String> result = spiedHandler.handle(kafkaRecord);

    // Then
    assertTrue(result.succeeded());
    verify(recordService).saveRecords(any(RecordCollection.class), any());
  }

  @Test
  public void shouldReturnFailedFutureWhenRecordServiceFails() {
    // Given
    String jobId = UUID.randomUUID().toString();
    RecordCollection recordCollection = new RecordCollection()
      .withRecords(List.of(new Record()))
      .withTotalRecords(1);
    Event event = new Event().withEventPayload(Json.encode(recordCollection));

    when(cancelledJobsIdsCache.contains(jobId)).thenReturn(false);
    when(recordService.saveRecords(any(RecordCollection.class), any()))
      .thenReturn(Future.failedFuture(new RuntimeException("test error")));

    KafkaConsumerRecord<String, byte[]> kafkaRecord = mock(KafkaConsumerRecord.class);
    when(kafkaRecord.key()).thenReturn(KAFKA_RECORD_KEY);
    when(kafkaRecord.headers()).thenReturn(List.of(
      KafkaHeader.header(JOB_EXECUTION_ID_HEADER, jobId),
      KafkaHeader.header(OKAPI_TENANT_HEADER, TENANT_ID)
    ));
    when(kafkaRecord.value()).thenReturn(Json.encode(event).getBytes());

    // When
    Future<String> result = handler.handle(kafkaRecord);

    // Then
    assertTrue(result.failed());
    assertEquals("test error", result.cause().getMessage());
  }

  @Test
  public void shouldReturnFailedFutureWhenKafkaProducerFails() {
    // Given
    String jobId = UUID.randomUUID().toString();
    RecordCollection recordCollection = new RecordCollection()
      .withRecords(List.of(new Record()))
      .withTotalRecords(1);
    Event event = new Event().withEventPayload(Json.encode(recordCollection));

    when(cancelledJobsIdsCache.contains(jobId)).thenReturn(false);
    when(recordService.saveRecords(any(RecordCollection.class), any()))
      .thenReturn(Future.succeededFuture(new RecordsBatchResponse().withTotalRecords(1)));

    KafkaConsumerRecord<String, byte[]> kafkaRecord = mock(KafkaConsumerRecord.class);
    when(kafkaRecord.key()).thenReturn(KAFKA_RECORD_KEY);
    when(kafkaRecord.headers()).thenReturn(List.of(
      KafkaHeader.header(JOB_EXECUTION_ID_HEADER, jobId),
      KafkaHeader.header(OKAPI_TENANT_HEADER, TENANT_ID)
    ));
    when(kafkaRecord.value()).thenReturn(Json.encode(event).getBytes());

    // Replace the producer manager with a mock that returns a failing producer
    ParsedRecordChunksKafkaHandler spiedHandler = new ParsedRecordChunksKafkaHandler(recordService, cancelledJobsIdsCache, Vertx.vertx(), kafkaConfig) {
      @Override
      protected Future<String> sendBackRecordsBatchResponse(RecordsBatchResponse recordsBatchResponse, List<KafkaHeader> kafkaHeaders, String tenantId, int chunkNumber, String eventType, KafkaConsumerRecord<String, byte[]> commonRecord) {
        return Future.failedFuture("kafka producer error");
      }
    };

    // When
    Future<String> result = spiedHandler.handle(kafkaRecord);

    // Then
    assertTrue(result.failed());
    assertEquals("kafka producer error", result.cause().getMessage());
  }

}
