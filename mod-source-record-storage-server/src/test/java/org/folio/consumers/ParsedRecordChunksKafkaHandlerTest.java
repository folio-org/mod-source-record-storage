package org.folio.consumers;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaHeader;
import org.folio.kafka.KafkaConfig;
import org.folio.rest.jaxrs.model.RecordCollection;
import org.folio.services.RecordService;
import org.folio.services.caches.CancelledJobsIdsCache;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.List;
import java.util.UUID;

import static org.folio.consumers.ParsedRecordChunksKafkaHandler.JOB_EXECUTION_ID_HEADER;
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

}
