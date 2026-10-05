package org.folio.services.kafka;

import static io.vertx.core.Future.failedFuture;
import static io.vertx.core.Future.succeededFuture;
import static org.folio.RecordStorageKafkaTopic.MARC_BIB;
import static org.folio.services.AbstractLBServiceTest.KAFKA_ENV;
import static org.folio.services.AbstractLBServiceTest.KAFKA_ENV_ID;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentCaptor.forClass;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.kafka.admin.KafkaAdminClient;
import io.vertx.kafka.admin.NewTopic;
import java.util.List;
import java.util.Set;
import org.apache.kafka.common.errors.TopicExistsException;
import org.folio.kafka.services.KafkaAdminClientService;
import org.folio.kafka.services.KafkaTopic;
import org.folio.services.SRSKafkaTopicService;
import org.folio.services.SRSKafkaTopicService.SRSKafkaTopic;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;

@ExtendWith(VertxExtension.class)
public class KafkaAdminClientServiceTest {

  private static final String STUB_TENANT = "foo-tenant";
  private KafkaAdminClient mockClient;
  private Vertx vertx;
  private SRSKafkaTopicService srsKafkaTopicService;

  @BeforeEach
  void setUp() {
    System.setProperty(KAFKA_ENV, KAFKA_ENV_ID);
    vertx = mock(Vertx.class);
    mockClient = mock(KafkaAdminClient.class);
    srsKafkaTopicService = mock(SRSKafkaTopicService.class);
    KafkaTopic[] topicObjects = {
      MARC_BIB,
      new SRSKafkaTopic("DI_PARSED_RECORDS_CHUNK_SAVED", 10),
      new SRSKafkaTopic("DI_SRS_MARC_BIB_RECORD_MODIFIED_READY_FOR_POST_PROCESSING", 10),
      new SRSKafkaTopic("DI_SRS_MARC_AUTHORITY_RECORD_MATCHED", 10),
      new SRSKafkaTopic("DI_SRS_MARC_AUTHORITY_RECORD_NOT_MATCHED", 10),
      new SRSKafkaTopic("DI_SRS_MARC_AUTHORITY_RECORD_DELETED", 10),
      new SRSKafkaTopic("DI_SRS_MARC_HOLDINGS_HOLDING_HRID_SET", 10),
      new SRSKafkaTopic("DI_SRS_MARC_HOLDINGS_RECORD_MODIFIED_READY_FOR_POST_PROCESSING", 10),
      new SRSKafkaTopic("DI_SRS_MARC_HOLDINGS_RECORD_UPDATED", 10),
      new SRSKafkaTopic("DI_SRS_MARC_BIB_RECORD_UPDATED", 10),
      new SRSKafkaTopic("DI_SRS_MARC_AUTHORITY_RECORD_MODIFIED_READY_FOR_POST_PROCESSING", 10),
      new SRSKafkaTopic("DI_LOG_SRS_MARC_AUTHORITY_RECORD_CREATED", 10),
      new SRSKafkaTopic("DI_LOG_SRS_MARC_AUTHORITY_RECORD_UPDATED", 10),
      new SRSKafkaTopic("DI_SRS_MARC_HOLDINGS_RECORD_MATCHED", 10),
      new SRSKafkaTopic("DI_SRS_MARC_HOLDINGS_RECORD_NOT_MATCHED", 10),
      new SRSKafkaTopic("DI_SRS_MARC_AUTHORITY_RECORD_UPDATED", 10)
    };

    when(srsKafkaTopicService.createTopicObjects()).thenReturn(topicObjects);
  }

  @Test
  void shouldCreateTopicIfAlreadyExist(VertxTestContext testContext) {
    when(mockClient.createTopics(anyList()))
      .thenReturn(failedFuture(new TopicExistsException("x")))
      .thenReturn(failedFuture(new TopicExistsException("y")))
      .thenReturn(failedFuture(new TopicExistsException("z")))
      .thenReturn(succeededFuture());
    when(mockClient.listTopics()).thenReturn(succeededFuture(Set.of("old")));
    when(mockClient.close()).thenReturn(succeededFuture());

    createKafkaTopicsAsync(mockClient)
      .onComplete(testContext.succeeding(notUsed -> {
        verify(mockClient, times(4)).listTopics();
        verify(mockClient, times(4)).createTopics(anyList());
        verify(mockClient, times(1)).close();
        testContext.completeNow();
      }));
  }

  @Test
  void shouldFailIfExistExceptionIsPermanent(VertxTestContext testContext) {
    when(mockClient.createTopics(anyList())).thenReturn(failedFuture(new TopicExistsException("x")));
    when(mockClient.listTopics()).thenReturn(succeededFuture(Set.of("old")));
    when(mockClient.close()).thenReturn(succeededFuture());

    createKafkaTopicsAsync(mockClient)
      .onComplete(testContext.failing(e -> {
        assertThat(e, instanceOf(TopicExistsException.class));
        verify(mockClient, times(1)).close();
        testContext.completeNow();
      }));
  }

  @Test
  void shouldNotCreateTopicOnOther(VertxTestContext testContext) {
    when(mockClient.createTopics(anyList())).thenReturn(failedFuture(new RuntimeException("err msg")));
    when(mockClient.listTopics()).thenReturn(succeededFuture(Set.of("old")));
    when(mockClient.close()).thenReturn(succeededFuture());

    createKafkaTopicsAsync(mockClient)
      .onComplete(testContext.failing(cause -> {
          assertEquals("err msg", cause.getMessage());
          verify(mockClient, times(1)).close();
          testContext.completeNow();
        }
      ));
  }

  @Test
  void shouldCreateTopicIfNotExist(VertxTestContext testContext) {
    when(mockClient.createTopics(anyList())).thenReturn(succeededFuture());
    when(mockClient.listTopics()).thenReturn(succeededFuture(Set.of("old")));
    when(mockClient.close()).thenReturn(succeededFuture());

    createKafkaTopicsAsync(mockClient)
      .onComplete(testContext.succeeding(notUsed -> {

        @SuppressWarnings("unchecked")
        final ArgumentCaptor<List<NewTopic>> createTopicsCaptor = forClass(List.class);

        verify(mockClient, times(1)).createTopics(createTopicsCaptor.capture());
        verify(mockClient, times(1)).close();

        // Only these items are expected, so implicitly checks size of list
        assertThat(getTopicNames(createTopicsCaptor), containsInAnyOrder(allExpectedTopics.toArray()));
        testContext.completeNow();
      }));
  }

  private List<String> getTopicNames(ArgumentCaptor<List<NewTopic>> createTopicsCaptor) {
    return createTopicsCaptor.getAllValues().getFirst().stream()
      .map(NewTopic::getName)
      .toList();
  }

  private Future<Void> createKafkaTopicsAsync(KafkaAdminClient client) {
    try (var mocked = mockStatic(KafkaAdminClient.class)) {
      mocked.when(() -> KafkaAdminClient.create(eq(vertx), anyMap())).thenReturn(client);

      return new KafkaAdminClientService(vertx)
        .createKafkaTopics(srsKafkaTopicService.createTopicObjects(), STUB_TENANT);
    }
  }

  private final Set<String> allExpectedTopics = Set.of(
    "test-env.foo-tenant.srs.marc-bib",
    "test-env.Default.foo-tenant.DI_PARSED_RECORDS_CHUNK_SAVED",
    "test-env.Default.foo-tenant.DI_SRS_MARC_BIB_RECORD_MODIFIED_READY_FOR_POST_PROCESSING",
    "test-env.Default.foo-tenant.DI_SRS_MARC_AUTHORITY_RECORD_MATCHED",
    "test-env.Default.foo-tenant.DI_SRS_MARC_AUTHORITY_RECORD_NOT_MATCHED",
    "test-env.Default.foo-tenant.DI_SRS_MARC_AUTHORITY_RECORD_DELETED",
    "test-env.Default.foo-tenant.DI_SRS_MARC_HOLDINGS_HOLDING_HRID_SET",
    "test-env.Default.foo-tenant.DI_SRS_MARC_HOLDINGS_RECORD_MODIFIED_READY_FOR_POST_PROCESSING",
    "test-env.Default.foo-tenant.DI_SRS_MARC_HOLDINGS_RECORD_UPDATED",
    "test-env.Default.foo-tenant.DI_SRS_MARC_BIB_RECORD_UPDATED",
    "test-env.Default.foo-tenant.DI_SRS_MARC_AUTHORITY_RECORD_MODIFIED_READY_FOR_POST_PROCESSING",
    "test-env.Default.foo-tenant.DI_LOG_SRS_MARC_AUTHORITY_RECORD_CREATED",
    "test-env.Default.foo-tenant.DI_LOG_SRS_MARC_AUTHORITY_RECORD_UPDATED",
    "test-env.Default.foo-tenant.DI_SRS_MARC_HOLDINGS_RECORD_MATCHED",
    "test-env.Default.foo-tenant.DI_SRS_MARC_HOLDINGS_RECORD_NOT_MATCHED",
    "test-env.Default.foo-tenant.DI_SRS_MARC_AUTHORITY_RECORD_UPDATED"
  );
}
