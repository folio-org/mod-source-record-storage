package org.folio.services.kafka;

import static io.vertx.core.Future.failedFuture;
import static io.vertx.core.Future.succeededFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.instanceOf;
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
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.kafka.admin.KafkaAdminClient;
import io.vertx.kafka.admin.NewTopic;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.common.errors.TopicExistsException;
import org.folio.kafka.services.KafkaAdminClientService;
import org.folio.kafka.services.KafkaTopic;
import org.folio.services.SRSKafkaTopicService;
import org.folio.services.SRSKafkaTopicService.SRSKafkaTopic;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;

@RunWith(VertxUnitRunner.class)
public class KafkaAdminClientServiceTest {

  private final String STUB_TENANT = "foo-tenant";
  private KafkaAdminClient mockClient;
  private Vertx vertx;
  @Mock
  private SRSKafkaTopicService srsKafkaTopicService;

  @Before
  public void setUp() {
    vertx = mock(Vertx.class);
    mockClient = mock(KafkaAdminClient.class);
    srsKafkaTopicService = mock(SRSKafkaTopicService.class);
    KafkaTopic[] topicObjects = {
      new SRSKafkaTopic("MARC_BIB", 10),
      new SRSKafkaTopic("DI_PARSED_RECORDS_CHUNK_SAVED", 10),
      new SRSKafkaTopic("DI_SRS_MARC_BIB_INSTANCE_HRID_SET", 10),
      new SRSKafkaTopic("DI_SRS_MARC_BIB_RECORD_MODIFIED", 10),
      new SRSKafkaTopic("DI_SRS_MARC_BIB_RECORD_MODIFIED_READY_FOR_POST_PROCESSING", 10),
      new SRSKafkaTopic("DI_SRS_MARC_BIB_RECORD_MATCHED", 10),
      new SRSKafkaTopic("DI_SRS_MARC_BIB_RECORD_NOT_MATCHED", 10),
      new SRSKafkaTopic("DI_SRS_MARC_AUTHORITY_RECORD_MATCHED", 10),
      new SRSKafkaTopic("DI_SRS_MARC_AUTHORITY_RECORD_NOT_MATCHED", 10),
      new SRSKafkaTopic("DI_SRS_MARC_AUTHORITY_RECORD_DELETED", 10),
      new SRSKafkaTopic("DI_SRS_MARC_HOLDINGS_HOLDING_HRID_SET", 10),
      new SRSKafkaTopic("DI_SRS_MARC_HOLDINGS_RECORD_MODIFIED_READY_FOR_POST_PROCESSING", 10),
      new SRSKafkaTopic("DI_SRS_MARC_HOLDINGS_RECORD_UPDATED", 10),
      new SRSKafkaTopic("DI_SRS_MARC_BIB_RECORD_UPDATED", 10),
      new SRSKafkaTopic("DI_SRS_MARC_AUTHORITY_RECORD_MODIFIED_READY_FOR_POST_PROCESSING", 10),
      new SRSKafkaTopic("DI_SRS_MARC_BIB_RECORD_MATCHED_READY_FOR_POST_PROCESSING", 10),
      new SRSKafkaTopic("DI_LOG_SRS_MARC_AUTHORITY_RECORD_CREATED", 10),
      new SRSKafkaTopic("DI_LOG_SRS_MARC_AUTHORITY_RECORD_UPDATED", 10),
      new SRSKafkaTopic("DI_SRS_MARC_HOLDINGS_RECORD_MATCHED", 10),
      new SRSKafkaTopic("DI_SRS_MARC_HOLDINGS_RECORD_NOT_MATCHED", 10),
      new SRSKafkaTopic("DI_SRS_MARC_AUTHORITY_RECORD_UPDATED", 10)
    };

    when(srsKafkaTopicService.createTopicObjects()).thenReturn(topicObjects);
  }

  @Test
  public void shouldCreateTopicIfAlreadyExist(TestContext testContext) {
    when(mockClient.createTopics(anyList()))
      .thenReturn(failedFuture(new TopicExistsException("x")))
      .thenReturn(failedFuture(new TopicExistsException("y")))
      .thenReturn(failedFuture(new TopicExistsException("z")))
      .thenReturn(succeededFuture());
    when(mockClient.listTopics()).thenReturn(succeededFuture(Set.of("old")));
    when(mockClient.close()).thenReturn(succeededFuture());

    createKafkaTopicsAsync(mockClient)
      .onComplete(testContext.asyncAssertSuccess(notUsed -> {
        verify(mockClient, times(4)).listTopics();
        verify(mockClient, times(4)).createTopics(anyList());
        verify(mockClient, times(1)).close();
      }));
  }

  @Test
  public void shouldFailIfExistExceptionIsPermanent(TestContext testContext) {
    when(mockClient.createTopics(anyList())).thenReturn(failedFuture(new TopicExistsException("x")));
    when(mockClient.listTopics()).thenReturn(succeededFuture(Set.of("old")));
    when(mockClient.close()).thenReturn(succeededFuture());

    createKafkaTopicsAsync(mockClient)
      .onComplete(testContext.asyncAssertFailure(e -> {
        assertThat(e, instanceOf(TopicExistsException.class));
        verify(mockClient, times(1)).close();
      }));
  }

  @Test
  public void shouldNotCreateTopicOnOther(TestContext testContext) {
    when(mockClient.createTopics(anyList())).thenReturn(failedFuture(new RuntimeException("err msg")));
    when(mockClient.listTopics()).thenReturn(succeededFuture(Set.of("old")));
    when(mockClient.close()).thenReturn(succeededFuture());

    createKafkaTopicsAsync(mockClient)
      .onComplete(testContext.asyncAssertFailure(cause -> {
          testContext.assertEquals("err msg", cause.getMessage());
          verify(mockClient, times(1)).close();
        }
      ));
  }

  @Test
  public void shouldCreateTopicIfNotExist(TestContext testContext) {
    when(mockClient.createTopics(anyList())).thenReturn(succeededFuture());
    when(mockClient.listTopics()).thenReturn(succeededFuture(Set.of("old")));
    when(mockClient.close()).thenReturn(succeededFuture());

    createKafkaTopicsAsync(mockClient)
      .onComplete(testContext.asyncAssertSuccess(notUsed -> {

        @SuppressWarnings("unchecked")
        final ArgumentCaptor<List<NewTopic>> createTopicsCaptor = forClass(List.class);

        verify(mockClient, times(1)).createTopics(createTopicsCaptor.capture());
        verify(mockClient, times(1)).close();

        // Only these items are expected, so implicitly checks size of list
        assertThat(getTopicNames(createTopicsCaptor), containsInAnyOrder(allExpectedTopics.toArray()));
      }));
  }

  private List<String> getTopicNames(ArgumentCaptor<List<NewTopic>> createTopicsCaptor) {
    return createTopicsCaptor.getAllValues().get(0).stream()
      .map(NewTopic::getName)
      .collect(Collectors.toList());
  }

  private Future<Void> createKafkaTopicsAsync(KafkaAdminClient client) {
    try (var mocked = mockStatic(KafkaAdminClient.class)) {
      mocked.when(() -> KafkaAdminClient.create(eq(vertx), anyMap())).thenReturn(client);

      return new KafkaAdminClientService(vertx)
        .createKafkaTopics(srsKafkaTopicService.createTopicObjects(), STUB_TENANT);
    }
  }

  private final Set<String> allExpectedTopics = Set.of(
    "folio.Default.foo-tenant.MARC_BIB",
    "folio.Default.foo-tenant.DI_PARSED_RECORDS_CHUNK_SAVED",
    "folio.Default.foo-tenant.DI_SRS_MARC_BIB_INSTANCE_HRID_SET",
    "folio.Default.foo-tenant.DI_SRS_MARC_BIB_RECORD_MODIFIED",
    "folio.Default.foo-tenant.DI_SRS_MARC_BIB_RECORD_MODIFIED_READY_FOR_POST_PROCESSING",
    "folio.Default.foo-tenant.DI_SRS_MARC_BIB_RECORD_MATCHED",
    "folio.Default.foo-tenant.DI_SRS_MARC_BIB_RECORD_NOT_MATCHED",
    "folio.Default.foo-tenant.DI_SRS_MARC_AUTHORITY_RECORD_MATCHED",
    "folio.Default.foo-tenant.DI_SRS_MARC_AUTHORITY_RECORD_NOT_MATCHED",
    "folio.Default.foo-tenant.DI_SRS_MARC_AUTHORITY_RECORD_DELETED",
    "folio.Default.foo-tenant.DI_SRS_MARC_HOLDINGS_HOLDING_HRID_SET",
    "folio.Default.foo-tenant.DI_SRS_MARC_HOLDINGS_RECORD_MODIFIED_READY_FOR_POST_PROCESSING",
    "folio.Default.foo-tenant.DI_SRS_MARC_HOLDINGS_RECORD_UPDATED",
    "folio.Default.foo-tenant.DI_SRS_MARC_BIB_RECORD_UPDATED",
    "folio.Default.foo-tenant.DI_SRS_MARC_AUTHORITY_RECORD_MODIFIED_READY_FOR_POST_PROCESSING",
    "folio.Default.foo-tenant.DI_SRS_MARC_BIB_RECORD_MATCHED_READY_FOR_POST_PROCESSING",
    "folio.Default.foo-tenant.DI_LOG_SRS_MARC_AUTHORITY_RECORD_CREATED",
    "folio.Default.foo-tenant.DI_LOG_SRS_MARC_AUTHORITY_RECORD_UPDATED",
    "folio.Default.foo-tenant.DI_SRS_MARC_HOLDINGS_RECORD_MATCHED",
    "folio.Default.foo-tenant.DI_SRS_MARC_HOLDINGS_RECORD_NOT_MATCHED",
    "folio.Default.foo-tenant.DI_SRS_MARC_AUTHORITY_RECORD_UPDATED"
  );
}
