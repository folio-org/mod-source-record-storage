package org.folio.verticle;

import static org.folio.rest.jaxrs.model.Record.State.ACTUAL;
import static org.folio.rest.jaxrs.model.Record.State.OLD;
import static org.folio.rest.jooq.Tables.MARC_RECORDS_TRACKING;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.name;
import static org.jooq.impl.DSL.table;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.vertx.core.Future;
import io.vertx.junit5.VertxTestContext;
import java.util.Map;
import java.util.UUID;

import org.folio.TestMocks;
import org.folio.dao.RecordDao;
import org.folio.dao.RecordDaoImpl;
import org.folio.dao.util.SnapshotDaoUtil;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.rest.jaxrs.model.ExternalIdsHolder;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.services.AbstractLBServiceTest;
import org.folio.services.RecordService;
import org.folio.services.RecordServiceImpl;
import org.folio.services.TenantDataProvider;
import org.folio.services.TenantDataProviderImpl;
import org.folio.services.caches.ConsortiumConfigurationCache;
import org.folio.services.domainevent.RecordDomainEventPublisher;
import org.jooq.Field;
import org.jooq.Table;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class MarcIndexersVersionDeletionVerticleTest extends AbstractLBServiceTest {

  private static final String MARC_INDEXERS_TABLE = "marc_indexers";
  private static final String OLD_RECORDS_TRACKING_TABLE = "old_records_tracking";
  private static final String MARC_ID_FIELD = "marc_id";
  private static final String VERSION_FIELD = "version";

  @Mock
  private RecordDomainEventPublisher recordDomainEventPublisher;
  @Mock
  private ConsortiumConfigurationCache consortiumConfigurationCache;
  private RecordDao recordDao;
  private TenantDataProvider tenantDataProvider;
  private RecordService recordService;
  private Record record;
  private MarcIndexersVersionDeletionVerticle marcIndexersVersionDeletionVerticle;

  @BeforeEach
  void setUp(VertxTestContext testContext) {
    recordDao = new RecordDaoImpl(postgresClientFactory, recordDomainEventPublisher);
    tenantDataProvider = new TenantDataProviderImpl(vertx);
    recordService = new RecordServiceImpl(recordDao, consortiumConfigurationCache);
    marcIndexersVersionDeletionVerticle = new MarcIndexersVersionDeletionVerticle(recordDao, tenantDataProvider);

    String recordId = UUID.randomUUID().toString();
    Snapshot snapshot = TestMocks.getSnapshot(0);

    this.record = new Record()
      .withId(recordId)
      .withState(ACTUAL)
      .withMatchedId(recordId)
      .withSnapshotId(snapshot.getJobExecutionId())
      .withGeneration(0)
      .withRecordType(Record.RecordType.MARC_BIB)
      .withRawRecord(TestMocks.getRecord(0).getRawRecord().withId(recordId))
      .withParsedRecord(TestMocks.getRecord(0).getParsedRecord().withId(recordId))
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(UUID.randomUUID().toString()).withInstanceHrid("hrid00001"));

    var okapiHeaders = Map.of(XOkapiHeaders.TENANT, TENANT_ID);
    postgresClientFactory.getQueryExecutor(TENANT_ID)
      .execute(dsl -> dsl.deleteFrom(table(name(OLD_RECORDS_TRACKING_TABLE))))
      .compose(v -> SnapshotDaoUtil.save(postgresClientFactory.getQueryExecutor(TENANT_ID), snapshot))
      .compose(savedSnapshot -> recordService.saveRecord(record, okapiHeaders))
      .onComplete(testContext.succeedingThenComplete());
  }

  @AfterEach
  void cleanUp(VertxTestContext testContext) {
    SnapshotDaoUtil.deleteAll(postgresClientFactory.getQueryExecutor(TENANT_ID))
      .onComplete(testContext.succeedingThenComplete());
  }

  @Test
  void shouldDeleteOldVersionsOfMarcIndexers(VertxTestContext testContext) {
    var okapiHeaders = Map.of(XOkapiHeaders.TENANT, TENANT_ID);
    recordService.updateRecord(record, okapiHeaders)
      .compose(v -> existOldMarcIndexersVersions())
      .onSuccess(existsBefore -> testContext.verify(() -> assertTrue(existsBefore)))
      .compose(v -> marcIndexersVersionDeletionVerticle.deleteOldMarcIndexerVersions(2))
      .compose(deleteRes -> existOldMarcIndexersVersions())
      .onComplete(testContext.succeeding(existsAfterDelete -> testContext.verify(() -> {
        assertFalse(existsAfterDelete);
        testContext.completeNow();
      })));
  }

  @Test
  void shouldDeleteMarcIndexersRelatedToRecordInOldState(VertxTestContext testContext) {
    var okapiHeaders = Map.of(XOkapiHeaders.TENANT, TENANT_ID);
    recordService.updateRecord(record.withState(OLD), okapiHeaders)
      .compose(v -> existMarcIndexersByRecordId(record.getId()))
      .onSuccess(existsBefore -> testContext.verify(() -> assertTrue(existsBefore)))
      .compose(v -> marcIndexersVersionDeletionVerticle.deleteOldMarcIndexerVersions(2))
      .compose(deleteRes -> existMarcIndexersByRecordId(record.getId()))
      .onComplete(testContext.succeeding(existsAfterDelete -> testContext.verify(() -> {
        assertFalse(existsAfterDelete);
        testContext.completeNow();
      })));
  }

  private Future<Boolean> existOldMarcIndexersVersions() {
    Table<org.jooq.Record> marcIndexers = table(name(MARC_INDEXERS_TABLE));
    Field<UUID> indexersIdField = field(name(MARC_INDEXERS_TABLE, MARC_ID_FIELD), UUID.class);
    Field<Integer> indexersVersionField = field(name(MARC_INDEXERS_TABLE, VERSION_FIELD), Integer.class);

    return postgresClientFactory.getQueryExecutor(TENANT_ID).execute(dslContext -> dslContext
        .select()
        .from(marcIndexers)
        .join(MARC_RECORDS_TRACKING).on(MARC_RECORDS_TRACKING.MARC_ID.eq(indexersIdField))
        .and(indexersVersionField.lessThan(MARC_RECORDS_TRACKING.VERSION))
        .limit(1))
      .map(rows -> rows.size() != 0);
  }

  private Future<Boolean> existMarcIndexersByRecordId(String recordId) {
    Table<org.jooq.Record> marcIndexers = table(name(MARC_INDEXERS_TABLE));
    Field<UUID> indexersIdField = field(name(MARC_INDEXERS_TABLE, MARC_ID_FIELD), UUID.class);

    return postgresClientFactory.getQueryExecutor(TENANT_ID).execute(dslContext -> dslContext
        .select()
        .from(marcIndexers)
        .where(indexersIdField.eq(UUID.fromString(recordId)))
        .limit(1))
      .map(rows -> rows.size() != 0);
  }

}
