package org.folio.verticle;

import io.vertx.core.Future;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.TestMocks;
import org.folio.dao.RecordDao;
import org.folio.dao.RecordDaoImpl;
import org.folio.dao.util.SnapshotDaoUtil;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.services.AbstractLBServiceTest;
import org.folio.services.RecordService;
import org.folio.services.RecordServiceImpl;
import org.folio.services.TenantDataProvider;
import org.folio.services.TenantDataProviderImpl;
import org.jooq.Field;
import org.jooq.Table;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.UUID;

import static org.folio.rest.jaxrs.model.Record.State.ACTUAL;
import static org.folio.rest.jaxrs.model.Record.State.OLD;
import static org.folio.rest.jooq.Tables.MARC_RECORDS_TRACKING;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.name;
import static org.jooq.impl.DSL.table;

@RunWith(VertxUnitRunner.class)
public class MarcIndexersVersionDeletionVerticleTest extends AbstractLBServiceTest {

  private static final String MARC_INDEXERS_TABLE = "marc_indexers";
  private static final String MARC_ID_FIELD = "marc_id";
  private static final String VERSION_FIELD = "version";

  private RecordDao recordDao;
  private TenantDataProvider tenantDataProvider;
  private RecordService recordService;
  private Record record;
  private MarcIndexersVersionDeletionVerticle marcIndexersVersionDeletionVerticle;

  @Before
  public void setUp(TestContext context) {
    Async async = context.async();
    recordDao = new RecordDaoImpl(postgresClientFactory);
    tenantDataProvider = new TenantDataProviderImpl(vertx);
    recordService = new RecordServiceImpl(recordDao);
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
      .withParsedRecord(TestMocks.getRecord(0).getParsedRecord().withId(recordId));

    SnapshotDaoUtil.save(postgresClientFactory.getQueryExecutor(TENANT_ID), snapshot)
      .compose(savedSnapshot -> recordService.saveRecord(record, TENANT_ID))
      .onComplete(save -> {
        if (save.failed()) {
          context.fail(save.cause());
        }
        async.complete();
      });
  }

  @After
  public void cleanUp(TestContext context) {
    Async async = context.async();
    SnapshotDaoUtil.deleteAll(postgresClientFactory.getQueryExecutor(TENANT_ID)).onComplete(delete -> {
      if (delete.failed()) {
        context.fail(delete.cause());
      }
      async.complete();
    });
  }

  @Test
  public void shouldDeleteOldVersionsOfMarcIndexers(TestContext context) {
    Async async = context.async();

    // performs record update in the DB that leads to new indexers creation with incremented version
    // so that previous existing indexers become old and should be deleted
    Future<Boolean> future = recordService.updateRecord(record, TENANT_ID)
      .compose(v -> existOldMarcIndexersVersions())
      .onSuccess(context::assertTrue)
      .compose(v -> marcIndexersVersionDeletionVerticle.deleteOldMarcIndexerVersions())
      .compose(deleteRes -> existOldMarcIndexersVersions());

    future.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      context.assertFalse(ar.result());
      async.complete();
    });
  }

  @Test
  public void shouldDeleteMarcIndexersRelatedToRecordInOldState(TestContext context) {
    Async async = context.async();

    Future<Boolean> future = recordService.updateRecord(record.withState(OLD), TENANT_ID)
      .compose(v -> existMarcIndexersByRecordId(record.getId()))
      .onSuccess(context::assertTrue)
      .compose(v -> marcIndexersVersionDeletionVerticle.deleteOldMarcIndexerVersions())
      .compose(deleteRes -> existMarcIndexersByRecordId(record.getId()));

    future.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      context.assertFalse(ar.result());
      async.complete();
    });
  }

  private Future<Boolean> existOldMarcIndexersVersions() {
    Table<org.jooq.Record> marcIndexers = table(name(MARC_INDEXERS_TABLE));
    Field<UUID> indexersIdField = field(name(MARC_INDEXERS_TABLE, MARC_ID_FIELD), UUID.class);
    Field<Integer> indexersVersionField = field(name(MARC_INDEXERS_TABLE, VERSION_FIELD), Integer.class);

    return postgresClientFactory.getQueryExecutor(TENANT_ID).executeAny(dslContext -> dslContext
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

    return postgresClientFactory.getQueryExecutor(TENANT_ID).executeAny(dslContext -> dslContext
        .select()
        .from(marcIndexers)
        .where(indexersIdField.eq(UUID.fromString(recordId)))
        .limit(1))
      .map(rows -> rows.size() != 0);
  }

}



