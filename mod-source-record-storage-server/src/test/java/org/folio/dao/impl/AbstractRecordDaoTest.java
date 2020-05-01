package org.folio.dao.impl;

import java.io.IOException;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.folio.dao.BeanDao;
import org.folio.dao.LBRecordDao;
import org.folio.dao.LBSnapshotDao;
import org.folio.dao.filter.BeanFilter;
import org.folio.rest.impl.TestUtil;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.rest.persist.PostgresClient;

import io.vertx.core.CompositeFuture;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;

public abstract class AbstractRecordDaoTest<I, C, F extends BeanFilter, DAO extends BeanDao<I, C, F>> extends AbstractBeanDaoTest<I, C, F, DAO> {

  static final String RAW_RECORD_CONTENT_SAMPLE_PATH = "src/test/resources/rawRecordContent.sample";
  static final String PARSED_RECORD_CONTENT_SAMPLE_PATH = "src/test/resources/parsedRecordContent.sample";

  static String rawMarc;
  static String parsedMarc;

  static {
    try {
      rawMarc = new ObjectMapper().readValue(TestUtil.readFileFromPath(RAW_RECORD_CONTENT_SAMPLE_PATH), String.class);
    } catch (IOException e) {
      e.printStackTrace();
    }
    try {
      parsedMarc = new ObjectMapper().readValue(TestUtil.readFileFromPath(PARSED_RECORD_CONTENT_SAMPLE_PATH), JsonObject.class).encode();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  LBSnapshotDao snapshotDao;

  LBRecordDao recordDao;

  Record mockRecord;

  Record[] mockRecords;

  @Override
  public void createDependentBeans(TestContext context) {
    snapshotDao = new LBSnapshotDaoImpl(postgresClientFactory);
    recordDao = new LBRecordDaoImpl(postgresClientFactory);
    Async async = context.async();

    Snapshot[] snapshots = MockSnapshotFactory.getMockSnapshots();
    CompositeFuture.all(
      snapshotDao.save(snapshots[0], TENANT_ID),
      snapshotDao.save(snapshots[1], TENANT_ID)
    ).setHandler(save -> {
      if (save.failed()) {
        context.fail(save.cause());
      }
      Snapshot snapshot1 = save.result().resultAt(0);
      Snapshot snapshot2 = save.result().resultAt(1);

      mockRecord = MockRecordFactory.getMockRecord(snapshot1);
      mockRecords = MockRecordFactory.getMockRecords(snapshot1, snapshot2);

      recordDao.save(mockRecord, TENANT_ID).setHandler(saveRecord -> {
        if (saveRecord.failed()) {
          context.fail(saveRecord.cause());
        }
        mockRecord = saveRecord.result();

        CompositeFuture.all(
          recordDao.save(mockRecords[0], TENANT_ID),
          recordDao.save(mockRecords[1], TENANT_ID),
          recordDao.save(mockRecords[2], TENANT_ID),
          recordDao.save(mockRecords[3], TENANT_ID),
          recordDao.save(mockRecords[4], TENANT_ID)
        ).setHandler(saveRecords -> {
          if (saveRecords.failed()) {
            context.fail(saveRecords.cause());
          }
  
          mockRecords[0] = saveRecords.result().resultAt(0);
          mockRecords[1] = saveRecords.result().resultAt(1);
          mockRecords[2] = saveRecords.result().resultAt(2);
          mockRecords[3] = saveRecords.result().resultAt(3);
          mockRecords[4] = saveRecords.result().resultAt(4);
  
          async.complete();
        });
      });
    });
  }

  @Override
  public void clearTables(TestContext context) {
    Async async = context.async();
    PostgresClient pgClient = PostgresClient.getInstance(vertx, TENANT_ID);
    String sql = String.format(DELETE_SQL_TEMPLATE, dao.getTableName());
    pgClient.execute(sql, delete -> {
      if (delete.failed()) {
        context.fail(delete.cause());
      }
      String recordSql = String.format(DELETE_SQL_TEMPLATE, recordDao.getTableName());
      pgClient.execute(recordSql, recordDelete -> {
        if (recordDelete.failed()) {
          context.fail(recordDelete.cause());
        }
        String snapshotSql = String.format(DELETE_SQL_TEMPLATE, snapshotDao.getTableName());
        pgClient.execute(snapshotSql, snapshotDelete -> {
          if (snapshotDelete.failed()) {
            context.fail(snapshotDelete.cause());
          }
          async.complete();
        });
      });
    });
  }

  @Override
  public void testSaveGeneratingId(TestContext context) {
    Async async = context.async();
    dao.save(getMockBeanWithoutId(), TENANT_ID).setHandler(res -> {
      context.assertTrue(res.failed());
      async.complete();
    });
  }

}
