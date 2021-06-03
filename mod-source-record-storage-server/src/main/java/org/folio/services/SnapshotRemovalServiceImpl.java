package org.folio.services;

import static java.lang.String.format;
import static org.apache.http.HttpStatus.SC_NO_CONTENT;
import static org.folio.dao.util.RecordDaoUtil.filterRecordBySnapshotId;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.folio.dao.RecordDao;
import org.folio.dao.util.RecordDaoUtil;
import org.folio.dao.util.RecordType;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.dataimport.util.RestUtil;
import org.folio.rest.jaxrs.model.Record;
import org.jooq.Condition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import org.folio.okapi.common.GenericCompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpMethod;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@Service
public class SnapshotRemovalServiceImpl implements SnapshotRemovalService {

  private static final Logger LOG = LogManager.getLogger();

  private static final String INVENTORY_INSTANCES_PATH = "/inventory/instances/%s";
  private static final int RECORDS_LIMIT = Integer.parseInt(System.getProperty("RECORDS_READING_LIMIT", "50"));

  private SnapshotService snapshotService;
  private RecordService recordService;
  private RecordDao recordDao;

  @Autowired
  public SnapshotRemovalServiceImpl(SnapshotService snapshotService, RecordService recordService, RecordDao recordDao) {
    this.snapshotService = snapshotService;
    this.recordService = recordService;
    this.recordDao = recordDao;
  }

  @Override
  public Future<Boolean> deleteSnapshot(String snapshotId, OkapiConnectionParams params) {
    return deleteInstancesBySnapshotId(snapshotId, params)
      .compose(ar -> snapshotService.deleteSnapshot(snapshotId, params.getTenantId()));
  }

  private Future<Void> deleteInstancesBySnapshotId(String snapshotId, OkapiConnectionParams params) {
    Condition condition = filterRecordBySnapshotId(snapshotId);
    return recordDao.executeInTransaction(txQE -> RecordDaoUtil.countByCondition(txQE, condition), params.getTenantId())
      .compose(totalRecords -> {
        int totalRequestedRecords = 0;
        Future<Void> future = Future.succeededFuture();

        while (totalRequestedRecords < totalRecords) {
          int offset = totalRequestedRecords;
          future = future.compose(ar -> recordService.getRecords(condition, RecordType.MARC_BIB, Collections.emptyList(), offset, RECORDS_LIMIT, params.getTenantId()))
            .compose(recordCollection -> deleteInstances(recordCollection.getRecords(), params));
          totalRequestedRecords += RECORDS_LIMIT;
        }
        return future;
      });
  }

  private Future<Void> deleteInstances(List<Record> records, OkapiConnectionParams params) {
    List<String> instanceIds = records.stream()
      .filter(record -> record.getExternalIdsHolder() != null)
      .map(record -> record.getExternalIdsHolder().getInstanceId())
      .collect(Collectors.toList());

    Promise<Void> promise = Promise.promise();
    List<Future<Boolean>> deleteInstancesFutures = new ArrayList<>();
    for (String instanceId : instanceIds) {
      deleteInstancesFutures.add(deleteInstanceById(instanceId, params));
    }

    GenericCompositeFuture.join(deleteInstancesFutures)
      .onSuccess(ar -> promise.complete())
      .onFailure(promise::fail);
    return promise.future();
  }

  private Future<Boolean> deleteInstanceById(String id, OkapiConnectionParams params) {
    Promise<Boolean> promise = Promise.promise();
    String instacesUrl = format(INVENTORY_INSTANCES_PATH, id);

    RestUtil.doRequest(params, instacesUrl, HttpMethod.DELETE, null)
      .onComplete(responseAr -> {
        if (responseAr.failed()) {
          LOG.error("Error deleting inventory instance by id '{}'", id, responseAr.cause());
          promise.complete(false);
        } else if (responseAr.result().getCode() != SC_NO_CONTENT) {
          LOG.error("Failed to delete inventory instance by id '{}', response status: {}", id, responseAr.result().getCode());
          promise.complete(false);
        } else {
          promise.complete(true);
        }
      });
    return promise.future();
  }
}
