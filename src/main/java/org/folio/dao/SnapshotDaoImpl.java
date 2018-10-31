package org.folio.dao;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.sql.UpdateResult;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.rest.persist.Criteria.Criteria;
import org.folio.rest.persist.Criteria.Criterion;
import org.folio.rest.persist.Criteria.Limit;
import org.folio.rest.persist.Criteria.Offset;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.persist.cql.CQLWrapper;
import org.folio.rest.persist.interfaces.Results;
import org.z3950.zing.cql.cql2pgjson.CQL2PgJSON;

import java.util.List;
import java.util.Optional;

public class SnapshotDaoImpl implements SnapshotDao {

  private static final String SNAPSHOTS_TABLE = "snapshots";
  private static final String SNAPSHOT_ID_FIELD = "'jobExecutionId'";

  private PostgresClient pgClient;

  public SnapshotDaoImpl(Vertx vertx, String tenantId) {
    pgClient = PostgresClient.getInstance(vertx, tenantId);
  }

  @Override
  public Future<List<Snapshot>> getSnapshots(String query, int offset, int limit) {
    Future<Results<Snapshot>> future = Future.future();
    try {
      String[] fieldList = {"*"};
      CQLWrapper cql = getCQL(query, limit, offset);
      pgClient.get(SNAPSHOTS_TABLE, Snapshot.class, fieldList, cql, true, false, future.completer());
    } catch (Exception e) {
      future.fail(e);
    }
    return future.map(Results::getResults);
  }

  @Override
  public Future<Optional<Snapshot>> getSnapshotById(String id) {
    Future<Results<Snapshot>> future = Future.future();
    try {
      Criteria idCrit = constructCriteria(SNAPSHOT_ID_FIELD, id);
      pgClient.get(SNAPSHOTS_TABLE, Snapshot.class, new Criterion(idCrit), true, false, future.completer());
    } catch (Exception e) {
      future.fail(e);
    }
    return future
      .map(Results::getResults)
      .map(Snapshots -> Snapshots.isEmpty() ? Optional.empty() : Optional.of(Snapshots.get(0)));
  }

  @Override
  public Future<String> saveSnapshot(Snapshot snapshot) {
    Future<String> future = Future.future();
    pgClient.save(SNAPSHOTS_TABLE, snapshot.getJobExecutionId(), snapshot, future.completer());
    return future;
  }

  @Override
  public Future<Boolean> updateSnapshot(Snapshot snapshot) {
    Future<UpdateResult> future = Future.future();
    try {
      Criteria idCrit = constructCriteria(SNAPSHOT_ID_FIELD, snapshot.getJobExecutionId());
      pgClient.update(SNAPSHOTS_TABLE, snapshot, new Criterion(idCrit), true, future.completer());
    } catch (Exception e) {
      future.fail(e);
    }
    return future.map(updateResult -> updateResult.getUpdated() == 1);
  }

  @Override
  public Future<Boolean> deleteSnapshot(String id) {
    Future<UpdateResult> future = Future.future();
    pgClient.delete(SNAPSHOTS_TABLE, id, future.completer());
    return future.map(updateResult -> updateResult.getUpdated() == 1);
  }

  /**
   * Build CQL from request URL query
   *
   * @param query - query from URL
   * @param limit - limit of records for pagination
   * @return - CQL wrapper for building postgres request to database
   * @throws org.z3950.zing.cql.cql2pgjson.FieldException field exception
   */
  private CQLWrapper getCQL(String query, int limit, int offset)
    throws org.z3950.zing.cql.cql2pgjson.FieldException {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON(SNAPSHOTS_TABLE + ".jsonb");
    return new CQLWrapper(cql2pgJson, query)
      .setLimit(new Limit(limit))
      .setOffset(new Offset(offset));
  }

  /**
   * Builds criteria by which db result is filtered
   *
   * @param jsonbField - json key name
   * @param value - value corresponding to the key
   * @return - Criteria object
   */
  private Criteria constructCriteria(String jsonbField, String value) {
    Criteria criteria = new Criteria();
    criteria.addField(jsonbField);
    criteria.setOperation("=");
    criteria.setValue(value);
    return criteria;
  }
}
