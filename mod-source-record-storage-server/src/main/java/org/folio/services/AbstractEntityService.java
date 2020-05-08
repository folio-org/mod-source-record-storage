package org.folio.services;

import java.util.List;
import java.util.Optional;

import org.folio.dao.EntityDao;
import org.folio.dao.query.EntityQuery;
import org.springframework.beans.factory.annotation.Autowired;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;

public abstract class AbstractEntityService<E, C, Q extends EntityQuery, DAO extends EntityDao<E, C, Q>>
    implements EntityService<E, C, Q, DAO> {

  @Autowired
  protected DAO dao;

  public Future<Optional<E>> getById(String id, String tenantId) {
    return dao.getById(id, tenantId);
  }

  public Future<C> getByQuery(Q query, int offset, int limit, String tenantId) {
    return dao.getByQuery(query, offset, limit, tenantId);
  }

  public void getByQuery(Q query, int offset, int limit, String tenantId, Handler<E> handler, 
      Handler<AsyncResult<Void>> replyHandler) {
    dao.getByQuery(query, offset, limit, tenantId, handler, replyHandler);
  }

  public Future<E> save(E entity, String tenantId) {
    return dao.save(entity, tenantId);
  }

  public Future<List<E>> save(List<E> entities, String tenantId) {
    return dao.save(entities, tenantId);
  }

  public Future<E> update(E entity, String tenantId) {
    return dao.update(entity, tenantId);
  }

  public Future<Boolean> delete(String id, String tenantId) {
    return dao.delete(id, tenantId);
  }

}