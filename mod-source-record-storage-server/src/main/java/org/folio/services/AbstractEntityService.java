package org.folio.services;

import java.util.List;
import java.util.Optional;

import org.folio.dao.EntityDao;
import org.folio.dao.filter.EntityFilter;
import org.springframework.beans.factory.annotation.Autowired;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;

public abstract class AbstractEntityService<E, C, F extends EntityFilter, DAO extends EntityDao<E, C, F>>
    implements EntityService<E, C, F, DAO> {

  @Autowired
  protected DAO dao;

  public Future<Optional<E>> getById(String id, String tenantId) {
    return dao.getById(id, tenantId);
  }

  public Future<C> getByFilter(F filter, int offset, int limit, String tenantId) {
    return dao.getByFilter(filter, offset, limit, tenantId);
  }

  public void getByFilter(F filter, int offset, int limit, String tenantId, Handler<E> handler, 
      Handler<AsyncResult<Void>> replyHandler) {
    dao.getByFilter(filter, offset, limit, tenantId, handler, replyHandler);
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