package org.folio.dao;

import java.util.List;
import java.util.Optional;

import org.folio.dao.filter.BeanFilter;

import io.vertx.core.Future;

public interface BeanDao<B, C, F extends BeanFilter> {

  public Future<Optional<B>> getById(String id, String tenantId);

  public Future<C> getByFilter(F filter, int offset, int limit, String tenantId);

  public Future<B> save(B bean, String tenantId);

  public Future<List<B>> save(List<B> beans, String tenantId);

  public Future<B> update(B bean, String tenantId);

  public Future<Boolean> delete(String id, String tenantId);

  public String getTableName();

  public String getId(B bean);

}