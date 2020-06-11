package org.folio.services;

import java.util.Collection;
import java.util.Optional;

import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.RecordCollection;
import org.folio.rest.jaxrs.model.SourceRecordCollection;
import org.folio.rest.jaxrs.model.SuppressFromDiscoveryDto;
import org.jooq.Condition;
import org.jooq.OrderField;

import io.vertx.core.Future;

public interface LbRecordService extends RecordService {

  /**
   * {@inheritDoc}
   * @deprecated
   */
  @Override
  @Deprecated
  default Future<RecordCollection> getRecords(String query, int offset, int limit, String tenantId) {
    throw new UnsupportedOperationException("Lookup records by CQL is no longer supported");
  }

  Future<RecordCollection> getRecords(Condition condition, Collection<OrderField<?>> orderFields, int offset, int limit, String tenantId);

  /**
   * {@inheritDoc}
   * @deprecated
   */
  @Override
  @Deprecated
  default Future<SourceRecordCollection> getSourceRecords(String query, int offset, int limit, boolean deletedRecords, String tenantId) {
    throw new UnsupportedOperationException("Lookup source records by CQL is no longer supported");
  }

  Future<SourceRecordCollection> getSourceRecords(Condition condition, Collection<OrderField<?>> orderFields, int offset, int limit, String tenantId);

  /**
   * Searches for {@link Record} by id of external entity which was created from desired record
   *
   * @param externalId external relation id
   * @param idType     id type
   * @param tenantId   tenant id
   * @return future with optional {@link Record}
   */
  Future<Optional<Record>> getRecordByExternalId(String externalId, String idType, String tenantId);

  /**
   * {@inheritDoc}
   * @deprecated
   */
  @Override
  @Deprecated
  default Future<Boolean> updateSuppressFromDiscoveryForRecord(SuppressFromDiscoveryDto suppressFromDiscoveryDto, String tenantId) {
    throw new UnsupportedOperationException("Update supress from discovery by dto is no longer suported");
  }

  /**
   * Change suppress from discovery flag for record by external relation id
   *
   * @param id       id
   * @param idType   external id type
   * @param suppress suppress from discovery
   * @param tenantId tenant id
   * @return - future with true if succeeded
   */
  Future<Boolean> updateSuppressFromDiscoveryForRecord(String id, String idType, Boolean suppress, String tenantId);

}