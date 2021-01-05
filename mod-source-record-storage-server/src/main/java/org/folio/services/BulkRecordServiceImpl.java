package org.folio.services;

import io.vertx.core.Future;
import org.folio.dao.BulkRecordDao;
import org.folio.rest.util.OkapiConnectionParams;
import org.jooq.Condition;
import org.jooq.Cursor;
import org.jooq.impl.DSL;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class BulkRecordServiceImpl implements BulkRecordService {

  private BulkRecordDao bulkRecordDao;

  public BulkRecordServiceImpl(@Autowired BulkRecordDao bulkRecordDao) {
    this.bulkRecordDao = bulkRecordDao;
  }

  @Override
  public Future<Cursor> searchRecords(OkapiConnectionParams params) {
    Condition noCondition = DSL.noCondition();
    return bulkRecordDao.searchRecords(noCondition, params.getTenantId());
  }
}
