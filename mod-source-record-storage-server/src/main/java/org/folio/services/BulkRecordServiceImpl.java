package org.folio.services;

import io.vertx.core.http.HttpServerResponse;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowStream;
import io.vertx.sqlclient.Tuple;
import org.folio.dao.BulkRecordDao;
import org.folio.rest.jaxrs.model.SearchRecordRqBody;
import org.folio.rest.util.OkapiConnectionParams;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
public class BulkRecordServiceImpl implements BulkRecordService {

  private BulkRecordDao bulkRecordDao;

  public BulkRecordServiceImpl(@Autowired BulkRecordDao bulkRecordDao) {
    this.bulkRecordDao = bulkRecordDao;
  }

  public void searchRecords(HttpServerResponse response, SearchRecordRqBody searchRecordBody, OkapiConnectionParams okapiParams) {
    String query = generateSQLQuery(searchRecordBody);
    Tuple params = getParamsForQuery(searchRecordBody);

    bulkRecordDao.searchRecords(query, params, okapiParams.getTenantId()).onComplete(ar -> {
      RowStream<Row> rowStream = ar.result();
      rowStream.pipeTo(new WriteStreamRowToResponseWrapper(response), completionEvent -> {
        rowStream.close();
      });
    });
  }

  private Tuple getParamsForQuery(SearchRecordRqBody searchRecordBody) {
    String snapshotId = searchRecordBody.getSearchExpression().replace("snapshotId=", "");
    return Tuple.of(UUID.fromString(snapshotId));
  }

  private String generateSQLQuery(SearchRecordRqBody searchRecordBody) {
    return "SELECT * FROM diku_mod_source_record_storage.records_lb WHERE snapshot_id = $1";
  }
}
