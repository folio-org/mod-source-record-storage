package org.folio.services;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.core.streams.WriteStream;
import io.vertx.sqlclient.Row;

public class WriteStreamRowToResponseWrapper implements WriteStream<Row> {
  private final HttpServerResponse delegate;

  public WriteStreamRowToResponseWrapper(HttpServerResponse delegate) {
    this.delegate = delegate;
  }

  @Override
  public WriteStream<Row> exceptionHandler(Handler<Throwable> handler) {
    delegate.exceptionHandler(handler);
    return this;
  }

  @Override
  public WriteStream<Row> write(Row row) {
    JsonObject jsonObject = rowToJsonObject(row);
    this.delegate.write(jsonObject.toBuffer());
    return this;
  }

  @Override
  public WriteStream<Row> write(Row row, Handler<AsyncResult<Void>> handler) {
    JsonObject jsonObject = rowToJsonObject(row);
    this.delegate.write(jsonObject.toBuffer());
    return this;
  }

  @Override
  public void end() {
    this.delegate.end();
  }

  @Override
  public void end(Handler<AsyncResult<Void>> handler) {
    this.delegate.end(handler);
  }

  @Override
  public WriteStream<Row> setWriteQueueMaxSize(int maxSize) {
    delegate.setWriteQueueMaxSize(maxSize);
    return this;
  }

  @Override
  public boolean writeQueueFull() {
    return delegate.writeQueueFull();
  }

  @Override
  public WriteStream<Row> drainHandler(Handler<Void> handler) {
    delegate.drainHandler(handler);
    return this;
  }

  private JsonObject rowToJsonObject(Row row) {
    JsonObject jsonObject = new JsonObject();
    for (int i = 0; i < row.size(); i++) {
      Object columnValue = row.getValue(i);
      if (columnValue != null) {
        String columnName = row.getColumnName(i);
        jsonObject.put(columnName, columnValue.toString());
      }
    }
    return jsonObject;
  }
}
