package org.folio.rest.impl.wrapper;

import io.vertx.codegen.annotations.Nullable;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.streams.WriteStream;
import io.vertx.sqlclient.Row;
import org.apache.commons.lang3.StringUtils;

import java.util.UUID;

import static java.lang.String.format;
import static org.folio.rest.jooq.Tables.RECORDS_LB;

/**
 * The stream needed to build HTTP response following the pre-defined schema:
 * {
 * "records" : array of instance UUIDs,
 * "totalCount" : integer
 * }
 */
public class SearchRecordIdsWriteStream implements WriteStream<Row> {
  private static final String EMPTY_RESPONSE = "{\n  \"records\" : [ ],\n  \"totalCount\" : 0\n}";
  private static final String RESPONSE_BEGINNING = "{\n  \"records\" : [%s";
  private static final String RESPONSE_ENDING = "],\n  \"totalCount\" : %s\n}";
  private static final String COMMA = ",";
  private static final String DOUBLE_QUOTE = "\"";

  private final HttpServerResponse delegate;
  private int writeIndex = 0;
  private int totalCount = 0;

  public SearchRecordIdsWriteStream(HttpServerResponse delegate) {
    this.delegate = delegate;
  }

  @Override
  public Future<Void> write(Row row) {
    UUID externalUUID = row.getUUID(RECORDS_LB.EXTERNAL_ID.getName());
    this.totalCount = row.getInteger("count");
    if (writeIndex == 0) {
      this.writeIndex++;
      String id = externalUUID == null ? StringUtils.EMPTY : DOUBLE_QUOTE + externalUUID + DOUBLE_QUOTE;
      return this.delegate.write(format(RESPONSE_BEGINNING, id));
    } else {
      this.writeIndex++;
      return this.delegate.write(COMMA + DOUBLE_QUOTE + externalUUID.toString() + DOUBLE_QUOTE);
    }
  }

  @Override
  public Future<Void> end() {
    if (this.writeIndex == 0) {
      return this.delegate.end(EMPTY_RESPONSE);
    } else {
      return this.delegate.end(format(RESPONSE_ENDING, totalCount));
    }
  }

  @Override
  public WriteStream<Row> exceptionHandler(Handler<Throwable> handler) {
    delegate.exceptionHandler(handler);
    return this;
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
  public WriteStream<Row> drainHandler(@Nullable Handler<Void> handler) {
    delegate.drainHandler(handler);
    return this;
  }

}
