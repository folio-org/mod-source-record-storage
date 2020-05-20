package org.folio.dao.util;

import static java.util.Objects.nonNull;

import java.time.ZoneOffset;
import java.util.Date;
import java.util.UUID;

import io.vertx.sqlclient.Tuple;

/**
 * Simple {@link Tuple} builder. Reduces complexity of non null checks.
 */
public class TupleWrapper {

  private final Tuple tuple;

  private TupleWrapper() {
    tuple = Tuple.tuple();
  }

  public TupleWrapper addUUID(String uuid) {
    if (nonNull(uuid)) {
      tuple.addUUID(UUID.fromString(uuid));
    } else {
      tuple.addValue(null);
    }
    return this;
  }

  public TupleWrapper addInteger(Integer integer) {
    tuple.addInteger(integer);
    return this;
  }

  public TupleWrapper addString(String string) {
    tuple.addString(string);
    return this;
  }

  public TupleWrapper addBoolean(Boolean bool) {
    tuple.addBoolean(bool);
    return this;
  }

  public TupleWrapper addValue(Object value) {
    tuple.addValue(value);
    return this;
  }

  public TupleWrapper addNull() {
    tuple.addString(null);
    return this;
  }

  public TupleWrapper addEnum(Enum<?> e) {
    if (nonNull(e)) {
      tuple.addString(e.toString());
    } else {
      tuple.addValue(null);
    }
    return this;
  }

  public TupleWrapper addOffsetDateTime(Date date) {
    if (nonNull(date)) {
      tuple.addOffsetDateTime(date.toInstant().atOffset(ZoneOffset.UTC));
    } else {
      tuple.addValue(null);
    }
    return this;
  }

  public Tuple get() {
    return tuple;
  }

  public static TupleWrapper of() {
    return new TupleWrapper();
  }

}