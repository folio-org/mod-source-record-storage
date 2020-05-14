package org.folio.dao.query;

import java.util.Collection;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class Where {

  private final String property;

  private final Object value;

  private final Op op;

  private Where(String property, Object value, Op op) {
    this.property = property;
    this.value = value;
    this.op = op;
  }

  public String getProperty() {
    return property;
  }

  public Op getOp() {
    return op;
  }

  public Object getValue() {
    return value;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }
    if (!(other instanceof Where)) {
      return false;
    }
    Where rhs = ((Where) other);
    return new EqualsBuilder()
      .append(property, rhs.property)
      .append(value, rhs.value)
      .append(op, rhs.op)
      .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder()
      .append(property)
      .append(value)
      .append(op)
      .toHashCode();
  }

  public enum Op {
    EQUAL("="),
    GREATER_THEN(">"),
    LESS_THEN("<"),
    GREATER_THEN_OR_EQUAL(">="),
    LESS_THEN_OR_EQUAL("<="),
    NOT_EQUAL("!="),
    LIKE("LIKE"),
    BETWEEN("BETWEEN"),
    IN("IN"),
    AND("AND"),
    OR("OR"),
    START_EXPRESSION("("),
    END_EXPRESSION(")");

    private String token;

    Op(String token) {
      this.token = token;
    }

    public String getToken() {
      return token;
    }

  }

  public static Where equal(String property, Object value) {
    return new Where(property, value, Op.EQUAL);
  }

  public static Where greaterThen(String property, Object value) {
    return new Where(property, value, Op.GREATER_THEN);
  }

  public static Where lessThen(String property, Object value) {
    return new Where(property, value, Op.LESS_THEN);
  }

  public static Where greaterThenOrEqual(String property, Object value) {
    return new Where(property, value, Op.GREATER_THEN_OR_EQUAL);
  }

  public static Where lessThenOrEqual(String property, Object value) {
    return new Where(property, value, Op.LESS_THEN_OR_EQUAL);
  }

  public static Where notEqual(String property, Object value) {
    return new Where(property, value, Op.NOT_EQUAL);
  }

  public static Where like(String property, String value) {
    return new Where(property, value, Op.LIKE);
  }

  public static <T> Where between(String property, T from, T to) {
    return new Where(property, new Object[] { from, to }, Op.BETWEEN);
  }

  public static <T> Where in(String property, Collection<T> values) {
    return new Where(property, values, Op.IN);
  }

  public static Where and() {
    return new Where(null, null, Op.AND);
  }

  public static Where or() {
    return new Where(null, null, Op.OR);
  }

  public static Where startExpression() {
    return new Where(null, null, Op.START_EXPRESSION);
  }

  public static Where endExpression() {
    return new Where(null, null, Op.END_EXPRESSION);
  }

}