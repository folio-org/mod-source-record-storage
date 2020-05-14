package org.folio.dao.query;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.EqualsBuilder;

public class OrderBy {

  private final String property;

  private final Direction direction;

  private OrderBy(String property) {
    this.property = property;
    this.direction = Direction.ASC;
  }

  private OrderBy(String property, Direction direction) {
    this.property = property;
    this.direction = direction;
  }

  public String getProperty() {
    return property;
  }

  public Direction getDirection() {
    return direction;
  }

  public static OrderBy by(String property) {
    return new OrderBy(property);
  }

  public static OrderBy by(String property, Direction direction) {
    return new OrderBy(property, direction);
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }
    if (!(other instanceof OrderBy)) {
      return false;
    }
    OrderBy rhs = ((OrderBy) other);
    return new EqualsBuilder()
      .append(property, rhs.property)
      .append(direction, rhs.direction)
      .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder()
      .append(property)
      .append(direction)
      .toHashCode();
  }

  public enum Direction {
    ASC,
    DESC
  }

}