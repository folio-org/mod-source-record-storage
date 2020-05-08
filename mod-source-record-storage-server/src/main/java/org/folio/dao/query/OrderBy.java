package org.folio.dao.query;

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

  public static OrderBy of(String property) {
    return new OrderBy(property);
  }

  public static OrderBy of(String property, Direction direction) {
    return new OrderBy(property, direction);
  }

  public enum Direction {
    ASC,
    DESC
  }

}