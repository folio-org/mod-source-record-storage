package org.folio.dao.exceptions;

public class DuplicateEventException extends RuntimeException {

  public DuplicateEventException(String message) {
    super(message);
  }
}
