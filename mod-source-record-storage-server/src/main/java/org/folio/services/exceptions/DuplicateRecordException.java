package org.folio.services.exceptions;

public class DuplicateRecordException extends RuntimeException {

  public DuplicateRecordException(String message) {
    super(message);
  }
}
