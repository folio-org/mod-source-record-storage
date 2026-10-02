package org.folio.services.exceptions;

/**
 * Thrown when a record generation cannot be saved because the record was modified by another process
 * (e.g. quickMARC or another job) after it had been matched by the current operation.
 */
public class RecordOptimisticLockingException extends RuntimeException {

  public RecordOptimisticLockingException(String message) {
    super(message);
  }
}
