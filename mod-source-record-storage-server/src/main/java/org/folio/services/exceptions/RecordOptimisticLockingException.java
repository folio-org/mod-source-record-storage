package org.folio.services.exceptions;

/**
 * Thrown when a record generation cannot be saved because the record was modified by another process
 * (e.g. quickMARC or another job) after it had been matched by the current operation.
 */
public class RecordOptimisticLockingException extends RuntimeException {

  private static final String MESSAGE_TEMPLATE = "Optimistic locking: record with matchedId '%s' was modified by another "
    + "process (snapshot '%s') while it was being processed by the current operation (snapshot '%s'). Generation %s cannot be "
    + "saved, please repeat the operation to apply changes to the latest version of the record";

  public RecordOptimisticLockingException(String matchedId, String currentSnapshotId, String incomingSnapshotId,
                                          Integer incomingGeneration) {
    super(MESSAGE_TEMPLATE.formatted(matchedId, currentSnapshotId, incomingSnapshotId, incomingGeneration));
  }
}
