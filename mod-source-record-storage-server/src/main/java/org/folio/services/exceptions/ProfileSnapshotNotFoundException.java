package org.folio.services.exceptions;

/**
 * Signals that a job profile snapshot does not exist (HTTP 404) even after all retries.
 * Being an exceptional completion, it is never retained by the profile snapshot cache.
 */
public class ProfileSnapshotNotFoundException extends RuntimeException {

  public ProfileSnapshotNotFoundException(String message) {
    super(message);
  }
}
