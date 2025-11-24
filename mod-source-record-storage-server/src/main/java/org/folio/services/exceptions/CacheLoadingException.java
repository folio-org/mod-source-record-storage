package org.folio.services.exceptions;

/**
 * Exception for signaling errors during cache loading.
 */
public class CacheLoadingException extends RuntimeException {

  public CacheLoadingException(String message) {
    super(message);
  }

  /**
   * Constructs a new CacheLoadingException with the specified detail message and cause.
   *
   * @param message the detail message.
   * @param cause   the cause (which is saved for later retrieval by the
   *                {@link #getCause()} method).
   */
  public CacheLoadingException(String message, Throwable cause) {
    super(message, cause);
  }
}
