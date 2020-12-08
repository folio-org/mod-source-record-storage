package org.folio.services.exceptions;

/**
 * Exception for post processing errors
 */
public class PostProcessingException extends RuntimeException {

  public PostProcessingException(String message) {
    super(message);
  }
}
