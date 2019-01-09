package org.folio.util;

import io.vertx.core.Future;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.folio.HttpStatus;
import org.folio.rest.tools.utils.ValidationHelper;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

public class SourceStorageHelper {

  private static final Logger LOG = LoggerFactory.getLogger("mod-source-record-storage");

  private SourceStorageHelper() {
  }

  public static Response mapExceptionToResponse(Throwable throwable) {
    if (throwable instanceof BadRequestException) {
      return Response.status(HttpStatus.HTTP_BAD_REQUEST.toInt())
        .type(MediaType.TEXT_PLAIN)
        .entity(throwable.getMessage())
        .build();
    }
    if (throwable instanceof NotFoundException) {
      return Response.status(HttpStatus.HTTP_NOT_FOUND.toInt())
        .type(MediaType.TEXT_PLAIN)
        .entity(throwable.getMessage())
        .build();
    }
    Future<Response> validationFuture = Future.future();
    ValidationHelper.handleError(throwable, validationFuture.completer());
    if (validationFuture.isComplete()) {
      Response response = validationFuture.result();
      if (response.getStatus() == HttpStatus.HTTP_INTERNAL_SERVER_ERROR.toInt()) {
        LOG.error(throwable.getMessage(), throwable);
      }
      return response;
    }
    LOG.error(throwable.getMessage(), throwable);
    return Response.status(HttpStatus.HTTP_INTERNAL_SERVER_ERROR.toInt())
      .type(MediaType.TEXT_PLAIN)
      .entity(Response.Status.INTERNAL_SERVER_ERROR.getReasonPhrase())
      .build();
  }
}
