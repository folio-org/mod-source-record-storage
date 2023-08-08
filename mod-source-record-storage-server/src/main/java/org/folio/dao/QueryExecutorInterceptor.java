package org.folio.dao;

import io.github.jklingsporn.vertx.jooq.classic.reactivepg.ReactiveClassicGenericQueryExecutor;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.impl.NoStackTraceThrowable;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.SuperMethodCall;
import net.bytebuddy.matcher.ElementMatchers;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.channels.ClosedChannelException;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

/**
 * This class provides an interceptor for the `transaction` and `query` methods of
 * `ReactiveClassicGenericQueryExecutor`. It includes functionality for retrying the
 * operations based on a configurable number of retries and delay, specifically for
 * exceptions like `NoStackTraceThrowable` or `ClosedChannelException`.
 */
public class QueryExecutorInterceptor {
  private static final Logger LOGGER = LogManager.getLogger();
  private static int numRetries = 1;
  private static long retryDelay = 1000;

  private QueryExecutorInterceptor(){}

  /**
   * Sets the number of retries for the transaction and query methods.
   *
   * @param numberOfRetries the number of retries to set
   */
  public static void setNumberOfRetries(int numberOfRetries) {
    numRetries = numberOfRetries;
  }

  /**
   * Sets the delay time between retries in milliseconds.
   *
   * @param delay the delay time to set in milliseconds
   */
  public static void setRetryDelay(long delay) {
    if (delay < 0) throw new IllegalArgumentException("delay cannot be less than zero");
    retryDelay = delay;
  }

  /**
   * Generates a subclass of `ReactiveClassicGenericQueryExecutor` with interceptors for the
   * transaction and query methods.
   *
   * @return the generated subclass
   */
  public static Class<? extends ReactiveClassicGenericQueryExecutor> generateClass() {
    return new ByteBuddy()
      .subclass(ReactiveClassicGenericQueryExecutor.class)
      .constructor(ElementMatchers.any()) // Match all constructors
      .intercept(SuperMethodCall.INSTANCE) // Call the original constructor
      .method(ElementMatchers.named("transaction")) // For transaction method
      .intercept(MethodDelegation.to(QueryExecutorInterceptor.class))
      .method(ElementMatchers.named("query")) // For query method
      .intercept(MethodDelegation.to(QueryExecutorInterceptor.class))
      .make()
      .load(QueryExecutorInterceptor.class.getClassLoader())
      .getLoaded();
  }

  /**
   * Interceptor for the query method, with retry functionality.
   *
   * @param superCall the original method call
   * @return the result of the query operation
   */
  @SuppressWarnings("unused")
  public static <U> Future<U> query(
    @net.bytebuddy.implementation.bind.annotation.SuperCall Callable<Future<U>> superCall
  ) {
    LOGGER.trace("query method of ReactiveClassicGenericQueryExecutor proxied");
    return retryOf(() -> {
      try {
        return superCall.call();
      } catch (Throwable e) {
        LOGGER.error("Something happened while attempting to make proxied call for query method", e);
        return Future.failedFuture(e);
      }
    }, numRetries);
  }

  /**
   * Interceptor for the transaction method, with retry functionality.
   *
   * @param superCall the original method call
   * @return the result of the transaction operation
   */
  @SuppressWarnings("unused")
  public static <U> Future<U> transaction(
    @net.bytebuddy.implementation.bind.annotation.SuperCall Callable<Future<U>> superCall
  ) {
    LOGGER.trace("transaction method of ReactiveClassicGenericQueryExecutor proxied");
    return retryOf(() -> {
      try {
        return superCall.call();
      } catch (Throwable e) {
        LOGGER.error("Something happened while attempting to make proxied call for transaction method", e);
        return Future.failedFuture(e);
      }
    }, numRetries);
  }

  /**
   * Private helper method for retrying the transaction or query operations. Introduces
   * a delay and recursively retries in case of specific exceptions.
   *
   * @param supplier the operation to retry
   * @param times    the number of times to retry
   * @return the result of the operation
   */
  private static <U> Future<U> retryOf(Supplier<Future<U>> supplier, int times) {
    if (times <= 0) {
      return supplier.get();
    }

    return supplier.get().recover(err -> {
      if (err instanceof NoStackTraceThrowable ||
        err instanceof ClosedChannelException) {
        Promise<U> promise = Promise.promise();
        Context currentContext = Vertx.currentContext();
        if (currentContext == null) { // don't introduce a delay
          LOGGER.error("Execution error during proxied call. Retrying...", err);
          return retryOf(supplier, times - 1);
        } else {
          Vertx vertx = currentContext.owner();
          LOGGER.error("Execution error during proxied call. Retry in {}ms...", retryDelay, err);
          vertx.setTimer(retryDelay, timerId -> { // Introduce a delay
            retryOf(supplier, times - 1).onComplete(promise); // Recursively call retryOf and pass on the result
          });
          return promise.future();
        }
      }

      return Future.failedFuture(err);
    });
  }
}
