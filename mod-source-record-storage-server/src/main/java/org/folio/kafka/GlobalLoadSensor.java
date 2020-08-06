package org.folio.kafka;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * This is the simplest solution to track the load
 * Further it makes sense to consider usage of SPI metrics engine profided by Vert.x
 */


public class GlobalLoadSensor {
  private final AtomicInteger index = new AtomicInteger();

  public int increment() {
    return index.incrementAndGet();
  }

  public int decrement() {
    return index.decrementAndGet();
  }

  public int current() {
    return index.get();
  }

}
