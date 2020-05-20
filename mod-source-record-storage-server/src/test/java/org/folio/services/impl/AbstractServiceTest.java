package org.folio.services.impl;

import org.junit.Before;

import io.vertx.ext.unit.TestContext;

public abstract class AbstractServiceTest {

  static final String TENANT_ID = "diku";

  @Before
  public abstract void createBeans(TestContext context) throws IllegalAccessException;

}