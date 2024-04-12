package org.folio.dao;

import org.folio.TestUtil;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.net.UnknownHostException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class PostgresSocketFactoryTest {

  private static final String SERVER_PEM_KEY = "server_pem";
  private static final String SERVER_PEM_VALUE = TestUtil.resourceToString("/tls/server.crt");

  @BeforeAll
  public static void setUp() {
    System.setProperty(SERVER_PEM_KEY, SERVER_PEM_VALUE);
  }

  @Test
  public void createSocketFactory () throws Exception {
    System.setProperty(SERVER_PEM_KEY, SERVER_PEM_VALUE);
    var factory = new PostgresSocketFactory();
    assertThat(factory, CoreMatchers.notNullValue());
    assertThat(factory.getDefaultCipherSuites(), CoreMatchers.notNullValue());
    assertThat(factory.getSupportedCipherSuites(), CoreMatchers.notNullValue());
  }

  @Test
  public void shouldThrowNpeInCaseMissingProperty() {
    System.clearProperty(SERVER_PEM_KEY);
    assertThrows(NullPointerException.class, () -> new PostgresSocketFactory());
  }

  @Test
  public void shouldThrowExceptionInCaseInvalidHost () throws Exception {
    System.setProperty(SERVER_PEM_KEY, SERVER_PEM_VALUE);
    var factory = new PostgresSocketFactory();
    assertThrows(UnknownHostException.class, () -> factory.createSocket("test", 1234));
  }

  @AfterAll
  public static void teardown() {
    System.clearProperty(SERVER_PEM_KEY);
  }
}
