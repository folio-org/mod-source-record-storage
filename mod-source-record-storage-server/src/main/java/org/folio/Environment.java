package org.folio;

public class Environment {

  private Environment() { }

  private static final String MODULE_PREFIX = "srs";

  public static String environmentName() {
    return System.getenv().getOrDefault("ENV", "folio");
  }

  public static String modulePrefix() {
    return MODULE_PREFIX;
  }
}
