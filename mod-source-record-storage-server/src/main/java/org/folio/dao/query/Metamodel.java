package org.folio.dao.query;

import static java.lang.annotation.ElementType.TYPE;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Documented
@Target({ TYPE })
@Retention(RetentionPolicy.RUNTIME)
public @interface Metamodel {

  Class<?> entity();

  String table();

  Property[] properties();

  @Documented
  @Target({ TYPE })
  @Retention(RetentionPolicy.RUNTIME)
  public @interface Property {

    String path();

    String column();

  }

}