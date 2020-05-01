package org.folio.dao.util;

import static com.fasterxml.jackson.databind.util.StdDateFormat.DATE_FORMAT_STR_ISO8601;
import static org.folio.dao.util.DaoUtil.COMMA;
import static org.folio.dao.util.DaoUtil.UNWRAPPED_TEMPLATE;
import static org.folio.dao.util.DaoUtil.WRAPPED_TEMPLATE;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.lang3.StringUtils;

public class ValuesBuilder {

  private final StringBuilder values;

  private final SimpleDateFormat simpleDateFormat;

  private ValuesBuilder() {
    values = new StringBuilder();
    simpleDateFormat = new SimpleDateFormat(DATE_FORMAT_STR_ISO8601);
  }

  private ValuesBuilder(String initialValue) {
    this();
    values.append(initialValue);
  }

  public ValuesBuilder append(Object value) {
    return wrappedWithCheck(value);
  }

  public ValuesBuilder append(String value) {
    if (StringUtils.isNotEmpty(value)) {
      return wrapped(value);
    }
    return this;
  }

  public ValuesBuilder append(Integer value) {
    return unwrappedWithCheck(value);
  }

  public ValuesBuilder append(Boolean value) {
    return unwrappedWithCheck(value);
  }

  public ValuesBuilder append(Date value) {
    if (value != null) {
      return wrapped(simpleDateFormat.format(value));
    }
    return this;
  }

  public String build() {
    return values.toString();
  }

  private ValuesBuilder wrappedWithCheck(Object value) {
    if (value != null) {
      return wrapped(value);
    }
    return this;
  }

  private ValuesBuilder wrapped(Object value) {
    if (values.length() > 0) {
      values.append(COMMA);
    }
    values.append(String.format(WRAPPED_TEMPLATE, value));
    return this;
  }

  private ValuesBuilder unwrappedWithCheck(Object value) {
    if (value != null) {
      return unwrapped(value);
    }
    return this;
  }

  private ValuesBuilder unwrapped(Object value) {
    if (values.length() > 0) {
      values.append(COMMA);
    }
    values.append(String.format(UNWRAPPED_TEMPLATE, value));
    return this;
  }

  public static ValuesBuilder of() {
    return new ValuesBuilder();
  }

  public static ValuesBuilder of(String initialValue) {
    return new ValuesBuilder(String.format(WRAPPED_TEMPLATE, initialValue));
  }

  public static ValuesBuilder of(Integer initialValue) {
    return new ValuesBuilder(String.format(UNWRAPPED_TEMPLATE, initialValue));
  }

}