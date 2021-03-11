package org.folio.services.util.parser;

import org.apache.commons.lang.StringUtils;

import java.util.Arrays;
import java.util.function.BiConsumer;

import static java.lang.String.format;

public class ExpressionValidator {
  private static final BiConsumer<String, String> BLANK_OR_EMPTY = (input, key) -> {
    if (input.isBlank() || input.isEmpty()) {
      throw new IllegalArgumentException(format("The input expression should not be black or empty [expression: %s]", key));
    }
  };
  private static final BiConsumer<String, String> CORRECT_BRACKETS = (input, key) -> {
    int opened = StringUtils.countMatches(input, "(");
    int closed = StringUtils.countMatches(input, ")");
    if (opened != closed) {
      throw new IllegalArgumentException(format("The number of opened brackets should be equal to number of closed brackets [expression: %s]", key));
    }
  };
  private static final BiConsumer<String, String> CORRECT_APOSTROPHES = (input, key) -> {
    int apostrophes = StringUtils.countMatches(input, "'");
    if (apostrophes % 2 != 0) {
      throw new IllegalArgumentException(format("Each value in the expression should be surrounded by single quotes [expression: %s]", key));
    }
  };

  public static void validate(String input, String key) {
    Arrays.asList(BLANK_OR_EMPTY, CORRECT_BRACKETS, CORRECT_APOSTROPHES).forEach(rule -> rule.accept(input, key));
  }
}
