package org.folio.services.util.parser.lexeme;

import java.util.Optional;

public enum Lexicon {
  OPENED_BRACKET("("),
  CLOSED_BRACKET(")"),
  OPERATOR_AND("and"),
  OPERATOR_OR("or"),
  OPERATOR_EQUALS("="),
  OPERATOR_LEFT_ANCHORED_EQUALS("^=");

  Lexicon(String searchExpressionRepresentation) {
    this.searchExpressionRepresentation = searchExpressionRepresentation;
  }

  private String searchExpressionRepresentation;

  public String getSearchExpressionRepresentation() {
    return searchExpressionRepresentation;
  }

  public static Optional<Lexicon> findBySearchExpressionRepresentation(String input) {
    String loweredCaseInput = input.toLowerCase();
    for (Lexicon lexicon : Lexicon.values()) {
      if (lexicon.getSearchExpressionRepresentation().equals(loweredCaseInput)) {
        return Optional.of(lexicon);
      }
    }
    return Optional.empty();
  }
}
