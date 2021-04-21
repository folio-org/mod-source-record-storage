package org.folio.services.util.parser.lexeme;

public enum Lexicon {
  MARC_FIELD("^[0-9]{3}.*"),
  LEADER_FIELD("^p_.*"),
  OPENED_BRACKET("("),
  CLOSED_BRACKET(")"),
  BOOLEAN_OPERATOR_AND("and"),
  BOOLEAN_OPERATOR_OR("or"),
  BINARY_OPERATOR_EQUALS("="),
  BINARY_OPERATOR_NOT_EQUALS("not="),
  BINARY_OPERATOR_LEFT_ANCHORED_EQUALS("^="),
  BINARY_OPERATOR_FROM("from"),
  BINARY_OPERATOR_TO("to"),
  BINARY_OPERATOR_IN("in"),
  BINARY_OPERATOR_IS("is");

  private final String searchValue;

  Lexicon(String searchValue) {
    this.searchValue = searchValue;
  }

  public String getSearchValue() {
    return searchValue;
  }
}
