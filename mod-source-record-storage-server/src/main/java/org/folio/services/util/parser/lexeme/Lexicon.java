package org.folio.services.util.parser.lexeme;

public enum Lexicon {
  MARC_FIELD("^[0-9]{3}.*"),
  LEADER_FIELD("^p_.*"),
  OPENED_BRACKET("("),
  CLOSED_BRACKET(")"),
  OPERATOR_AND("and"),
  OPERATOR_OR("or"),
  OPERATOR_EQUALS("="),
  OPERATOR_LEFT_ANCHORED_EQUALS("^=");

  Lexicon(String searchValue) {
    this.searchValue = searchValue;
  }

  private String searchValue;

  public String getSearchValue() {
    return searchValue;
  }
}
