package org.folio.services.util.parser.lexeme.bracket;

public class BracketLexeme {
  private static final OpenedBracketLexeme OPENED_BRACKET_LEXEME = new OpenedBracketLexeme();
  private static final ClosedBracketLexeme CLOSED_BRACKET_LEXEME = new ClosedBracketLexeme();

  private BracketLexeme() {
  }

  public static OpenedBracketLexeme opened() {
    return OPENED_BRACKET_LEXEME;
  }

  public static ClosedBracketLexeme closed() {
    return CLOSED_BRACKET_LEXEME;
  }
}
