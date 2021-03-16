package org.folio.services.util.parser.lexeme.bracket;

import org.folio.services.util.parser.lexeme.Lexeme;
import org.folio.services.util.parser.lexeme.LexemeType;

public class ClosedBracketLexeme implements Lexeme {

  @Override
  public LexemeType getType() {
    return LexemeType.CLOSED_BRACKET;
  }

  @Override
  public String toSqlRepresentation() {
    return ")";
  }
}
