package org.folio.services.util.parser.lexeme.bracket;

import org.folio.services.util.parser.lexeme.Lexeme;
import org.folio.services.util.parser.lexeme.LexemeType;

public class OpenedBracketLexeme implements Lexeme {

  @Override
  public LexemeType getType() {
    return LexemeType.OPENED_BRACKET;
  }

  @Override
  public String toSqlRepresentation() {
    return "(";
  }
}
