package org.folio.services.util.parser.lexeme.operator;

import org.folio.services.util.parser.lexeme.Lexeme;
import org.folio.services.util.parser.lexeme.LexemeType;
import org.folio.services.util.parser.lexeme.Lexicon;

import static org.apache.commons.lang3.StringUtils.SPACE;

public class OperatorLexeme implements Lexeme {
  private final Lexicon operator;

  public OperatorLexeme(Lexicon operator) {
    this.operator = operator;
  }

  public static OperatorLexeme of(Lexicon operator) {
    return new OperatorLexeme(operator);
  }

  @Override
  public LexemeType getType() {
    return LexemeType.OPERATOR;
  }

  @Override
  public String toSqlRepresentation() {
    return SPACE + operator.getSearchExpressionRepresentation() + SPACE;
  }
}
