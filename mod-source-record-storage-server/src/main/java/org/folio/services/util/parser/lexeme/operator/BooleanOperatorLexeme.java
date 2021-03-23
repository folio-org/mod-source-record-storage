package org.folio.services.util.parser.lexeme.operator;

import org.folio.services.util.parser.lexeme.Lexeme;
import org.folio.services.util.parser.lexeme.LexemeType;
import org.folio.services.util.parser.lexeme.Lexicon;

import static org.apache.commons.lang3.StringUtils.SPACE;

public class BooleanOperatorLexeme implements Lexeme {
  private final Lexicon operator;

  public BooleanOperatorLexeme(Lexicon operator) {
    this.operator = operator;
  }

  public static BooleanOperatorLexeme of(Lexicon operator) {
    return new BooleanOperatorLexeme(operator);
  }

  @Override
  public LexemeType getType() {
    return LexemeType.OPERATOR;
  }

  @Override
  public String toSqlRepresentation() {
    return SPACE + operator.getSearchValue() + SPACE;
  }
}
