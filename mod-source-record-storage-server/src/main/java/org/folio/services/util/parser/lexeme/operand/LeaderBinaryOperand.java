package org.folio.services.util.parser.lexeme.operand;

import org.folio.services.util.parser.lexeme.Lexicon;

import static java.lang.String.format;
import static org.folio.services.util.parser.lexeme.Lexicon.OPERATOR_EQUALS;

public class LeaderBinaryOperand extends BinaryOperandLexeme {

  public LeaderBinaryOperand(String key, Lexicon operator, String value) {
    super(key, operator, value);
  }

  public static boolean isApplicable(String key) {
    return key.matches("^p_.*");
  }

  @Override
  public String toSqlRepresentation() {
    if (OPERATOR_EQUALS.equals(getOperator())) {
      return key + " = ?";
    }
    throw new IllegalArgumentException(format("Operator [%s] is not supported for the given Leader operand. Supported operators: [%s]", getOperator().getSearchValue(), OPERATOR_EQUALS.getSearchValue()));
  }
}
