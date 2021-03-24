package org.folio.services.util.parser.lexeme.operand;

import org.folio.services.util.parser.lexeme.Lexicon;

import static java.lang.String.format;
import static org.folio.services.util.parser.lexeme.Lexicon.BINARY_OPERATOR_EQUALS;

/**
 * Given: "leader": "01542ccm a2200361   4500". Available search cases:
 * p_05 = "c"  - simple equality
 * Available leader positions:
 * p_00_04, p_05, p_06, p_07, p_08, p_09, p_10, p_11, p_12_16, p_17, p_18, p_19, p_20, p_21, p_22
 */
public class LeaderBinaryOperand extends BinaryOperandLexeme {

  public LeaderBinaryOperand(String key, Lexicon operator, String value) {
    super(key, operator, value);
  }

  public static boolean matches(String key) {
    return key.matches("^p_.*");
  }

  @Override
  public String toSqlRepresentation() {
    if (BINARY_OPERATOR_EQUALS.equals(getOperator())) {
      return key + " = ?";
    }
    throw new IllegalArgumentException(format("Operator [%s] is not supported for the given Leader operand. Supported operators: [%s]", getOperator().getSearchValue(), BINARY_OPERATOR_EQUALS.getSearchValue()));
  }
}
