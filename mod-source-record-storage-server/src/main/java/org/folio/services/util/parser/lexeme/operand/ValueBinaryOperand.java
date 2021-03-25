package org.folio.services.util.parser.lexeme.operand;

import org.folio.services.util.parser.lexeme.Lexicon;

import static java.lang.String.format;
import static org.folio.services.util.parser.lexeme.Lexicon.BINARY_OPERATOR_EQUALS;
import static org.folio.services.util.parser.lexeme.Lexicon.BINARY_OPERATOR_LEFT_ANCHORED_EQUALS;
import static org.folio.services.util.parser.lexeme.Lexicon.BINARY_OPERATOR_NOT_EQUALS;

/**
 * Given "008": "830419m19559999gw mua". Available search cases:
 * 008.value = '830419m19559999gw mua'    - simple equality
 * 008.value ^= '830419m1'                - left-anchored equality
 * 008.value not= '830419m19559999gw mua' - not equals
 */
public class ValueBinaryOperand extends BinaryOperandLexeme {

  public ValueBinaryOperand(String key, Lexicon operator, String value) {
    super(key, operator, value);
  }

  public static boolean matches(String key) {
    return key.matches("^[0-9]{3}.value$");
  }

  @Override
  public String toSqlRepresentation() {
    StringBuilder stringBuilder = new StringBuilder();
    String[] keyParts = getKey().split("\\.");
    String prefix = stringBuilder.append("\"").append("i").append(keyParts[0]).append("\"").append(".\"")
      .append(keyParts[1]).append("\"").toString();
    if (BINARY_OPERATOR_LEFT_ANCHORED_EQUALS.equals(getOperator())) {
      return prefix + " like ?";
    } else if (BINARY_OPERATOR_EQUALS.equals(getOperator())) {
      return prefix + " = ?";
    } else if (BINARY_OPERATOR_NOT_EQUALS.equals(getOperator())) {
      return stringBuilder.append(" <> ?)").toString();
    }
    throw new IllegalArgumentException(format("Operator [%s] is not supported for the given Value operand", getOperator().getSearchValue()));
  }
}
