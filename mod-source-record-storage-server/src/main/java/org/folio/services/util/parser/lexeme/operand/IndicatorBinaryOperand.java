package org.folio.services.util.parser.lexeme.operand;

import org.folio.services.util.parser.lexeme.Lexicon;

import static java.lang.String.format;
import static org.folio.services.util.parser.lexeme.Lexicon.BINARY_OPERATOR_EQUALS;
import static org.folio.services.util.parser.lexeme.Lexicon.BINARY_OPERATOR_IS;
import static org.folio.services.util.parser.lexeme.Lexicon.BINARY_OPERATOR_LEFT_ANCHORED_EQUALS;
import static org.folio.services.util.parser.lexeme.Lexicon.BINARY_OPERATOR_NOT_EQUALS;

/**
 * Given
 * "240": {
 *      "subfields": [...]
 *      "ind1": "1",
 *      "ind2": " "
 * }
 * Available search cases:
 * 240.ind1 = '1'        - simple equality, use '#' to search by empty values (240.ind2 = "#")
 * 240.ind1 ^= '1'       - left-anchored equality
 * 240.ind2 not= '0'     - not equals
 * 010.ind1 is 'present' - is present
 * 010.ind2 is 'absent'  - is absent
 */
public class IndicatorBinaryOperand extends BinaryOperandLexeme {

  public IndicatorBinaryOperand(String key, Lexicon operator, String value) {
    super(key, operator, value);
  }

  public static boolean matches(String key) {
    return key.matches("^[0-9]{3}.ind[1-2]$");
  }

  @Override
  public String toSqlRepresentation() {
    String[] keyParts = getKey().split("\\.");
    var field = keyParts[0];
    var indicator = keyParts[1];
    var sqlRepresentation = "\"" + "i" + field + "\"" + "." + "\"" + indicator + "\"";
    if (BINARY_OPERATOR_LEFT_ANCHORED_EQUALS.equals(getOperator())) {
      return sqlRepresentation + " like ?";
    } else if (BINARY_OPERATOR_EQUALS.equals(getOperator())) {
      return sqlRepresentation + " = ?";
    } else if (BINARY_OPERATOR_NOT_EQUALS.equals(getOperator())) {
      return sqlRepresentation + " <> ?";
    } else if (BINARY_OPERATOR_IS.equals(getOperator())) {
      return PresenceBinaryOperand.getSqlRepresentationForIndicator(field, indicator, value);
    }
    throw new IllegalArgumentException(format("Operator [%s] is not supported for the given Indicator operand", getOperator().getSearchValue()));
  }
}
