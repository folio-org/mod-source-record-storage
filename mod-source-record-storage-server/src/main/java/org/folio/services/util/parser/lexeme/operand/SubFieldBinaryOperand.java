package org.folio.services.util.parser.lexeme.operand;

import org.folio.services.util.parser.lexeme.Lexicon;

import static java.lang.String.format;
import static org.folio.services.util.parser.lexeme.Lexicon.BINARY_OPERATOR_EQUALS;
import static org.folio.services.util.parser.lexeme.Lexicon.BINARY_OPERATOR_IS;
import static org.folio.services.util.parser.lexeme.Lexicon.BINARY_OPERATOR_LEFT_ANCHORED_EQUALS;
import static org.folio.services.util.parser.lexeme.Lexicon.BINARY_OPERATOR_NOT_EQUALS;

/**
 * Given
 * "010": {
 *    "subfields": [
 *      {
 *        "a": "   55001156/M "
 *      }
 *    ],
 *    "ind1": "",
 *    "ind2": " "
 *  }
 *  Available search cases:
 *  010.a = '   55001156/M '    - simple equality
 *  010.a ^= '   55'            - left-anchored equality
 *  010.a not= '   55001156/M ' - not equals
 *  010.a is 'present'          - check sub field for presence
 *  010.a is 'absent'           - check sub field for absence
 */
public class SubFieldBinaryOperand extends BinaryOperandLexeme {

  public SubFieldBinaryOperand(String key, Lexicon lexiconOperator, String value) {
    super(key, lexiconOperator, value);
  }

  public static boolean matches(String key) {
    return key.matches("^[0-9]{3}.[0-9a-z]$");
  }

  @Override
  public String toSqlRepresentation() {
    String[] keyParts = getKey().split("\\.");
    String field = keyParts[0];
    String iField = "\"" + "i" + field + "\"";
    String subField = keyParts[1];
    StringBuilder stringBuilder = new StringBuilder()
      .append("(").append(iField).append(".\"subfield_no\" = '").append(subField).append("'")
      .append(" and ")
      .append(iField).append(".\"value\" ");
    if (BINARY_OPERATOR_LEFT_ANCHORED_EQUALS.equals(getOperator())) {
      return stringBuilder.append("like ?)").toString();
    } else if (BINARY_OPERATOR_EQUALS.equals(getOperator())) {
      return stringBuilder.append("= ?)").toString();
    } else if (BINARY_OPERATOR_NOT_EQUALS.equals(getOperator())) {
      return stringBuilder.append("<> ?)").toString();
    } else if (BINARY_OPERATOR_IS.equals(getOperator())) {
      return PresenceBinaryOperand.getSqlRepresentationForSubField(field, subField, value);
    }
    throw new IllegalArgumentException(format("Operator [%s] is not supported for the given SubField operand", getOperator().getSearchValue()));
  }
}
