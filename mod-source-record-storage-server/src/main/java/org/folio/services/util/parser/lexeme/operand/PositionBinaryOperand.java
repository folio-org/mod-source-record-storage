package org.folio.services.util.parser.lexeme.operand;

import org.folio.services.util.parser.lexeme.Lexicon;

import static java.lang.String.format;
import static org.folio.services.util.parser.lexeme.Lexicon.OPERATOR_EQUALS;

/**
 * Given "001": "393893". Available search cases:
 * 001.04_02 = "89" - simple equality
 */
public class PositionBinaryOperand extends BinaryOperandLexeme {
  private final String field;
  private final int startPosition;
  private final int endPosition;

  public PositionBinaryOperand(String key, Lexicon operator, String value) {
    super(key, operator, value);
    this.field = key.substring(0, key.indexOf('.'));
    this.startPosition = Integer.parseInt(key.substring(key.indexOf('.') + 1, key.indexOf('_')));
    this.endPosition = Integer.parseInt(key.substring(key.indexOf('_') + 1));
    if (endPosition != value.length()) {
      throw new IllegalArgumentException(format("The length of the value [%s] should be equal to the end position [expected length = %s]", value, endPosition));
    }
  }

  public static boolean isApplicable(String key) {
    return key.matches("^[0-9]{3}.[0-9]{2,3}_[0-9]{2,3}$");
  }

  @Override
  public String toSqlRepresentation() {
    String iField = "\"" + "i" + field + "\"";
    if (OPERATOR_EQUALS.equals(getOperator())) {
      return "substring(" + iField + ".\"value\", " + startPosition + ", " + endPosition + ") = ?";
    } else {
      throw new IllegalArgumentException(format("Operator [%s] is not supported for the given PositionBinary operand", getOperator().getSearchValue()));
    }
  }
}
