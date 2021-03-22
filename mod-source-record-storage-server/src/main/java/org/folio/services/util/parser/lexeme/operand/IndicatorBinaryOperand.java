package org.folio.services.util.parser.lexeme.operand;

import org.folio.services.util.parser.lexeme.Lexicon;

import static java.lang.String.format;
import static org.folio.services.util.parser.lexeme.Lexicon.OPERATOR_EQUALS;
import static org.folio.services.util.parser.lexeme.Lexicon.OPERATOR_LEFT_ANCHORED_EQUALS;

public class IndicatorBinaryOperand extends BinaryOperandLexeme {

  public IndicatorBinaryOperand(String key, Lexicon operator, String value) {
    super(key, operator, value);
  }

  public static boolean isApplicable(String key) {
    return key.matches("[0-9]{3}.ind[1-2]$");
  }

  @Override
  public String toSqlRepresentation() {
    String[] keyParts = getKey().split("\\.");
    String iField = "\"" + "i" + keyParts[0] + "\"";
    String indicator = "\"" + keyParts[1] + "\"";
    if (OPERATOR_LEFT_ANCHORED_EQUALS.equals(getOperator())) {
      return iField + "." + indicator + " like ?";
    } else if (OPERATOR_EQUALS.equals(getOperator())) {
      return iField + "." + indicator + " = ?";
    }
    throw new IllegalArgumentException(format("Operator [%s] is not supported for the given Indicator operand", getOperator().getSearchValue()));
  }
}
