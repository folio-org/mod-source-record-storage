package org.folio.services.util.parser.lexeme.operand;

import org.folio.services.util.parser.lexeme.Lexicon;

import static java.lang.String.format;
import static org.folio.services.util.parser.lexeme.Lexicon.OPERATOR_EQUALS;
import static org.folio.services.util.parser.lexeme.Lexicon.OPERATOR_LEFT_ANCHORED_EQUALS;

public class SubFieldBinaryOperand extends BinaryOperandLexeme {

  public SubFieldBinaryOperand(String key, Lexicon lexiconOperator, String value) {
    super(key, lexiconOperator, value);
  }

  public static boolean isApplicable(String key) {
    return key.contains(".") && key.split("\\.")[1].length() == 1;
  }

  @Override
  public String toSqlRepresentation() {
    String[] keyParts = getKey().split("\\.");
    String iField = "\"" + "i" + keyParts[0] + "\"";
    String subField = keyParts[1];
    StringBuilder stringBuilder = new StringBuilder()
      .append("(").append(iField).append(".\"subfield_no\" = '").append(subField).append("'")
      .append(" and ")
      .append(iField).append(".\"value\" ");
    if (OPERATOR_LEFT_ANCHORED_EQUALS.equals(getOperator())) {
      return stringBuilder.append("like ?)").toString();
    } else if (OPERATOR_EQUALS.equals(getOperator())) {
      return stringBuilder.append("= ?)").toString();
    }
    throw new IllegalArgumentException(format("Operator %s is not supported for the given DataField operand", getOperator()));
  }
}
