package org.folio.services.util.parser.lexeme.operand;

import org.folio.services.util.parser.lexeme.Lexicon;

import java.util.Map;

import static java.lang.String.format;
import static org.folio.rest.jooq.Tables.RECORDS_LB;
import static org.folio.services.util.parser.lexeme.Lexicon.BINARY_OPERATOR_EQUALS;
import static org.folio.services.util.parser.lexeme.Lexicon.BINARY_OPERATOR_NOT_EQUALS;

/**
 * Given: "leader": "01542ccm a2200361   4500". Available search cases:
 * p_05 = 'c'           - simple equality
 * p_00_04 not= '01542' - not equals
 * Available leader positions:
 * p_00_04, p_05, p_06, p_07, p_08, p_09, p_10, p_11, p_12_16, p_17, p_18, p_19, p_20, p_21, p_22
 */
public class LeaderBinaryOperand extends BinaryOperandLexeme {

  private static final Map<String, String> LEADER_POSITIONS_TO_INDEXED_COLUMNS =
    Map.of("p_05", RECORDS_LB.LEADER_RECORD_STATUS.getName());

  private final boolean indexedFieldOperand;

  public LeaderBinaryOperand(String key, Lexicon operator, String value) {
    super(key, operator, value);
    indexedFieldOperand = LEADER_POSITIONS_TO_INDEXED_COLUMNS.containsKey(key);
  }

  public static boolean matches(String key) {
    return key.matches("^p_.*");
  }

  public boolean isIndexedFieldOperand() {
    return indexedFieldOperand;
  }

  @Override
  public String toSqlRepresentation() {
    String columnName = LEADER_POSITIONS_TO_INDEXED_COLUMNS.getOrDefault(key, key);
    if (BINARY_OPERATOR_EQUALS.equals(getOperator())) {
      return columnName + " = ?";
    } else if (BINARY_OPERATOR_NOT_EQUALS.equals(getOperator())) {
      return columnName + " <> ?";
    }
    throw new IllegalArgumentException(format("Operator [%s] is not supported for the given Leader operand. Supported operators: [%s]", getOperator().getSearchValue(), BINARY_OPERATOR_EQUALS.getSearchValue()));
  }
}
