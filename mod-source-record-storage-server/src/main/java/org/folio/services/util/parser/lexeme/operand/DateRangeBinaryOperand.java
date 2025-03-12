package org.folio.services.util.parser.lexeme.operand;

import org.folio.services.util.parser.lexeme.Lexicon;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;
import static org.folio.services.util.parser.lexeme.Lexicon.BINARY_OPERATOR_EQUALS;
import static org.folio.services.util.parser.lexeme.Lexicon.BINARY_OPERATOR_FROM;
import static org.folio.services.util.parser.lexeme.Lexicon.BINARY_OPERATOR_IN;
import static org.folio.services.util.parser.lexeme.Lexicon.BINARY_OPERATOR_NOT_EQUALS;
import static org.folio.services.util.parser.lexeme.Lexicon.BINARY_OPERATOR_TO;

/**
 * Given  "005": "20141107001016.0". Available search cases:
 * 005.date = '20141107'            - only single date equality
 * 005.date not= '20141107'         - not equals
 * 005.date from '20141106'         - from the given date to now
 * 005.date to '20141108'           - to the beginning of time to the given date
 * 005.date in '20141106-20141108'  - the given date is in the date range including boundaries
 */
public class DateRangeBinaryOperand extends BinaryOperandLexeme {
  private static final String DATE_PATTERN = "yyyymmdd";
  private final boolean rangeSearch;
  private final String firstDate;
  private String secondDate;

  public DateRangeBinaryOperand(String key, Lexicon operator, String value) {
    super(key, operator, value);
    if (value.contains("-")) {
      this.rangeSearch = true;
      String[] dates = value.split("-");
      this.firstDate = dates[0];
      this.secondDate = dates[1];
      validate(this.firstDate);
      validate(this.secondDate);
    } else {
      this.rangeSearch = false;
      this.firstDate = value;
      validate(this.firstDate);
    }
  }

  public static boolean matches(String key) {
    return key.matches("^[0-9]{3}.date$");
  }

  @Override
  public String toSqlRepresentation() {
    String[] keyParts = getKey().split("\\.");
    String field = keyParts[0];
    String fieldNumberToSearch = "\"field_no\" = '" + field + "'";

    StringBuilder builder = new StringBuilder("( " + fieldNumberToSearch + " and immutable_to_date(value)");

    if (BINARY_OPERATOR_EQUALS.equals(getOperator()) && !this.rangeSearch) {
      return builder.append(" = ?)").toString();
    } else if (BINARY_OPERATOR_NOT_EQUALS.equals(getOperator()) && !this.rangeSearch) {
      return builder.append(" <> ?)").toString();
    } else if (BINARY_OPERATOR_FROM.equals(getOperator()) && !this.rangeSearch) {
      return builder.append(" >= ?)").toString();
    } else if (BINARY_OPERATOR_TO.equals(getOperator()) && !this.rangeSearch) {
      return builder.append(" <= ?)").toString();
    } else if (BINARY_OPERATOR_IN.equals(getOperator()) && this.rangeSearch) {
      return builder.append(" between ? and ?)").toString();
    }
    throw new IllegalArgumentException(format("The given expression [%s %s '%s'] is not supported", key, operator.getSearchValue(), value));
  }

  @Override
  public List<String> getBindingParams() {
    List<String> params = new ArrayList<>();
    params.add(this.firstDate);
    if (this.rangeSearch) {
      params.add(this.secondDate);
    }
    return params;
  }

  private void validate(String date) {
    try {
      new SimpleDateFormat(DateRangeBinaryOperand.DATE_PATTERN).parse(date);
    } catch (ParseException e) {
      throw new IllegalArgumentException(format(
        "The given date [%s] is in a wrong format. Expected date pattern: [%s]", date, DateRangeBinaryOperand.DATE_PATTERN)
      );
    }
  }
}
