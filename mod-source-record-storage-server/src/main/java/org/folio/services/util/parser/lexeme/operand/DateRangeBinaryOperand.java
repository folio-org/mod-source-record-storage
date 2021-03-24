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
import static org.folio.services.util.parser.lexeme.Lexicon.BINARY_OPERATOR_TO;

/**
 * Given  "005": "20141107001016.0". Available search cases:
 * 005.date = '20141107'            - only single date equality
 * 005.date from '20141106'         - from the given date to now
 * 005.date to '20141108'           - to the beginning of time to the given date
 * 005.date in '20141106-20141108'  - the given date is in the date range including boundaries
 */
public class DateRangeBinaryOperand extends BinaryOperandLexeme {
  private static final String DATE_PATTERN = "YYYYMMDD";
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
    String iField = "\"i" + key.substring(0, key.indexOf('.')) + "\"";
    StringBuilder builder = new StringBuilder();
    if (BINARY_OPERATOR_EQUALS.equals(getOperator()) && !this.rangeSearch) {
      builder
        .append("to_date(substring(").append(iField).append(".\"value\", 1, 8), '").append(DATE_PATTERN).append("')")
        .append(" = ")
        .append("?");
    } else if (BINARY_OPERATOR_FROM.equals(getOperator()) && !this.rangeSearch) {
      builder
        .append("to_date(substring(").append(iField).append(".\"value\", 1, 8), '").append(DATE_PATTERN).append("')")
        .append(" >= ")
        .append("?");
    } else if (BINARY_OPERATOR_TO.equals(getOperator()) && !this.rangeSearch) {
      builder
        .append("to_date(substring(").append(iField).append(".\"value\", 1, 8), '").append(DATE_PATTERN).append("')")
        .append(" <= ")
        .append("?");
    } else if (BINARY_OPERATOR_IN.equals(getOperator()) && this.rangeSearch) {
      builder
        .append("to_date(substring(").append(iField).append(".\"value\", 1, 8), '").append(DATE_PATTERN).append("')")
        .append(" between ")
        .append("? and ?");
    } else {
      throw new IllegalArgumentException(format("The given expression [%s %s '%s'] is not supported", key, operator.getSearchValue(), value));
    }
    return builder.toString();
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
