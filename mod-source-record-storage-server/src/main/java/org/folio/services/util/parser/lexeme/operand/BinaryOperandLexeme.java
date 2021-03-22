package org.folio.services.util.parser.lexeme.operand;

import org.folio.services.util.parser.lexeme.Lexeme;
import org.folio.services.util.parser.lexeme.LexemeType;
import org.folio.services.util.parser.lexeme.Lexicon;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static java.lang.String.format;
import static org.folio.services.util.parser.lexeme.Lexicon.OPERATOR_EQUALS;
import static org.folio.services.util.parser.lexeme.Lexicon.OPERATOR_LEFT_ANCHORED_EQUALS;

public abstract class BinaryOperandLexeme implements BinaryOperand, Lexeme {
  private final static List<Lexicon> BINARY_OPERATORS = Arrays.asList(OPERATOR_EQUALS, OPERATOR_LEFT_ANCHORED_EQUALS);
  protected String key;
  protected Lexicon operator;
  protected String value;

  public BinaryOperandLexeme(String key, Lexicon operator, String value) {
    this.key = key;
    this.operator = operator;
    this.value = value;
  }

  public static BinaryOperandLexeme of(String key, String stringOperator, String value) {
    Optional<Lexicon> optionalLexiconOperator = BINARY_OPERATORS.stream()
      .filter(binaryOperator -> binaryOperator.getSearchValue().equals(stringOperator.toLowerCase()))
      .findFirst();
    if (optionalLexiconOperator.isPresent()) {
      Lexicon lexiconOperator = optionalLexiconOperator.get();
      if (LeaderBinaryOperand.isApplicable(key)) {
        return new LeaderBinaryOperand(key, lexiconOperator, value);
      } else if (IndicatorBinaryOperand.isApplicable(key)) {
        return new IndicatorBinaryOperand(key, lexiconOperator, value);
      } else if (SubFieldBinaryOperand.isApplicable(key)) {
        return new SubFieldBinaryOperand(key, lexiconOperator, value);
      } else if (ValueBinaryOperand.isApplicable(key)) {
        return new ValueBinaryOperand(key, lexiconOperator, value);
      } else if (PositionBinaryOperand.isApplicable(key)) {
        return new PositionBinaryOperand(key, lexiconOperator, value);
      } else {
        throw new IllegalArgumentException(format(
          "The given key is not supported [key: %s, operator: %s, value: %s]",
          key, stringOperator, value
        ));
      }
    } else {
      throw new IllegalArgumentException(format(
        "The given binary operator is not supported [key: %s, operator: %s, value: %s]. Supported operators: %s",
        key, stringOperator, value, Arrays.toString(BINARY_OPERATORS.stream().map(Lexicon::getSearchValue).toArray())
      ));
    }
  }

  @Override
  public LexemeType getType() {
    return LexemeType.BINARY_OPERAND;
  }

  @Override
  public String getField() {
    if (key.contains(".")) {
      return key.substring(0, key.indexOf("."));
    } else {
      return key;
    }
  }

  @Override
  public String getBindingParam() {
    if (OPERATOR_LEFT_ANCHORED_EQUALS.equals(getOperator())) {
      return this.value + "%";
    }
    return this.value;
  }

  public String getKey() {
    return this.key;
  }

  public Lexicon getOperator() {
    return this.operator;
  }
}
