package org.folio.services.util.parser.lexeme.operand;

import org.folio.services.util.parser.lexeme.Lexeme;
import org.folio.services.util.parser.lexeme.LexemeType;
import org.folio.services.util.parser.lexeme.Lexicon;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static java.lang.String.format;
import static org.folio.services.util.parser.lexeme.Lexicon.BINARY_OPERATOR_EQUALS;
import static org.folio.services.util.parser.lexeme.Lexicon.BINARY_OPERATOR_FROM;
import static org.folio.services.util.parser.lexeme.Lexicon.BINARY_OPERATOR_IN;
import static org.folio.services.util.parser.lexeme.Lexicon.BINARY_OPERATOR_IS;
import static org.folio.services.util.parser.lexeme.Lexicon.BINARY_OPERATOR_LEFT_ANCHORED_EQUALS;
import static org.folio.services.util.parser.lexeme.Lexicon.BINARY_OPERATOR_NOT_EQUALS;
import static org.folio.services.util.parser.lexeme.Lexicon.BINARY_OPERATOR_TO;

public abstract class BinaryOperandLexeme implements BinaryOperand, Lexeme {
  private final static List<Lexicon> BINARY_OPERATORS = Arrays.asList(
    BINARY_OPERATOR_EQUALS,
    BINARY_OPERATOR_LEFT_ANCHORED_EQUALS,
    BINARY_OPERATOR_NOT_EQUALS,
    BINARY_OPERATOR_FROM,
    BINARY_OPERATOR_TO,
    BINARY_OPERATOR_IN,
    BINARY_OPERATOR_IS
  );
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
      if (LeaderBinaryOperand.matches(key)) {
        return new LeaderBinaryOperand(key, lexiconOperator, value);
      } else if (IndicatorBinaryOperand.matches(key)) {
        return new IndicatorBinaryOperand(key, lexiconOperator, value);
      } else if (SubFieldBinaryOperand.matches(key)) {
        return new SubFieldBinaryOperand(key, lexiconOperator, value);
      } else if (ValueBinaryOperand.matches(key)) {
        return new ValueBinaryOperand(key, lexiconOperator, value);
      } else if (PositionBinaryOperand.matches(key)) {
        return new PositionBinaryOperand(key, lexiconOperator, value);
      } else if (DateRangeBinaryOperand.matches(key)) {
        return new DateRangeBinaryOperand(key, lexiconOperator, value);
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
  public Optional<String> getField() {
    if (BINARY_OPERATOR_IS.equals(operator)) {
      return Optional.empty();
    }
    if (key.contains(".")) {
      return Optional.of(key.substring(0, key.indexOf(".")));
    } else {
      return Optional.of(key);
    }
  }

  @Override
  public List<String> getBindingParams() {
    if (BINARY_OPERATOR_IS.equals(operator)) {
      return Collections.emptyList();
    }
    if (BINARY_OPERATOR_LEFT_ANCHORED_EQUALS.equals(getOperator())) {
      return Collections.singletonList(this.value + "%");
    } else {
      return Collections.singletonList(this.value);
    }
  }

  public String getKey() {
    return this.key;
  }

  public Lexicon getOperator() {
    return this.operator;
  }
}
