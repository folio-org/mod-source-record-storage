package org.folio.services.util.parser;

import org.folio.services.util.parser.lexeme.Lexeme;
import org.folio.services.util.parser.lexeme.LexemeType;
import org.folio.services.util.parser.lexeme.Lexicon;
import org.folio.services.util.parser.lexeme.bracket.BracketLexeme;
import org.folio.services.util.parser.lexeme.operand.BinaryOperand;
import org.folio.services.util.parser.lexeme.operand.BinaryOperandLexeme;
import org.folio.services.util.parser.lexeme.operator.BooleanOperatorLexeme;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.commons.lang3.StringUtils.SPACE;
import static org.folio.services.util.parser.lexeme.Lexicon.BOOLEAN_OPERATOR_AND;
import static org.folio.services.util.parser.lexeme.Lexicon.BOOLEAN_OPERATOR_OR;
import static org.folio.services.util.parser.lexeme.Lexicon.CLOSED_BRACKET;
import static org.folio.services.util.parser.lexeme.Lexicon.LEADER_FIELD;
import static org.folio.services.util.parser.lexeme.Lexicon.MARC_FIELD;
import static org.folio.services.util.parser.lexeme.Lexicon.OPENED_BRACKET;

/**
 * The parser is intended to parse the incoming search expressions for leader and marc fields
 *
 * @see ExpressionValidator
 * @see Lexeme
 * @see Lexicon
 */
public class SearchExpressionParser {

  public static ParseLeaderResult parseLeaderSearchExpression(String expression) {
    ParseLeaderResult parseLeaderResult = new ParseLeaderResult();
    if (expression != null) {
      ExpressionValidator.validate(expression, "leaderSearchExpression");
      List<Lexeme> lexemes = getLexemes(expression);
      parseLeaderResult.enable();
      parseLeaderResult.withWhereExpression(getWhereExpression(lexemes));
      parseLeaderResult.withBindingParams(getBindingParams(lexemes));
    }
    return parseLeaderResult;
  }

  public static ParseFieldsResult parseFieldsSearchExpression(String expression) {
    ParseFieldsResult parseFieldsResult = new ParseFieldsResult();
    if (expression != null) {
      ExpressionValidator.validate(expression, "marcFieldSearchExpression");
      List<Lexeme> lexemes = getLexemes(expression);
      parseFieldsResult.enable();
      parseFieldsResult.withFieldsToJoin(getFieldsToJoin(lexemes));
      parseFieldsResult.withWhereExpression(getWhereExpression(lexemes));
      parseFieldsResult.withBindingParams(getBindingParams(lexemes));
    }
    return parseFieldsResult;
  }

  private static List<Lexeme> getLexemes(String expression) {
    List<Lexeme> lexemes = new ArrayList<>();
    while (!expression.isEmpty()) {
      expression = processExpression(expression, lexemes);
    }
    return lexemes;
  }

  private static String processExpression(String expression, List<Lexeme> lexemes) {
    if (expression.matches(MARC_FIELD.getSearchValue()) || expression.matches(LEADER_FIELD.getSearchValue())) {
      String splitResult[] = expression.split(SPACE, 3);
      String leftOperand = splitResult[0];
      String operator = splitResult[1];
      String rightSuffix[] = splitResult[2].split("'*'", 3);
      String rightOperand = rightSuffix[1];
      lexemes.add(BinaryOperandLexeme.of(leftOperand, operator, rightOperand));
      return rightSuffix[2];
    } else if (expression.startsWith(OPENED_BRACKET.getSearchValue())) {
      lexemes.add(BracketLexeme.opened());
      return expression.substring(1);
    } else if (expression.startsWith(CLOSED_BRACKET.getSearchValue())) {
      lexemes.add(BracketLexeme.closed());
      return expression.substring(1);
    } else if (expression.startsWith(BOOLEAN_OPERATOR_AND.getSearchValue())) {
      lexemes.add(BooleanOperatorLexeme.of(BOOLEAN_OPERATOR_AND));
      return expression.substring(BOOLEAN_OPERATOR_AND.getSearchValue().length());
    } else if (expression.startsWith(BOOLEAN_OPERATOR_OR.getSearchValue())) {
      lexemes.add(BooleanOperatorLexeme.of(BOOLEAN_OPERATOR_OR));
      return expression.substring(BOOLEAN_OPERATOR_OR.getSearchValue().length());
    } else if (expression.startsWith(SPACE)) {
      return expression.substring(1);
    } else {
      throw new IllegalArgumentException(String.format("The given expression [%s] is not parsable", expression));
    }
  }

  private static Set<String> getFieldsToJoin(List<Lexeme> lexemes) {
    Set<String> fieldsToJoin = new HashSet<>();
    lexemes.forEach(lexeme -> {
      if (LexemeType.BINARY_OPERAND.equals(lexeme.getType())) {
        BinaryOperand binaryOperandLexeme = (BinaryOperand) lexeme;
        fieldsToJoin.add(binaryOperandLexeme.getField());
      }
    });
    return fieldsToJoin;
  }

  private static String getWhereExpression(List<Lexeme> lexemes) {
    StringBuilder stringBuilder = new StringBuilder();
    lexemes.forEach(lexeme -> stringBuilder.append(lexeme.toSqlRepresentation()));
    return stringBuilder.toString();
  }

  private static List<String> getBindingParams(List<Lexeme> lexemes) {
    List<String> bindingParams = new ArrayList<>();
    for (Lexeme lexeme : lexemes) {
      if (LexemeType.BINARY_OPERAND.equals(lexeme.getType())) {
        bindingParams.add(((BinaryOperandLexeme) lexeme).getBindingParam());
      }
    }
    return bindingParams;
  }
}
