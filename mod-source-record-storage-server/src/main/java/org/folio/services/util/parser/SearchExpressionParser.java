package org.folio.services.util.parser;

import org.folio.services.util.parser.lexeme.Lexeme;
import org.folio.services.util.parser.lexeme.LexemeType;
import org.folio.services.util.parser.lexeme.bracket.BracketLexeme;
import org.folio.services.util.parser.lexeme.operand.BinaryOperand;
import org.folio.services.util.parser.lexeme.operand.BinaryOperandLexeme;
import org.folio.services.util.parser.lexeme.operator.OperatorLexeme;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.commons.lang3.StringUtils.SPACE;
import static org.folio.services.util.parser.lexeme.Lexicon.CLOSED_BRACKET;
import static org.folio.services.util.parser.lexeme.Lexicon.OPENED_BRACKET;
import static org.folio.services.util.parser.lexeme.Lexicon.OPERATOR_AND;
import static org.folio.services.util.parser.lexeme.Lexicon.OPERATOR_OR;

public class SearchExpressionParser {

  public static ParseFieldsResult parseFieldsSearchExpression(String expression) {
    ParseFieldsResult parseFieldsResult = new ParseFieldsResult();
    if (expression != null) {
      ExpressionValidator.validate(expression, "marcFieldSearchExpression");
      List<Lexeme> lexemes = getLexemes(expression);
      parseFieldsResult.withFieldsToJoin(getFieldsToJoin(lexemes));
      parseFieldsResult.withWhereExpression(getWhereExpression(lexemes));
      parseFieldsResult.withBindingParams(getBindingParams(lexemes));
    }
    return parseFieldsResult;
  }

  private static List<Lexeme> getLexemes(String expression) {
    List<String> expressionParts = Arrays.asList(expression.split(SPACE));
    List<Lexeme> lexemes = new ArrayList<>();
    for (int lexemeIndex = 0; lexemeIndex < expressionParts.size(); lexemeIndex++) {
      String currentExpressionPart = expressionParts.get(lexemeIndex);
      lexemeIndex = processExpressionPart(expressionParts, lexemes, lexemeIndex, currentExpressionPart);
    }
    return lexemes;
  }

  private static int processExpressionPart(List<String> expressionParts, List<Lexeme> lexemes, int i, String currentExpressionPart) {
    if (Character.isDigit(currentExpressionPart.charAt(0))) {
      String leftOperand = currentExpressionPart;
      String operator = expressionParts.get(++i);
      String rightOperand = expressionParts.get(++i);
      if (rightOperand.endsWith("'" + CLOSED_BRACKET.getSearchExpressionRepresentation())) {
        String[] rightOperandParts = rightOperand.split("'");
        rightOperand = rightOperandParts[1];
        int numberOfEndBrackets = rightOperandParts[2].length();
        lexemes.add(BinaryOperandLexeme.of(leftOperand, operator, rightOperand));
        for (int index = 0; index < numberOfEndBrackets; index++) {
          lexemes.add(BracketLexeme.closed());
        }
      } else {
        rightOperand = rightOperand.substring(1, rightOperand.length() - 1);
        lexemes.add(BinaryOperandLexeme.of(leftOperand, operator, rightOperand));
      }
    } else if (currentExpressionPart.startsWith(OPENED_BRACKET.getSearchExpressionRepresentation())) {
      lexemes.add(BracketLexeme.opened());
      currentExpressionPart = currentExpressionPart.substring(1);
      i = processExpressionPart(expressionParts, lexemes, i, currentExpressionPart);
    } else if (currentExpressionPart.equals(OPERATOR_AND.getSearchExpressionRepresentation())) {
      lexemes.add(OperatorLexeme.of(OPERATOR_AND));
    } else if (currentExpressionPart.equals(OPERATOR_OR.getSearchExpressionRepresentation())) {
      lexemes.add(OperatorLexeme.of(OPERATOR_OR));
    } else {
      throw new IllegalArgumentException("xxx");
    }
    return i;
  }

  private static Set<String> getFieldsToJoin(List<Lexeme> lexemes) {
    Set<String> fieldsToJoin = new HashSet<>();
    lexemes.forEach(lexeme -> {
      if (LexemeType.BINARY_OPERAND.equals(lexeme.getType())) {
        BinaryOperand binaryOperandLexeme = (BinaryOperand) lexeme;
        fieldsToJoin.add(binaryOperandLexeme.getMarcField());
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
