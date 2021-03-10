package org.folio.services;

import org.folio.services.util.parser.ParseFieldsResult;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

import java.util.HashSet;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static org.folio.services.util.parser.SearchExpressionParser.parseFieldsSearchExpression;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

@RunWith(BlockJUnit4ClassRunner.class)
public class SearchExpressionParserUnitTest {

  @Test
  public void shouldThrowException_if_fieldsSearchExpression_isBlank() {
    // given
    String fieldsSearchExpression = "     ";
    // when
    Exception exception = assertThrows(IllegalArgumentException.class, () -> {
      parseFieldsSearchExpression(fieldsSearchExpression);
    });
    // then
    String expectedMessage = "The input expression should not be black or empty [expression: marcFieldSearchExpression]";
    String actualMessage = exception.getMessage();
    assertEquals(expectedMessage, actualMessage);
  }

  @Test
  public void shouldThrowException_if_fieldsSearchExpression_isEmpty() {
    // given
    String fieldsSearchExpression = "";
    // when
    Exception exception = assertThrows(IllegalArgumentException.class, () -> {
      parseFieldsSearchExpression(fieldsSearchExpression);
    });
    // then
    String expectedMessage = "The input expression should not be black or empty [expression: marcFieldSearchExpression]";
    String actualMessage = exception.getMessage();
    assertEquals(expectedMessage, actualMessage);
  }

  @Test
  public void shouldThrowException_if_fieldsSearchExpression_hasWrongBrackets() {
    // given
    String fieldsSearchExpression = "(035.a = '0' or (035.a = '1')";
    // when
    Exception exception = assertThrows(IllegalArgumentException.class, () -> {
      parseFieldsSearchExpression(fieldsSearchExpression);
    });
    // then
    String expectedMessage = "The number of opened brackets should be equal to number of closed brackets [expression: marcFieldSearchExpression]";
    String actualMessage = exception.getMessage();
    assertEquals(expectedMessage, actualMessage);
  }

  @Test
  public void shouldThrowException_if_fieldsSearchExpression_hasWrongApostrophes() {
    // given
    String fieldsSearchExpression = "(035.a = '0') or (035.a = 1')";
    // when
    Exception exception = assertThrows(IllegalArgumentException.class, () -> {
      parseFieldsSearchExpression(fieldsSearchExpression);
    });
    // then
    String expectedMessage = "The number of apostrophes should be multiple of two [expression: marcFieldSearchExpression]";
    String actualMessage = exception.getMessage();
    assertEquals(expectedMessage, actualMessage);
  }

  @Test
  public void shouldReturnParseResult_if_fieldsSearchExpression_isNull() {
    // given
    String fieldsSearchExpression = null;
    // when
    ParseFieldsResult result = parseFieldsSearchExpression(fieldsSearchExpression);
    // then
    assertEquals(emptyList(), result.getBindingParams());
    assertEquals(emptySet(), result.getFieldsToJoin());
    assertNull(result.getWhereExpression());
  }

  @Test
  public void shouldReturnParseResult_if_fieldsSearchExpression_hasWrongOperator() {
    // given
    String fieldsSearchExpression = "035.a none '1'";
    // when
    Exception exception = assertThrows(IllegalArgumentException.class, () -> {
      parseFieldsSearchExpression(fieldsSearchExpression);
    });
    // then
    String expectedMessage = "The given operator is not supported. key: 035.a, operator: none, value: 1";
    String actualMessage = exception.getMessage();
    assertEquals(expectedMessage, actualMessage);
  }

  @Test
  public void shouldParseFieldsSearchExpression_for_SubFieldOperand_EqualsOperator() {
    // given
    String fieldsSearchExpression = "035.a = '(OCoLC)63611770'";
    // when
    ParseFieldsResult result = parseFieldsSearchExpression(fieldsSearchExpression);
    // then
    assertEquals(singletonList("(OCoLC)63611770"), result.getBindingParams());
    assertEquals(new HashSet<>(singletonList("035")), result.getFieldsToJoin());
    assertEquals("(\"i035\".\"subfield_no\" = 'a' and \"i035\".\"value\" = ?)", result.getWhereExpression());
  }

  @Test
  public void shouldParseFieldsSearchExpression_for_IndicatorOperand_EqualsOperator() {
    // given
    String fieldsSearchExpression = "036.ind1 = '1'";
    // when
    ParseFieldsResult result = parseFieldsSearchExpression(fieldsSearchExpression);
    // then
    assertEquals(singletonList("1"), result.getBindingParams());
    assertEquals(new HashSet<>(singletonList("036")), result.getFieldsToJoin());
    assertEquals("\"i036\".\"ind1\" = ?", result.getWhereExpression());
  }

  @Test
  public void shouldParseFieldsSearchExpression_for_ValueOperand_EqualsOperator() {
    // given
    String fieldsSearchExpression = "005.value = '20141107001016.0'";
    // when
    ParseFieldsResult result = parseFieldsSearchExpression(fieldsSearchExpression);
    // then
    assertEquals(singletonList("20141107001016.0"), result.getBindingParams());
    assertEquals(new HashSet<>(singletonList("005")), result.getFieldsToJoin());
    assertEquals("\"i005\".\"value\" = ?", result.getWhereExpression());
  }

  @Test
  public void shouldParseFieldsSearchExpression_for_SubFieldOperand_LeftAnchoredEqualsOperator() {
    // given
    String fieldsSearchExpression = "035.a ^= '(OCoLC)'";
    // when
    ParseFieldsResult result = parseFieldsSearchExpression(fieldsSearchExpression);
    // then
    assertEquals(singletonList("(OCoLC)%"), result.getBindingParams());
    assertEquals(new HashSet<>(singletonList("035")), result.getFieldsToJoin());
    assertEquals("(\"i035\".\"subfield_no\" = 'a' and \"i035\".\"value\" like ?)", result.getWhereExpression());
  }

  @Test
  public void shouldParseFieldsSearchExpression_for_IndicatorOperand_LeftAnchoredEqualsOperator() {
    // given
    String fieldsSearchExpression = "036.ind1 ^= '1'";
    // when
    ParseFieldsResult result = parseFieldsSearchExpression(fieldsSearchExpression);
    // then
    assertEquals(singletonList("1%"), result.getBindingParams());
    assertEquals(new HashSet<>(singletonList("036")), result.getFieldsToJoin());
    assertEquals("\"i036\".\"ind1\" like ?", result.getWhereExpression());
  }

  @Test
  public void shouldParseFieldsSearchExpression_for_ValueOperand_LeftAnchoredEqualsOperator() {
    // given
    String fieldsSearchExpression = "005.value ^= '20141107'";
    // when
    ParseFieldsResult result = parseFieldsSearchExpression(fieldsSearchExpression);
    // then
    assertEquals(singletonList("20141107%"), result.getBindingParams());
    assertEquals(new HashSet<>(singletonList("005")), result.getFieldsToJoin());
    assertEquals("\"i005\".\"value\" like ?", result.getWhereExpression());
  }

  @Test
  public void shouldParseFieldsSearchExpression_with_AND_OR_operators() {
    // given
    String fieldsSearchExpression = "(035.a = '(OCoLC)63611770' and 036.ind1 = '1') or (036.ind1 ^= '1' and 005.value ^= '20141107')";
    // when
    ParseFieldsResult result = parseFieldsSearchExpression(fieldsSearchExpression);
    // then
    assertEquals(asList("(OCoLC)63611770", "1", "1%", "20141107%"), result.getBindingParams());
    assertEquals(new HashSet<>(asList("035", "036", "005")), result.getFieldsToJoin());
    assertEquals("((\"i035\".\"subfield_no\" = 'a' and \"i035\".\"value\" = ?) and \"i036\".\"ind1\" = ?) or (\"i036\".\"ind1\" like ? and \"i005\".\"value\" like ?)", result.getWhereExpression());
  }
}
