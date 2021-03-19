package org.folio.services;

import org.folio.services.util.parser.ParseFieldsResult;
import org.folio.services.util.parser.ParseLeaderResult;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

import java.util.HashSet;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static org.folio.services.util.parser.SearchExpressionParser.parseFieldsSearchExpression;
import static org.folio.services.util.parser.SearchExpressionParser.parseLeaderSearchExpression;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

@RunWith(BlockJUnit4ClassRunner.class)
public class SearchExpressionParserUnitTest {

  /* - TESTING SearchExpressionParser#parseFieldsSearchExpression */

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
  public void shouldThrowException_if_fieldsSearchExpression_hasWrongQuotes() {
    // given
    String fieldsSearchExpression = "(035.a = '0') or (035.a = 1')";
    // when
    Exception exception = assertThrows(IllegalArgumentException.class, () -> {
      parseFieldsSearchExpression(fieldsSearchExpression);
    });
    // then
    String expectedMessage = "Each value in the expression should be surrounded by single quotes [expression: marcFieldSearchExpression]";
    String actualMessage = exception.getMessage();
    assertEquals(expectedMessage, actualMessage);
  }

  @Test
  public void shouldThrowException_if_fieldsSearchExpression_hasEmptyValue() {
    // given
    String fieldsSearchExpression = "(035.a = '')";
    // when
    Exception exception = assertThrows(IllegalArgumentException.class, () -> {
      parseFieldsSearchExpression(fieldsSearchExpression);
    });
    // then
    String expectedMessage = "Empty values are not allowed [expression: marcFieldSearchExpression]";
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
    assertFalse(result.isEnabled());
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
    String expectedMessage = "The given binary operator is not supported [key: 035.a, operator: none, value: 1]. Supported operators: [=, ^=]";
    String actualMessage = exception.getMessage();
    assertEquals(expectedMessage, actualMessage);
  }

  @Test
  public void shouldThrowException_if_fieldsSearchExpression_hasWrongOperand() {
    // given
    String fieldsSearchExpression = "xxx.a = '1'";
    // when
    Exception exception = assertThrows(IllegalArgumentException.class, () -> {
      parseFieldsSearchExpression(fieldsSearchExpression);
    });
    // then
    String expectedMessage = "The given expression [xxx.a = '1'] is not parsable";
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
    assertTrue(result.isEnabled());
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
    assertTrue(result.isEnabled());
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
    assertTrue(result.isEnabled());
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
    assertTrue(result.isEnabled());
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
    assertTrue(result.isEnabled());
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
    assertTrue(result.isEnabled());
    assertEquals(singletonList("20141107%"), result.getBindingParams());
    assertEquals(new HashSet<>(singletonList("005")), result.getFieldsToJoin());
    assertEquals("\"i005\".\"value\" like ?", result.getWhereExpression());
  }

  @Test
  public void shouldParseFieldsSearchExpression_with_boolean_operators() {
    // given
    String fieldsSearchExpression = "(035.a = '(OCoLC)63611770' and 036.ind1 = '1') or (036.ind1 ^= '1' and 005.value ^= '20141107')";
    // when
    ParseFieldsResult result = parseFieldsSearchExpression(fieldsSearchExpression);
    // then
    assertTrue(result.isEnabled());
    assertEquals(asList("(OCoLC)63611770", "1", "1%", "20141107%"), result.getBindingParams());
    assertEquals(new HashSet<>(asList("035", "036", "005")), result.getFieldsToJoin());
    assertEquals("((\"i035\".\"subfield_no\" = 'a' and \"i035\".\"value\" = ?) and \"i036\".\"ind1\" = ?) or (\"i036\".\"ind1\" like ? and \"i005\".\"value\" like ?)", result.getWhereExpression());
  }

  /* - TESTING SearchExpressionParser#parseLeaderSearchExpression */

  @Test
  public void shouldThrowException_if_leaderSearchExpression_isBlank() {
    // given
    String leaderSearchExpression = "     ";
    // when
    Exception exception = assertThrows(IllegalArgumentException.class, () -> {
      parseLeaderSearchExpression(leaderSearchExpression);
    });
    // then
    String expectedMessage = "The input expression should not be black or empty [expression: leaderSearchExpression]";
    String actualMessage = exception.getMessage();
    assertEquals(expectedMessage, actualMessage);
  }

  @Test
  public void shouldThrowException_if_leaderSearchExpression_isEmpty() {
    // given
    String leaderSearchExpression = "";
    // when
    Exception exception = assertThrows(IllegalArgumentException.class, () -> {
      parseLeaderSearchExpression(leaderSearchExpression);
    });
    // then
    String expectedMessage = "The input expression should not be black or empty [expression: leaderSearchExpression]";
    String actualMessage = exception.getMessage();
    assertEquals(expectedMessage, actualMessage);
  }

  @Test
  public void shouldThrowException_if_leaderSearchExpression_hasWrongBrackets() {
    // given
    String leaderSearchExpression = "(p_05 = 'a') and (p_06 = 'c'";
    // when
    Exception exception = assertThrows(IllegalArgumentException.class, () -> {
      parseLeaderSearchExpression(leaderSearchExpression);
    });
    // then
    String expectedMessage = "The number of opened brackets should be equal to number of closed brackets [expression: leaderSearchExpression]";
    String actualMessage = exception.getMessage();
    assertEquals(expectedMessage, actualMessage);
  }

  @Test
  public void shouldThrowException_if_leaderSearchExpression_hasWrongQuotes() {
    // given
    String leaderSearchExpression = "(p_05 = '0') or (p_06 = 1')";
    // when
    Exception exception = assertThrows(IllegalArgumentException.class, () -> {
      parseLeaderSearchExpression(leaderSearchExpression);
    });
    // then
    String expectedMessage = "Each value in the expression should be surrounded by single quotes [expression: leaderSearchExpression]";
    String actualMessage = exception.getMessage();
    assertEquals(expectedMessage, actualMessage);
  }

  @Test
  public void shouldThrowException_if_leaderSearchExpression_hasEmptyValue() {
    // given
    String leaderSearchExpression = "(p_05 = '')";
    // when
    Exception exception = assertThrows(IllegalArgumentException.class, () -> {
      parseLeaderSearchExpression(leaderSearchExpression);
    });
    // then
    String expectedMessage = "Empty values are not allowed [expression: leaderSearchExpression]";
    String actualMessage = exception.getMessage();
    assertEquals(expectedMessage, actualMessage);
  }

  @Test
  public void shouldReturnParseResult_if_leaderSearchExpression_isNull() {
    // given
    String leaderSearchExpression = null;
    // when
    ParseLeaderResult result = parseLeaderSearchExpression(leaderSearchExpression);
    // then
    assertFalse(result.isEnabled());
    assertEquals(emptyList(), result.getBindingParams());
    assertNull(result.getWhereExpression());
  }

  @Test
  public void shouldReturnParseResult_if_leaderSearchExpression_hasWrongOperator() {
    // given
    String leaderSearchExpression = "p_05 ^= 'a'";
    // when
    Exception exception = assertThrows(IllegalArgumentException.class, () -> {
      parseLeaderSearchExpression(leaderSearchExpression);
    });
    // then
    String expectedMessage = "Operator [^=] is not supported for the given Leader operand. Supported operators: [=]";
    String actualMessage = exception.getMessage();
    assertEquals(expectedMessage, actualMessage);
  }

  @Test
  public void shouldThrowException_if_leaderSearchExpression_hasWrongOperand() {
    // given
    String leaderSearchExpression = "xxx.a = '1'";
    // when
    Exception exception = assertThrows(IllegalArgumentException.class, () -> {
      parseLeaderSearchExpression(leaderSearchExpression);
    });
    // then
    String expectedMessage = "The given expression [xxx.a = '1'] is not parsable";
    String actualMessage = exception.getMessage();
    assertEquals(expectedMessage, actualMessage);
  }

  @Test
  public void shouldParseLeaderSearchExpression_for_EqualsOperator() {
    // given
    String leaderSearchExpression = "p_05 = 'a'";
    // when
    ParseLeaderResult result = parseLeaderSearchExpression(leaderSearchExpression);
    // then
    assertTrue(result.isEnabled());
    assertEquals(singletonList("a"), result.getBindingParams());
    assertEquals("p_05 = ?", result.getWhereExpression());
  }

  @Test
  public void shouldParseLeaderSearchExpression_with_boolean_operators() {
    // given
    String fieldsSearchExpression = "(p_05 = 'a' and p_06 = 'b') or (p_07 = '1' and p_08 = '2')";
    // when
    ParseLeaderResult result = parseLeaderSearchExpression(fieldsSearchExpression);
    // then
    assertTrue(result.isEnabled());
    assertEquals(asList("a", "b", "1", "2"), result.getBindingParams());
    assertEquals("(p_05 = ? and p_06 = ?) or (p_07 = ? and p_08 = ?)", result.getWhereExpression());
  }
}
