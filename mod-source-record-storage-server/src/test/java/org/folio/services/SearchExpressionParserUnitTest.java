package org.folio.services;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static org.folio.rest.jooq.Tables.RECORDS_LB;
import static org.folio.services.util.parser.SearchExpressionParser.parseFieldsSearchExpression;
import static org.folio.services.util.parser.SearchExpressionParser.parseLeaderSearchExpression;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.HashSet;
import java.util.stream.Stream;
import org.folio.services.util.parser.ParseFieldsResult;
import org.folio.services.util.parser.ParseLeaderResult;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class SearchExpressionParserUnitTest {

  /* - TESTING SearchExpressionParser#parseFieldsSearchExpression */

  private static Stream<Arguments> invalidFieldsSearchExpressionArguments() {
    return Stream.of(
      Arguments.of("     ", "The input expression should not be black or empty [expression: marcFieldSearchExpression]"),
      Arguments.of("", "The input expression should not be black or empty [expression: marcFieldSearchExpression]"),
      Arguments.of("(035.a = '0' or (035.a = '1')", "The number of opened brackets should be equal to number of closed brackets [expression: marcFieldSearchExpression]"),
      Arguments.of("(035.a = '0') or (035.a = 1')", "Each value in the expression should be surrounded by single quotes [expression: marcFieldSearchExpression]"),
      Arguments.of("(035.a = '')", "Empty values are not allowed [expression: marcFieldSearchExpression]"),
      Arguments.of("035.a none '1'", "The given binary operator is not supported [key: 035.a, operator: none, value: 1]. Supported operators: [=, ^=, not=, from, to, in, is]"),
      Arguments.of("xxx.a = '1'", "The given expression [xxx.a = '1'] is not parsable"),
      Arguments.of("001.08_01 = 'abc'", "The length of the value [abc] should be equal to the end position [expected length = 1]"),
      Arguments.of("001.08_01 ^= 'a'", "Operator [^=] is not supported for the given Position operand"),
      Arguments.of("005.date in 'wrong date'", "The given date [wrong date] is in a wrong format. Expected date pattern: [yyyymmdd]"),
      Arguments.of("005.date ^= '201701025'", "The given expression [005.date ^= '201701025'] is not supported"),
      Arguments.of("005.date in '201701025'", "The given expression [005.date in '201701025'] is not supported")
    );
  }

  @DisplayName("should throw IllegalArgumentException when fields search expression is invalid")
  @ParameterizedTest(name = "expression=[{0}]")
  @MethodSource("invalidFieldsSearchExpressionArguments")
  void shouldThrowException_if_fieldsSearchExpression_isInvalid(String fieldsSearchExpression, String expectedMessage) {
    // when
    Exception exception = assertThrows(IllegalArgumentException.class,
      () -> parseFieldsSearchExpression(fieldsSearchExpression));
    // then
    assertEquals(expectedMessage, exception.getMessage());
  }

  @Test
  void shouldReturnParseResult_if_fieldsSearchExpression_isNull() {
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
  void shouldParseFieldsSearchExpression_for_SubFieldOperand_EqualsOperator() {
    // given
    String fieldsSearchExpression = "035.a = '(OCoLC)63611770'";
    // when
    ParseFieldsResult result = parseFieldsSearchExpression(fieldsSearchExpression);
    // then
    assertTrue(result.isEnabled());
    assertEquals(singletonList("(OCoLC)63611770"), result.getBindingParams());
    assertEquals(new HashSet<>(singletonList("035")), result.getFieldsToJoin());
    assertEquals("( \"field_no\" = '035' and \"subfield_no\" = 'a' and \"value\" = ?)", result.getWhereExpression());
  }

  @Test
  void shouldParseFieldsSearchExpression_for_SubFieldOperand_LeftAnchoredEqualsOperator() {
    // given
    String fieldsSearchExpression = "035.a ^= '(OCoLC)'";
    // when
    ParseFieldsResult result = parseFieldsSearchExpression(fieldsSearchExpression);
    // then
    assertTrue(result.isEnabled());
    assertEquals(singletonList("(OCoLC)%"), result.getBindingParams());
    assertEquals(new HashSet<>(singletonList("035")), result.getFieldsToJoin());
    assertEquals("( \"field_no\" = '035' and \"subfield_no\" = 'a' and \"value\" like ?)", result.getWhereExpression());
  }

  @Test
  void shouldParseFieldsSearchExpression_for_SubFieldOperand_NotEqualsOperator() {
    // given
    String fieldsSearchExpression = "035.a not= '(OCoLC)'";
    // when
    ParseFieldsResult result = parseFieldsSearchExpression(fieldsSearchExpression);
    // then
    assertTrue(result.isEnabled());
    assertEquals(singletonList("(OCoLC)"), result.getBindingParams());
    assertEquals(new HashSet<>(singletonList("035")), result.getFieldsToJoin());
    assertEquals("( \"field_no\" = '035' and \"subfield_no\" = 'a' and \"value\" <> ?)", result.getWhereExpression());
  }

  private static Stream<Arguments> presenceFieldsSearchExpressionArguments() {
    return Stream.of(
      Arguments.of("035.a is 'present'", "( \"field_no\" = '035' and marc_indexers.marc_id in (select marc_id from marc_indexers_035 where subfield_no = 'a'))"),
      Arguments.of("035.z is 'absent'", "( \"field_no\" = '035' and marc_indexers.marc_id not in (select marc_id from marc_indexers_035 where subfield_no = 'z'))"),
      Arguments.of("035.value is 'present'", "( \"field_no\" = '035' and marc_indexers.marc_id in (select marc_id from marc_indexers_035))"),
      Arguments.of("035.value is 'absent'", "( \"field_no\" = '035' and marc_indexers.marc_id not in (select marc_id from marc_indexers_035))"),
      Arguments.of("050.ind1 is 'present'", "( \"field_no\" = '050' and marc_indexers.marc_id in (select marc_id from marc_indexers_050 where ind1 <> '#'))"),
      Arguments.of("050.ind2 is 'absent'", "( \"field_no\" = '050' and marc_indexers.marc_id in (select marc_id from marc_indexers_050 where ind2 = '#'))")
    );
  }

  @DisplayName("should parse fields search expression with presence (is present/absent) operator")
  @ParameterizedTest(name = "expression=[{0}]")
  @MethodSource("presenceFieldsSearchExpressionArguments")
  void shouldParseFieldsSearchExpression_for_PresenceOperator(String fieldsSearchExpression, String expectedWhereExpression) {
    // when
    ParseFieldsResult result = parseFieldsSearchExpression(fieldsSearchExpression);
    // then
    assertTrue(result.isEnabled());
    assertEquals(emptyList(), result.getBindingParams());
    assertEquals(emptySet(), result.getFieldsToJoin());
    assertEquals(expectedWhereExpression, result.getWhereExpression());
  }

  @Test
  void shouldParseFieldsSearchExpression_for_IndicatorOperand_EqualsOperator() {
    // given
    String fieldsSearchExpression = "036.ind1 = '1'";
    // when
    ParseFieldsResult result = parseFieldsSearchExpression(fieldsSearchExpression);
    // then
    assertTrue(result.isEnabled());
    assertEquals(singletonList("1"), result.getBindingParams());
    assertEquals(new HashSet<>(singletonList("036")), result.getFieldsToJoin());
    assertEquals("( \"field_no\" = '036' and \"ind1\" = ?)", result.getWhereExpression());
  }

  @Test
  void shouldParseFieldsSearchExpression_for_IndicatorOperand_LeftAnchoredEqualsOperator() {
    // given
    String fieldsSearchExpression = "036.ind1 ^= '1'";
    // when
    ParseFieldsResult result = parseFieldsSearchExpression(fieldsSearchExpression);
    // then
    assertTrue(result.isEnabled());
    assertEquals(singletonList("1%"), result.getBindingParams());
    assertEquals(new HashSet<>(singletonList("036")), result.getFieldsToJoin());
    assertEquals("( \"field_no\" = '036' and \"ind1\" like ?)", result.getWhereExpression());
  }

  @Test
  void shouldParseFieldsSearchExpression_for_IndicatorOperand_NotEqualsOperator() {
    // given
    String fieldsSearchExpression = "036.ind1 not= '1'";
    // when
    ParseFieldsResult result = parseFieldsSearchExpression(fieldsSearchExpression);
    // then
    assertTrue(result.isEnabled());
    assertEquals(singletonList("1"), result.getBindingParams());
    assertEquals(new HashSet<>(singletonList("036")), result.getFieldsToJoin());
    assertEquals("( \"field_no\" = '036' and \"ind1\" <> ?)", result.getWhereExpression());
  }

  @Test
  void shouldParseFieldsSearchExpression_for_ValueOperand_EqualsOperator() {
    // given
    String fieldsSearchExpression = "005.value = '20141107001016.0'";
    // when
    ParseFieldsResult result = parseFieldsSearchExpression(fieldsSearchExpression);
    // then
    assertTrue(result.isEnabled());
    assertEquals(singletonList("20141107001016.0"), result.getBindingParams());
    assertEquals(new HashSet<>(singletonList("005")), result.getFieldsToJoin());
    assertEquals("( \"field_no\" = '005' and \"value\" = ?)", result.getWhereExpression());
  }

  @Test
  void shouldParseFieldsSearchExpression_for_ValueOperand_LeftAnchoredEqualsOperator() {
    // given
    String fieldsSearchExpression = "005.value ^= '20141107'";
    // when
    ParseFieldsResult result = parseFieldsSearchExpression(fieldsSearchExpression);
    // then
    assertTrue(result.isEnabled());
    assertEquals(singletonList("20141107%"), result.getBindingParams());
    assertEquals(new HashSet<>(singletonList("005")), result.getFieldsToJoin());
    assertEquals("( \"field_no\" = '005' and \"value\" like ?)", result.getWhereExpression());
  }

  @Test
  void shouldParseFieldsSearchExpression_for_ValueOperand_NotEqualsOperator() {
    // given
    String fieldsSearchExpression = "005.value not= '20141107'";
    // when
    ParseFieldsResult result = parseFieldsSearchExpression(fieldsSearchExpression);
    // then
    assertTrue(result.isEnabled());
    assertEquals(singletonList("20141107"), result.getBindingParams());
    assertEquals(new HashSet<>(singletonList("005")), result.getFieldsToJoin());
    assertEquals("( \"field_no\" = '005' and \"value\" <> ?)", result.getWhereExpression());
  }

  @Test
  void shouldParseFieldsSearchExpression_for_PositionOperand_EqualsOperator() {
    // given
    String fieldsSearchExpression = "005.00_04 = '2014'";
    // when
    ParseFieldsResult result = parseFieldsSearchExpression(fieldsSearchExpression);
    // then
    assertTrue(result.isEnabled());
    assertEquals(singletonList("2014"), result.getBindingParams());
    assertEquals(new HashSet<>(singletonList("005")), result.getFieldsToJoin());
    assertEquals("( \"field_no\" = '005' and substring(\"value\", 1, 4) = ?)", result.getWhereExpression());
  }

  @Test
  void shouldParseFieldsSearchExpression_for_PositionOperand_NotEqualsOperator() {
    // given
    String fieldsSearchExpression = "005.00_04 not= '2014'";
    // when
    ParseFieldsResult result = parseFieldsSearchExpression(fieldsSearchExpression);
    // then
    assertTrue(result.isEnabled());
    assertEquals(singletonList("2014"), result.getBindingParams());
    assertEquals(new HashSet<>(singletonList("005")), result.getFieldsToJoin());
    assertEquals("( \"field_no\" = '005' and substring(\"value\", 1, 4) <> ?)", result.getWhereExpression());
  }

  @Test
  void shouldParseFieldsSearchExpression_forDateRangeOperand_EqualsOperator() {
    // given
    String fieldsSearchExpression = "005.date = '201701025'";
    // when
    ParseFieldsResult result = parseFieldsSearchExpression(fieldsSearchExpression);
    // then
    assertTrue(result.isEnabled());
    assertEquals(singletonList("201701025"), result.getBindingParams());
    assertEquals(new HashSet<>(singletonList("005")), result.getFieldsToJoin());
    assertEquals("( \"field_no\" = '005' and immutable_to_date(value) = ?)", result.getWhereExpression());
  }

  @Test
  void shouldParseFieldsSearchExpression_forDateRangeOperand_NotEqualsOperator() {
    // given
    String fieldsSearchExpression = "005.date not= '201701025'";
    // when
    ParseFieldsResult result = parseFieldsSearchExpression(fieldsSearchExpression);
    // then
    assertTrue(result.isEnabled());
    assertEquals(singletonList("201701025"), result.getBindingParams());
    assertEquals(new HashSet<>(singletonList("005")), result.getFieldsToJoin());
    assertEquals("( \"field_no\" = '005' and immutable_to_date(value) <> ?)", result.getWhereExpression());
  }

  @Test
  void shouldParseFieldsSearchExpression_forDateRangeOperand_FromOperator() {
    // given
    String fieldsSearchExpression = "005.date from '201701025'";
    // when
    ParseFieldsResult result = parseFieldsSearchExpression(fieldsSearchExpression);
    // then
    assertTrue(result.isEnabled());
    assertEquals(singletonList("201701025"), result.getBindingParams());
    assertEquals(new HashSet<>(singletonList("005")), result.getFieldsToJoin());
    assertEquals("( \"field_no\" = '005' and immutable_to_date(value) >= ?)", result.getWhereExpression());
  }

  @Test
  void shouldParseFieldsSearchExpression_forDateRangeOperand_ToOperator() {
    // given
    String fieldsSearchExpression = "005.date to '201701025'";
    // when
    ParseFieldsResult result = parseFieldsSearchExpression(fieldsSearchExpression);
    // then
    assertTrue(result.isEnabled());
    assertEquals(singletonList("201701025"), result.getBindingParams());
    assertEquals(new HashSet<>(singletonList("005")), result.getFieldsToJoin());
    assertEquals("( \"field_no\" = '005' and immutable_to_date(value) <= ?)", result.getWhereExpression());
  }

  @Test
  void shouldParseFieldsSearchExpression_forDateRangeOperand_InOperator() {
    // given
    String fieldsSearchExpression = "005.date in '201701025-20200213'";
    // when
    ParseFieldsResult result = parseFieldsSearchExpression(fieldsSearchExpression);
    // then
    assertTrue(result.isEnabled());
    assertEquals(Arrays.asList("201701025", "20200213"), result.getBindingParams());
    assertEquals(new HashSet<>(singletonList("005")), result.getFieldsToJoin());
    assertEquals("( \"field_no\" = '005' and immutable_to_date(value) between ? and ?)", result.getWhereExpression());
  }

  @Test
  void shouldParseFieldsSearchExpression_with_boolean_operators() {
    // given
    String fieldsSearchExpression = "(035.a = '(OCoLC)63611770' and 036.ind1 not= '1') or (036.ind1 ^= '1' and 005.value ^= '20141107') or (001.01_03 = 'abc' and 005.date in '20171128-20200114')";
    // when
    ParseFieldsResult result = parseFieldsSearchExpression(fieldsSearchExpression);
    // then
    assertTrue(result.isEnabled());
    assertEquals(asList("(OCoLC)63611770", "1", "1%", "20141107%", "abc", "20171128", "20200114"), result.getBindingParams());
    assertEquals(new HashSet<>(asList("001", "035", "036", "005")), result.getFieldsToJoin());
    assertEquals("(( \"field_no\" = '035' and \"subfield_no\" = 'a' and \"value\" = ?) and ( \"field_no\" = '036' and \"ind1\" <> ?)) or (( \"field_no\" = '036' and \"ind1\" like ?) and ( \"field_no\" = '005' and \"value\" like ?)) or (( \"field_no\" = '001' and substring(\"value\", 2, 3) = ?) and ( \"field_no\" = '005' and immutable_to_date(value) between ? and ?))", result.getWhereExpression());
  }

  @Test
  void shouldThrowException_if_fieldsSearchExpression_hasWrongOperatorForIndicatorOperand() {
    // given
    String fieldsSearchExpression = "050.ind2 is 'empty'";
    // when
    Exception exception = assertThrows(IllegalArgumentException.class, () -> {
      parseFieldsSearchExpression(fieldsSearchExpression);
    });
    // then
    String expectedMessage = "Value [empty] is not supported for the given Presence operand";
    assertEquals(expectedMessage, exception.getMessage());
  }

  /* - TESTING SearchExpressionParser#parseLeaderSearchExpression */

  private static Stream<Arguments> invalidLeaderSearchExpressionArguments() {
    return Stream.of(
      Arguments.of("     ", "The input expression should not be black or empty [expression: leaderSearchExpression]"),
      Arguments.of("", "The input expression should not be black or empty [expression: leaderSearchExpression]"),
      Arguments.of("(p_05 = 'a') and (p_06 = 'c'", "The number of opened brackets should be equal to number of closed brackets [expression: leaderSearchExpression]"),
      Arguments.of("(p_05 = '0') or (p_06 = 1')", "Each value in the expression should be surrounded by single quotes [expression: leaderSearchExpression]"),
      Arguments.of("(p_05 = '')", "Empty values are not allowed [expression: leaderSearchExpression]"),
      Arguments.of("p_05 ^= 'a'", "Operator [^=] is not supported for the given Leader operand. Supported operators: [=]"),
      Arguments.of("xxx.a = '1'", "The given expression [xxx.a = '1'] is not parsable")
    );
  }

  @DisplayName("should throw IllegalArgumentException when leader search expression is invalid")
  @ParameterizedTest(name = "expression=[{0}]")
  @MethodSource("invalidLeaderSearchExpressionArguments")
  void shouldThrowException_if_leaderSearchExpression_isInvalid(String leaderSearchExpression, String expectedMessage) {
    // when
    Exception exception = assertThrows(IllegalArgumentException.class,
      () -> parseLeaderSearchExpression(leaderSearchExpression));
    // then
    assertEquals(expectedMessage, exception.getMessage());
  }

  @Test
  void shouldReturnParseResult_if_leaderSearchExpression_isNull() {
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
  void shouldParseLeaderSearchExpression_for_EqualsOperator() {
    // given
    String leaderSearchExpression = "p_05 = 'a'";
    String expectedWhereExpression = String.format("%s = ?", RECORDS_LB.LEADER_RECORD_STATUS.getName());
    // when
    ParseLeaderResult result = parseLeaderSearchExpression(leaderSearchExpression);
    // then
    assertTrue(result.isEnabled());
    assertEquals(singletonList("a"), result.getBindingParams());
    assertEquals(expectedWhereExpression, result.getWhereExpression());
  }

  @Test
  void shouldParseLeaderSearchExpression_for_NotEqualsOperator() {
    // given
    String leaderSearchExpression = "p_06 not= 'd'";
    // when
    ParseLeaderResult result = parseLeaderSearchExpression(leaderSearchExpression);
    // then
    assertTrue(result.isEnabled());
    assertEquals(singletonList("d"), result.getBindingParams());
    assertEquals("p_06 <> ?", result.getWhereExpression());
  }

  @Test
  void shouldParseLeaderSearchExpression_with_boolean_operators() {
    // given
    String fieldsSearchExpression = "(p_05 = 'a' and p_06 = 'b') or (p_07 = '1' and p_08 not= '2')";
    String expectedWhereExpression = String.format("(%s = ? and p_06 = ?) or (p_07 = ? and p_08 <> ?)", RECORDS_LB.LEADER_RECORD_STATUS.getName());
    // when
    ParseLeaderResult result = parseLeaderSearchExpression(fieldsSearchExpression);
    // then
    assertTrue(result.isEnabled());
    assertEquals(asList("a", "b", "1", "2"), result.getBindingParams());
    assertEquals(expectedWhereExpression, result.getWhereExpression());
  }
}
