package org.folio.services.util.parser.lexeme.operand;

import static java.lang.String.format;

/**
 * Available search cases:
 * 001.value is 'present'       - check field for presence
 * 999.value is 'absent'        - check field for absence
 * 010.a is 'present'           - check sub field for presence
 * 010.a is 'absent'            - check sub field for absence
 * 010.ind1 is 'present'        - check indicator for presence
 * 010.ind1 is 'absent'         - check indicator for absence
 */
public class PresenceBinaryOperand {

  private static final String PRESENT = "present";
  private static final String ABSENT = "absent";
  private static final String FIELD_NO = "(\"field_no\" = '";

  private PresenceBinaryOperand() {
  }

  public static String getSqlRepresentationForMarcField(String field, String value) {
    validateValue(value);
    if (PRESENT.equals(value)) {
      return FIELD_NO + field + "' and id in (select marc_id from marc_indexers))";
    } else {
      return FIELD_NO + field + "' and id not in (select marc_id from marc_indexers))";
    }
  }

  public static String getSqlRepresentationForSubField(String field, String subField, String value) {
    validateValue(value);
    if (PRESENT.equals(value)) {
      return FIELD_NO + field + "' and id in (select marc_id from marc_indexers where subfield_no = '" + subField + "'))";
    } else {
      return FIELD_NO + field + "' and id not in (select marc_id from marc_indexers where subfield_no = '" + subField + "'))";
    }
  }

  public static String getSqlRepresentationForIndicator(String field, String indicator, String value) {
    validateValue(value);
    if (PRESENT.equals(value)) {
      return FIELD_NO + field + "' and id in (select marc_id from marc_indexers where " + indicator + " <> '#'))";
    } else {
      return FIELD_NO + field + "' and id in (select marc_id from marc_indexers where " + indicator + " = '#'))";
    }
  }

  private static void validateValue(String value) {
    if (!PRESENT.equals(value) && !ABSENT.equals(value)) {
      throw new IllegalArgumentException(format("Value [%s] is not supported for the given Presence operand", value));
    }
  }
}
