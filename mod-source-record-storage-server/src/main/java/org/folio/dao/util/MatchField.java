package org.folio.dao.util;

import org.folio.processing.value.Value;
import org.folio.rest.jaxrs.model.Filter;
import org.marc4j.marc.impl.Verifier;

/**
 * The model of Marc field that needs to be matched
 */
public class MatchField {
  private static final String MATCHED_ID_MARC_FIELD = "999ffs";
  private static final String EXTERNAL_ID_MARC_FIELD = "999ffi";
  private static final String EXTERNAL_HRID_MARC_FIELD = "001";
  private final String tag;
  private final String ind1;
  private final String ind2;
  private final String subfield;
  private final Value value;
  private final QualifierMatch qualifierMatch;
  private final String fieldPath;
  private final Filter.ComparisonPartType comparisonPartType;

  public MatchField(String tag, String ind1, String ind2, String subfield, Value value) {
    this.tag = tag;
    this.ind1 = ind1;
    this.ind2 = ind2;
    this.subfield = subfield;
    this.value = value;
    this.fieldPath = tag + ind1 + ind2 + subfield;
    this.qualifierMatch = null;
    comparisonPartType = null;
  }

  public MatchField(String tag, String ind1, String ind2, String subfield, Value value, QualifierMatch qualifierMatch,
                    Filter.ComparisonPartType comparisonPartType) {
    this.tag = tag;
    this.ind1 = ind1;
    this.ind2 = ind2;
    this.subfield = subfield;
    this.value = value;
    this.qualifierMatch = qualifierMatch;
    this.fieldPath = tag + ind1 + ind2 + subfield;
    this.comparisonPartType = comparisonPartType;
  }

  public record QualifierMatch(Filter.Qualifier qualifier, String value) {
  }

  public String getTag() {
    return tag;
  }

  public String getInd1() {
    return ind1;
  }

  public String getInd2() {
    return ind2;
  }

  public String getSubfield() {
    return subfield;
  }

  public Value getValue() {
    return value;
  }

  public QualifierMatch getQualifierMatch() {
    return qualifierMatch;
  }

  public boolean isControlField() {
    return Verifier.isControlField(tag);
  }

  public boolean isDefaultField() {
    return isMatchedId() || isExternalId() || isExternalHrid();
  }

  public boolean isMatchedId() {
    return MATCHED_ID_MARC_FIELD.equals(fieldPath);
  }

  public boolean isExternalId() {
    return EXTERNAL_ID_MARC_FIELD.equals(fieldPath);
  }

  public boolean isExternalHrid() {
    return EXTERNAL_HRID_MARC_FIELD.equals(fieldPath);
  }

  public Filter.ComparisonPartType getComparisonPartType() {
    return comparisonPartType;
  }
}
