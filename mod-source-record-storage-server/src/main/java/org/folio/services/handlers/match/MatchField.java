package org.folio.services.handlers.match;

import org.marc4j.marc.impl.Verifier;

/**
 * The model of Marc field that needs to be matched
 */
public class MatchField {
  private final String tag;
  private final String ind1;
  private final String ind2;
  private final String subfield;
  private final String value;

  public MatchField(String tag, String ind1, String ind2, String subfield, String value) {
    this.tag = tag;
    this.ind1 = ind1;
    this.ind2 = ind2;
    this.subfield = subfield;
    this.value = value;
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

  public String getValue() {
    return value;
  }

  public boolean isControlField() {
    return Verifier.isControlField(tag);
  }
}
