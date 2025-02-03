package org.folio.dao.util;

import java.util.List;

public class CompositeMatchField {

  private List<MatchField> matchFields;

  public CompositeMatchField(List<MatchField> matchFields) {
    this.matchFields = matchFields;
  }

  public List<MatchField> getMatchFields() {
    return matchFields;
  }

  public boolean isDefaultField() {
    return matchFields.stream().allMatch(MatchField::isDefaultField);
  }

}
