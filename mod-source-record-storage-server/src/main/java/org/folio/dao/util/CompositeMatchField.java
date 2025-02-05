package org.folio.dao.util;

import org.folio.rest.jaxrs.model.RecordMatchingDto;

import java.util.List;

public class CompositeMatchField {

  private final List<MatchField> matchFields;
  private final RecordMatchingDto.LogicalOperator logicalOperator;

  public CompositeMatchField(List<MatchField> matchFields, RecordMatchingDto.LogicalOperator logicalOperator) {
    this.matchFields = matchFields;
    this.logicalOperator = logicalOperator;
  }

  public List<MatchField> getMatchFields() {
    return matchFields;
  }

  public RecordMatchingDto.LogicalOperator getLogicalOperator() {
    return logicalOperator;
  }

  public boolean isDefaultField() {
    return matchFields.stream().allMatch(MatchField::isDefaultField);
  }

}
