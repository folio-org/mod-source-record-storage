package org.folio.services.util.parser;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ParseFieldsResult {
  private final Set<String> fieldsToJoin = new HashSet<>();
  private String whereExpression;
  private final List<String> bindingParams = new ArrayList<>();

  public Set<String> getFieldsToJoin() {
    return fieldsToJoin;
  }

  public String getWhereExpression() {
    return whereExpression;
  }

  public List<String> getBindingParams() {
    return bindingParams;
  }

  public ParseFieldsResult withFieldsToJoin(Set<String> fieldsToJoin) {
    this.fieldsToJoin.addAll(fieldsToJoin);
    return this;
  }

  public ParseFieldsResult withWhereExpression(String whereExpression) {
    this.whereExpression = whereExpression;
    return this;
  }

  public ParseFieldsResult withBindingParams(List<String> bindingParams) {
    this.bindingParams.addAll(bindingParams);
    return this;
  }
}
