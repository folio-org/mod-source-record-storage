package org.folio.services.util.parser;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * The result of parsing the incoming fieldsSearchExpression.
 *
 * @see SearchExpressionParser
 */
public class ParseFieldsResult {
  private final Set<String> fieldsToJoin = new HashSet<>();
  private final List<String> bindingParams = new ArrayList<>();
  private boolean isEnabled;
  private String whereExpression;

  public ParseFieldsResult enable() {
    this.isEnabled = true;
    return this;
  }

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

  public boolean isEnabled() {
    return isEnabled;
  }
}
