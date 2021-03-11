package org.folio.services.util.parser;

import java.util.ArrayList;
import java.util.List;

/**
 * The result of parsing the incoming leaderSearchExpression.
 *
 * @see SearchExpressionParser
 */
public class ParseLeaderResult {
  private final List<String> bindingParams = new ArrayList<>();
  private boolean isEnabled = false;
  private String whereExpression;

  public ParseLeaderResult enable() {
    this.isEnabled = true;
    return this;
  }

  public ParseLeaderResult withWhereExpression(String whereExpression) {
    this.whereExpression = whereExpression;
    return this;
  }

  public ParseLeaderResult withBindingParams(List<String> bindingParams) {
    this.bindingParams.addAll(bindingParams);
    return this;
  }

  public boolean isEnabled() {
    return isEnabled;
  }

  public List<String> getBindingParams() {
    return bindingParams;
  }

  public String getWhereExpression() {
    return whereExpression;
  }
}
