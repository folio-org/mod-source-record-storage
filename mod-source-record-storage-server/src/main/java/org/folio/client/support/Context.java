package org.folio.client.support;

public class Context {
  private final String tenantId;
  private final String token;
  private final String okapiLocation;
  private final String userId;

  public Context(String tenantId, String token, String okapiLocation, String userId) {
    this.tenantId = tenantId;
    this.token = token;
    this.okapiLocation = okapiLocation;
    this.userId = userId;
  }

  public String getTenantId() {
    return tenantId;
  }

  public String getToken() {
    return token;
  }

  public String getOkapiLocation() {
    return okapiLocation;
  }

  public String getUserId() {
    return userId;
  }
}
