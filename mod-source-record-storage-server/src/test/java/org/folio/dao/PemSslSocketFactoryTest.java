package org.folio.dao;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.security.cert.CertificateException;
import java.util.Properties;
import org.folio.TestUtil;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.postgresql.PGProperty;

class PemSslSocketFactoryTest {

  private static final String SERVER_PEM = TestUtil.resourceToString("/tls/server-san.crt");
  private static final String WRONG_SERVER_PEM = TestUtil.resourceToString("/tls/server_wrong.crt");

  @DisplayName("should create a TLS socket factory when sslfactoryarg holds a PEM certificate")
  @Test
  void shouldCreateSocketFactory_whenArgumentHoldsPemCertificate() throws Exception {
    // arrange
    var info = propertiesWithArgument(SERVER_PEM);

    // act
    var factory = new PemSslSocketFactory(info);

    // assert
    assertThat(factory.getSupportedCipherSuites()).isNotEmpty();
  }

  @DisplayName("should create a TLS socket factory when sslfactoryarg holds several concatenated certificates")
  @Test
  void shouldCreateSocketFactory_whenArgumentHoldsSeveralCertificates() throws Exception {
    // arrange
    var info = propertiesWithArgument(SERVER_PEM + "\n" + WRONG_SERVER_PEM);

    // act
    var factory = new PemSslSocketFactory(info);

    // assert
    assertThat(factory.getSupportedCipherSuites()).isNotEmpty();
  }

  @DisplayName("should fail when sslfactoryarg is not set")
  @Test
  void shouldFail_whenArgumentIsMissing() {
    // arrange
    var info = new Properties();

    // act + assert
    assertThatThrownBy(() -> new PemSslSocketFactory(info))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessageContaining(PGProperty.SSL_FACTORY_ARG.getName());
  }

  @DisplayName("should fail when sslfactoryarg does not hold a PEM certificate")
  @Test
  void shouldFail_whenArgumentIsNotPemCertificate() {
    // arrange
    var info = propertiesWithArgument("not a certificate");

    // act + assert
    assertThatThrownBy(() -> new PemSslSocketFactory(info))
      .isInstanceOf(CertificateException.class);
  }

  private static Properties propertiesWithArgument(String pem) {
    var info = new Properties();
    PGProperty.SSL_FACTORY_ARG.set(info, pem);
    return info;
  }
}
