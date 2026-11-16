package org.folio.dao;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.util.Properties;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import org.postgresql.PGProperty;
import org.postgresql.ssl.WrappedFactory;

/**
 * SSL socket factory for the PostgreSQL JDBC driver that trusts the certificates of a PEM value held
 * in memory.
 *
 * <p>The JDBC driver reads its trusted certificates from a file ({@code sslrootcert}), while the
 * module gets them as a value ({@code DB_SERVER_PEM}). Writing that value to a file would put it in a
 * shared, publicly writable temporary directory, so the driver is given this factory instead
 * ({@code sslfactory}) and the PEM value with it ({@code sslfactoryarg}).
 *
 * <p>The driver creates the factory by reflection with the connection properties. It still checks
 * the server's host name itself when {@code sslmode} is {@code verify-full}.
 */
public class PemSslSocketFactory extends WrappedFactory {

  private static final String TRANSPORT_PROTOCOL = "TLSv1.3";
  private static final String CERTIFICATE_TYPE = "X.509";
  private static final String CERTIFICATE_ALIAS_PREFIX = "server-";

  /**
   * Called by the JDBC driver.
   *
   * @param info connection properties; {@code sslfactoryarg} holds the trusted certificates in PEM
   *             format, one or more concatenated certificates
   * @throws GeneralSecurityException if the certificates cannot be parsed or the SSL context cannot be
   *                                  created
   */
  public PemSslSocketFactory(Properties info) throws GeneralSecurityException {
    var pem = PGProperty.SSL_FACTORY_ARG.getOrDefault(info);
    if (pem == null || pem.isBlank()) {
      throw new IllegalArgumentException(PGProperty.SSL_FACTORY_ARG.getName() + " must hold a PEM certificate");
    }
    var context = SSLContext.getInstance(TRANSPORT_PROTOCOL);
    context.init(null, createTrustManagerFactory(pem).getTrustManagers(), null);
    factory = context.getSocketFactory();
  }

  private static TrustManagerFactory createTrustManagerFactory(String pem) throws GeneralSecurityException {
    var trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
    try {
      trustStore.load(null, null);
    } catch (IOException e) {
      // not reachable: loading an empty key store reads no data
      throw new GeneralSecurityException("Failed to create an empty trust store", e);
    }
    var certificates = CertificateFactory.getInstance(CERTIFICATE_TYPE)
      .generateCertificates(new ByteArrayInputStream(pem.getBytes(StandardCharsets.US_ASCII)));
    if (certificates.isEmpty()) {
      throw new GeneralSecurityException("No certificate found in " + PGProperty.SSL_FACTORY_ARG.getName());
    }
    int index = 0;
    for (Certificate certificate : certificates) {
      trustStore.setCertificateEntry(CERTIFICATE_ALIAS_PREFIX + index++, certificate);
    }
    var trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    trustManagerFactory.init(trustStore);
    return trustManagerFactory;
  }
}
