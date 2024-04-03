package org.folio.dao;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.KeyManagementException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;

public class PostgresSocketFactory extends SSLSocketFactory {

  private static final Logger LOG = LogManager.getLogger();
  private static final String CERT_FACTORY = "X.509";
  private static final String TRANSPORT_PROTOCOL = "TLSv1.3";
  private static final String CERTIFICATE_ALIAS = "CA";

  private final SSLSocketFactory sslSocketFactory;

  public PostgresSocketFactory() throws CertificateException, IOException, KeyStoreException, NoSuchAlgorithmException, KeyManagementException {
    sslSocketFactory = createSocketFactory();
  }

  private SSLSocketFactory createSocketFactory() throws CertificateException, IOException, KeyStoreException, NoSuchAlgorithmException, KeyManagementException {
    LOG.info("createSocketFactory:: Creating SSL SocketFactory");
    var cf = CertificateFactory.getInstance(CERT_FACTORY);

    try (InputStream certInputStream = new ByteArrayInputStream(System.getProperty(PostgresClientFactory.SERVER_PEM).getBytes())) {
      var ca = cf.generateCertificate(certInputStream);
      var keyStoreType = KeyStore.getDefaultType();
      var keyStore = KeyStore.getInstance(keyStoreType);
      keyStore.load(null, null);
      keyStore.setCertificateEntry(CERTIFICATE_ALIAS, ca);

      var tmfAlgorithm = TrustManagerFactory.getDefaultAlgorithm();
      var tmf = TrustManagerFactory.getInstance(tmfAlgorithm);
      tmf.init(keyStore);

      var context = SSLContext.getInstance(TRANSPORT_PROTOCOL);
      context.init(null, tmf.getTrustManagers(), new SecureRandom());
      return context.getSocketFactory();
    } catch (Exception exception) {
      LOG.error("createSocketFactory:: Error caused during creating SSL SocketFactory based on provided SSL certificate");
      throw exception;
    }
  }

  @Override
  public String[] getDefaultCipherSuites() {
    return sslSocketFactory.getDefaultCipherSuites();
  }

  @Override
  public String[] getSupportedCipherSuites() {
    return sslSocketFactory.getSupportedCipherSuites();
  }

  @Override
  public Socket createSocket(Socket s, String host, int port, boolean autoClose) throws IOException {
    return sslSocketFactory.createSocket(s, host, port, autoClose);
  }

  @Override
  public Socket createSocket(String host, int port) throws IOException {
    return sslSocketFactory.createSocket(host, port);
  }

  @Override
  public Socket createSocket(String host, int port, InetAddress localHost, int localPort) throws IOException {
    return sslSocketFactory.createSocket(host, port, localHost, localPort);
  }

  @Override
  public Socket createSocket(InetAddress host, int port) throws IOException {
    return sslSocketFactory.createSocket(host, port);
  }

  @Override
  public Socket createSocket(InetAddress address, int port, InetAddress localAddress, int localPort) throws IOException {
    return sslSocketFactory.createSocket(address, port, localAddress, localPort);
  }
}
