package org.folio.dao;

import org.folio.postgres.testing.PostgresTesterContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.MountableFile;

public class PostgresSslContainer<SELF extends PostgresSslContainer<SELF>> extends PostgreSQLContainer<SELF> {
  public PostgresSslContainer() {
    super(PostgresTesterContainer.getImageName());
    withCopyFileToContainer(MountableFile.forClasspathResource("tls/server.key", 0444), "/server.key");
    withCopyFileToContainer(MountableFile.forClasspathResource("tls/server.crt", 0444), "/server.crt");
    withCopyFileToContainer(MountableFile.forClasspathResource("tls/init.sh", 0555), "/docker-entrypoint-initdb.d/init.sh");
    withExposedPorts(5432);
    withUsername("username");
    withPassword("password");
    withDatabaseName("postgres");
  }
}
