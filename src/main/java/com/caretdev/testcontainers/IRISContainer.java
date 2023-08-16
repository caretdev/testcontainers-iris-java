package com.caretdev.testcontainers;

import org.apache.commons.lang3.StringUtils;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.utility.DockerImageName;

class IRISContainer<SELF extends IRISContainer<SELF>>
  extends JdbcDatabaseContainer<SELF> {

  public static final String NAME = "iris";

  private static final DockerImageName DEFAULT_IMAGE_NAME = DockerImageName.parse(
    "intersystemsdc/iris-community"
  );

  public static final String DEFAULT_TAG = "latest";

  static final String IMAGE = DEFAULT_IMAGE_NAME.getUnversionedPart();

  static final Integer IRIS_PORT = 1972;

  static final String DEFAULT_USER = "test";

  static final String DEFAULT_PASSWORD = "test";

  private String namespace = "USER";

  private String username = DEFAULT_USER;

  private String password = DEFAULT_PASSWORD;

  @Deprecated
  public IRISContainer() {
    this(DEFAULT_IMAGE_NAME.withTag(DEFAULT_TAG));
  }

  public IRISContainer(String dockerImageName) {
    this(DockerImageName.parse(dockerImageName));
  }

  public IRISContainer(final DockerImageName dockerImageName) {
    super(dockerImageName);
    dockerImageName.assertCompatibleWith(DEFAULT_IMAGE_NAME);

    addExposedPort(IRIS_PORT);
  }

  @Override
  public String getDriverClassName() {
    return "com.intersystems.jdbc.IRISDriver";
  }

  @Override
  public String getJdbcUrl() {
    String additionalUrlParams = constructUrlParameters("?", "&");
    return (
      "jdbc:IRIS://" +
      getHost() +
      ":" +
      getMappedPort(IRIS_PORT) +
      "/" +
      namespace +
      additionalUrlParams
    );
  }

  @Override
  public String getUsername() {
    return username;
  }

  @Override
  public String getPassword() {
    return password;
  }

  @Override
  public String getTestQueryString() {
    return "SELECT 1";
  }

  @Override
  protected void configure() {
    addEnv("IRIS_NAMESPACE", namespace);
    addEnv("IRIS_USERNAME", username);
    addEnv("IRIS_PASSWORD", password);
  }

  @Override
  public String getDatabaseName() {
    return namespace;
  }

  @Override
  public SELF withDatabaseName(String dbName) {
    this.namespace = dbName;
    return self();
  }

  @Override
  public SELF withUsername(String username) {
    this.username = username;
    return self();
  }

  @Override
  public SELF withPassword(String password) {
    if (StringUtils.isEmpty(password)) {
      throw new IllegalArgumentException("Password cannot be null or empty");
    }
    this.password = password;
    return self();
  }
}
