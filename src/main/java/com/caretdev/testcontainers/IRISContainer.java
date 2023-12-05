package com.caretdev.testcontainers;

import com.github.dockerjava.api.command.InspectContainerResponse;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Stream;

import static java.lang.String.format;

public class IRISContainer<SELF extends IRISContainer<SELF>>
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

    private String licenseKey = null;

    @Deprecated
    public IRISContainer() {
        this(DEFAULT_IMAGE_NAME.withTag(DEFAULT_TAG));
    }

    public IRISContainer(String dockerImageName) {
        this(DockerImageName.parse(dockerImageName));
    }

    public IRISContainer(final DockerImageName dockerImageName) {
        super(dockerImageName);

        DockerImageName[] allSupportedImages = Stream.of(new String[]{
                // InterSystemsDC imaages with ZPM
                "intersystemsdc/iris-community",
                "intersystemsdc/iris-ml-community",
                "intersystemsdc/irishealth-community",
                "intersystemsdc/irishealth-ml-community",

                // Vanilla Community Edition
                "containers.intersystems.com/intersystems/iris-community",
                "containers.intersystems.com/intersystemsdc/iris-ml-community",
                "containers.intersystems.com/intersystemsdc/irishealth-community",
                "containers.intersystems.com/intersystemsdc/irishealth-ml-community",

                // Vanilla Enterprise Edition
                "containers.intersystems.com/intersystems/iris",
                "containers.intersystems.com/intersystemsdc/iris-ml",
                "containers.intersystems.com/intersystemsdc/irishealth",
                "containers.intersystems.com/intersystemsdc/irishealth-ml"
        }).map(DockerImageName::new).toArray(DockerImageName[]::new);

        dockerImageName.assertCompatibleWith(
                allSupportedImages
        );

        addExposedPort(IRIS_PORT);
        addExposedPort(52773);
        setCommand("--ISCAgent", "false");
        if (dockerImageName.getRepository().startsWith("intersystemsdc")) {
            waitingFor(Wait.forLogMessage(".*executed command.*", 1));
        } else {
            waitingFor(Wait.forLogMessage(".*Enabling logons.*", 1));
        }
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
        if (this.licenseKey != null && new File(this.licenseKey).exists()) {
            addFileSystemBind(this.licenseKey, "/usr/irissys/mgr/iris.key", BindMode.READ_ONLY);
        }
    }

    @Override
    protected void waitUntilContainerStarted() {
        getWaitStrategy().waitUntilReady(this);
    }

    @NotNull
    @Override
    public Set<Integer> getLivenessCheckPortNumbers() {
        return Collections.singleton(getMappedPort(IRIS_PORT));
    }

    @Override
    protected void containerIsStarted(InspectContainerResponse containerInfo) {
        if (!this.getDockerImageName().startsWith("intersystemsdc")) {
            String createNamespaceCmd = format(
                    "##class(%%SQL.Statement).%%ExecDirect(,\"CREATE DATABASE %s\")", namespace);

            String createUserCmd = format(
                    "##class(Security.Users).Create(\"%s\",\"%s\",\"%s\",,\"%s\")",
                    username,
                    "%All",
                    password,
                    namespace);
            try {
                if (!namespace.equalsIgnoreCase("USER")) {
                    this.execInContainer("iris", "session", "iris", "-U", "%SYS", createNamespaceCmd);
                }
                this.execInContainer("iris", "session", "iris", "-U", "%SYS", createUserCmd);
            } catch (IOException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        super.containerIsStarted(containerInfo);
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

    public SELF withLicenseKey(String licenseKey) {
        this.licenseKey = licenseKey;
        return self();
    }
}
