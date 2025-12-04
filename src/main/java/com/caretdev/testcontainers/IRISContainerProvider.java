package com.caretdev.testcontainers;

import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.JdbcDatabaseContainerProvider;
import org.testcontainers.utility.DockerImageName;

public class IRISContainerProvider extends JdbcDatabaseContainerProvider {

    @Override
    public boolean supports(String databaseType) {
        return databaseType.equals(IRISContainer.NAME);
    }

    @Override
    public JdbcDatabaseContainer<IRISContainer> newInstance() {
        return newInstance(IRISContainer.DEFAULT_TAG);
    }

    @Override
    public JdbcDatabaseContainer<IRISContainer> newInstance(String tag) {
        if (tag != null) {
            return new IRISContainer(
                DockerImageName.parse(IRISContainer.IMAGE).withTag(tag)
            );
        }
        return newInstance();
    }
}
