package com.caretdev.testcontainers;

import static org.assertj.core.api.Assertions.assertThat;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import javax.sql.DataSource;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class IRISContainerTest {

    @Test
    public void testIRISWithNoSpecifiedVersion() throws SQLException {
        performSimpleTest("jdbc:tc:iris://hostname/databasename");
    }

    private void performSimpleTest(String jdbcUrl) throws SQLException {
        HikariDataSource dataSource = getDataSource(jdbcUrl, 1);
        new QueryRunner(dataSource)
            .query(
                "SELECT 1",
                new ResultSetHandler<Object>() {
                    @Override
                    public Object handle(ResultSet rs) throws SQLException {
                        rs.next();
                        int resultSetInt = rs.getInt(1);
                        assertThat(resultSetInt)
                            .as("A basic SELECT query succeeds")
                            .isEqualTo(1);
                        return true;
                    }
                }
            );
        dataSource.close();
    }

    protected ResultSet performQuery(IRISContainer container, String sql)
        throws SQLException {
        DataSource ds = getDataSource(container);
        Statement statement = ds.getConnection().createStatement();
        statement.execute(sql);
        ResultSet resultSet = statement.getResultSet();

        resultSet.next();
        return resultSet;
    }

    private HikariDataSource getDataSource(String jdbcUrl, int poolSize) {
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(jdbcUrl);
        hikariConfig.setConnectionTestQuery("SELECT 1");
        hikariConfig.setMinimumIdle(1);
        hikariConfig.setMaximumPoolSize(poolSize);

        return new HikariDataSource(hikariConfig);
    }

    protected DataSource getDataSource(IRISContainer container) {
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(container.getJdbcUrl());
        hikariConfig.setUsername(container.getUsername());
        hikariConfig.setPassword(container.getPassword());
        hikariConfig.setDriverClassName(container.getDriverClassName());
        return new HikariDataSource(hikariConfig);
    }

    @Test
    public void testWithCommunity() throws SQLException {
        try (
            IRISContainer container = new IRISContainer(
                "intersystemsdc/iris-community:latest-em-zpm"
            )
                .withUsername("test")
                .withPassword("test")
                .withDatabaseName("TEST")
                .withStartupTimeoutSeconds(15)
        ) {
            container.start();

            ResultSet resultSet = performQuery(container, "SELECT 1");

            int resultSetInt = resultSet.getInt(1);
            assertThat(resultSetInt).isEqualTo(1);

            ResultSet resultSet2 = performQuery(
                container,
                "SELECT $username, $namespace"
            );

            assertThat(resultSet2.getString(1)).isEqualTo(container.getUsername());
            assertThat(resultSet2.getString(2)).isEqualTo(container.getDatabaseName());
        }
    }

    @Test
    public void testWithVanillaCommunity() throws SQLException {
        try (
            IRISContainer container = new IRISContainer(
                "containers.intersystems.com/intersystems/iris-community:latest-em"
            )
                .withUsername("test")
                .withPassword("test")
                .withDatabaseName("TEST")
                .withStartupTimeoutSeconds(15)
        ) {
            container.start();
            // ResultSet resultSet = performQuery(container, "SELECT 1");

            // int resultSetInt = resultSet.getInt(1);
            // assertThat(resultSetInt).isEqualTo(1);
            ResultSet resultSet2 = performQuery(
                container,
                "SELECT $username, $namespace"
            );
            // assertThat(resultSet2.getString(1)).isEqualTo(container.getUsername());
            // assertThat(resultSet2.getString(2)).isEqualTo(container.getDatabaseName());
        }
    }

    @Disabled("Skip for now")
    @Test
    public void testWithEnterprise() throws SQLException {
        try (
            IRISContainer container = new IRISContainer(
                "containers.intersystems.com/intersystems/iris:latest-em"
            )
                .withUsername("test")
                .withPassword("test")
                .withDatabaseName("TEST")
                .withLicenseKey(System.getProperty("user.home") + "/iris.key")
                .withStartupTimeoutSeconds(15)
        ) {
            container.start();

            ResultSet resultSet = performQuery(container, "SELECT 1");

            int resultSetInt = resultSet.getInt(1);
            assertThat(resultSetInt).isEqualTo(1);

            ResultSet resultSet2 = performQuery(
                container,
                "SELECT $username, $namespace"
            );

            assertThat(resultSet2.getString(1)).isEqualTo(container.getUsername());
            assertThat(resultSet2.getString(2)).isEqualTo(container.getDatabaseName());
        }
    }
}
