package eu.dissco.exportjob.repository;

import static org.testcontainers.containers.PostgreSQLContainer.IMAGE;

import com.zaxxer.hikari.HikariDataSource;
import org.flywaydb.core.Flyway;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DefaultDSLContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
class BaseRepositoryIT {

  private static final DockerImageName POSTGIS =
      DockerImageName.parse("postgres:15.2").asCompatibleSubstituteFor(IMAGE);

  @Container
  private static final PostgreSQLContainer<?> CONTAINER = new PostgreSQLContainer<>(POSTGIS);
  protected DSLContext context;
  protected HikariDataSource dataSource;

  @BeforeEach
  void prepareDatabase() {
    dataSource = new HikariDataSource();
    dataSource.setJdbcUrl(CONTAINER.getJdbcUrl());
    dataSource.setUsername(CONTAINER.getUsername());
    dataSource.setPassword(CONTAINER.getPassword());
    dataSource.setMaximumPoolSize(2);
    dataSource.setConnectionInitSql(CONTAINER.getTestQueryString());
    Flyway.configure().mixed(true).dataSource(dataSource).load().migrate();
    context = new DefaultDSLContext(dataSource, SQLDialect.POSTGRES);
  }

  @AfterEach
  void disposeDataSource() {
    dataSource.close();
  }
}
