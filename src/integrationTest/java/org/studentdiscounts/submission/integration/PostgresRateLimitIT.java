package org.studentdiscounts.submission.integration;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.studentdiscounts.submission.config.PostgresRateLimiter;
import org.studentdiscounts.submission.config.SubmissionProperties;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

@Testcontainers
class PostgresRateLimitIT {
  @Container
  static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:17-bookworm");

  @Test
  void atomicallyCountsConcurrentUuidBucketsAndExpiresOnlyOldWindows() throws Exception {
    var dataSource =
        new DriverManagerDataSource(
            POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
    var jdbc = new JdbcTemplate(dataSource);
    jdbc.execute(
        "CREATE TABLE api_rate_limits (actor_id uuid, window_start timestamptz, request_count int,"
            + " PRIMARY KEY(actor_id,window_start))");
    var now = Instant.parse("2026-11-05T12:34:56Z");
    var clock = Clock.fixed(now, ZoneOffset.UTC);
    var settings =
        new SubmissionProperties(
            null, new SubmissionProperties.Limits(262144, 120), null, null, null, null);
    var limiter = new PostgresRateLimiter(jdbc, clock, settings);
    UUID actor = UUID.randomUUID();
    jdbc.update(
        "INSERT INTO api_rate_limits VALUES (?,?::timestamptz,1)", actor, "2026-11-05T12:30:00Z");
    jdbc.update(
        "INSERT INTO api_rate_limits VALUES (?,?::timestamptz,1)", actor, "2026-11-05T12:33:00Z");
    var decisions = new ArrayList<java.util.concurrent.Future<PostgresRateLimiter.Decision>>();
    try (var pool = Executors.newFixedThreadPool(10)) {
      for (int i = 0; i < 125; i++) decisions.add(pool.submit(() -> limiter.consume(actor)));
    }
    long accepted = 0;
    for (var decision : decisions) if (decision.get().allowed()) accepted++;
    assertThat(accepted).isEqualTo(120);
    assertThat(limiter.consume(actor).retryAfterSeconds()).isEqualTo(4);
    assertThat(limiter.consume(UUID.randomUUID()).allowed()).isTrue();
    assertThat(
            jdbc.queryForObject(
                "SELECT count(*) FROM api_rate_limits WHERE window_start < '2026-11-05T12:32:00Z'",
                Integer.class))
        .isZero();
    assertThat(
            jdbc.queryForObject(
                "SELECT count(*) FROM api_rate_limits WHERE window_start = '2026-11-05T12:33:00Z'",
                Integer.class))
        .isEqualTo(1);
    assertThat(
            new PostgresRateLimiter(jdbc, Clock.fixed(now.plusSeconds(4), ZoneOffset.UTC), settings)
                .consume(actor)
                .allowed())
        .isTrue();
  }
}
