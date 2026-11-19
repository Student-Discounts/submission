package org.studentdiscounts.submission.config;

import java.sql.Timestamp;
import java.time.Clock;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Component
public class PostgresRateLimiter {
  private final JdbcTemplate jdbc;
  private final Clock clock;
  private final int requestsPerMinute;

  public PostgresRateLimiter(JdbcTemplate jdbc, Clock clock, SubmissionProperties properties) {
    this.jdbc = jdbc;
    this.clock = clock;
    this.requestsPerMinute = properties.limits().requestsPerMinute();
  }

  public Decision consume(UUID actorId) {
    var now = clock.instant();
    var window = now.truncatedTo(ChronoUnit.MINUTES);
    Integer count =
        jdbc.queryForObject(
            """
            WITH expired AS (
                DELETE FROM api_rate_limits WHERE window_start < ?
            )
            INSERT INTO api_rate_limits (actor_id, window_start, request_count)
            VALUES (?, ?, 1)
            ON CONFLICT (actor_id, window_start) DO UPDATE
            SET request_count = LEAST(api_rate_limits.request_count::bigint + 1, 2147483647)::int
            RETURNING request_count
            """,
            Integer.class,
            Timestamp.from(window.minus(2, ChronoUnit.MINUTES)),
            actorId,
            Timestamp.from(window));
    long retryAfter =
        Math.max(
            1, (Duration.between(now, window.plus(1, ChronoUnit.MINUTES)).toMillis() + 999) / 1000);
    return new Decision(count != null && count <= requestsPerMinute, retryAfter);
  }

  public record Decision(boolean allowed, long retryAfterSeconds) {}
}
