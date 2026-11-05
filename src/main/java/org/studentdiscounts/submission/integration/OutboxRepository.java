package org.studentdiscounts.submission.integration;

import java.sql.Timestamp;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

/** Outbox row selection and acknowledgement; transport stays in the relay. */
@Repository
public class OutboxRepository {
  record Event(UUID id, UUID submissionId, String payload) {}

  private final NamedParameterJdbcTemplate jdbc;
  private final Clock clock;

  public OutboxRepository(NamedParameterJdbcTemplate jdbc, Clock clock) {
    this.jdbc = jdbc;
    this.clock = clock;
  }

  /** Retain this lock until the broker has acknowledged and published_at is committed. */
  Optional<Event> lockNext() {
    List<Event> records =
        jdbc.query(
            """
            select o.id,o.submission_id,o.payload::text from outbox_events o
            where o.published_at is null
              and not exists(select 1 from outbox_events earlier
                             where earlier.submission_id=o.submission_id and earlier.published_at is null
                               and earlier.aggregate_sequence<o.aggregate_sequence)
            order by o.occurred_at,o.id limit 1 for update of o skip locked
            """,
            Map.of(),
            (rs, index) ->
                new Event(
                    rs.getObject("id", UUID.class),
                    rs.getObject("submission_id", UUID.class),
                    rs.getString("payload")));
    return records.isEmpty() ? Optional.empty() : Optional.of(records.getFirst());
  }

  void markPublished(UUID eventId) {
    jdbc.update(
        "update outbox_events set published_at=:now where id=:id",
        Map.of("id", eventId, "now", Timestamp.from(clock.instant())));
  }

  double pendingCount() {
    return number("select count(*) from outbox_events where published_at is null");
  }

  double oldestPendingAge() {
    return number(
        """
        select coalesce(extract(epoch from (now()-min(occurred_at))),0)
        from outbox_events where published_at is null
        """);
  }

  private double number(String query) {
    try {
      Number value = jdbc.queryForObject(query, Map.of(), Number.class);
      return value == null ? 0 : value.doubleValue();
    } catch (RuntimeException unavailable) {
      return Double.NaN;
    }
  }
}
