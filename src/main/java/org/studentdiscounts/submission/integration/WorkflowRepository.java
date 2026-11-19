package org.studentdiscounts.submission.integration;

import static org.studentdiscounts.submission.integration.WorkflowEvent.require;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Clock;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;
import org.studentdiscounts.submission.domain.SubmissionIds;
import org.studentdiscounts.submission.domain.SubmissionStatus;
import org.studentdiscounts.submission.domain.SubmissionType;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;

/**
 * PostgreSQL workflow persistence; callers own transactions and acquire the parent lock before
 * writes.
 */
@Repository
public class WorkflowRepository {
  private static final String CONSUMER = "submission-workflow-v1";
  private static final String CURRENT_VERSION =
      """
      select s.id,s.author_id,s.type::text,s.status::text,s.target_offer_id,s.row_version,
             s.published_offer_ids,v.id version_id,v.version_no,v.payload::text,
             v.base_offer_row_version,v.resolved_timezone
      from submissions s
      join submission_versions v on v.submission_id=s.id and v.version_no=s.current_version_no
      where s.id=:id
      """;
  private final NamedParameterJdbcTemplate jdbc;
  private final ObjectMapper json;
  private final Clock clock;

  public WorkflowRepository(NamedParameterJdbcTemplate jdbc, ObjectMapper json, Clock clock) {
    this.jdbc = jdbc;
    this.json = json;
    this.clock = clock;
  }

  boolean inboxContains(UUID eventId) {
    return Boolean.TRUE.equals(
        jdbc.queryForObject(
            """
            select exists(select 1 from inbox_events where consumer_name=:consumer and event_id=:id)
            """,
            Map.of("consumer", CONSUMER, "id", eventId),
            Boolean.class));
  }

  boolean reserveInbox(UUID eventId) {
    return jdbc.update(
            """
            insert into inbox_events(consumer_name,event_id,processed_at)
            values(:consumer,:id,:now) on conflict do nothing
            """,
            Map.of("consumer", CONSUMER, "id", eventId, "now", Timestamp.from(clock.instant())))
        == 1;
  }

  WorkflowSubmission read(UUID id) {
    return read(id, false);
  }

  WorkflowSubmission lock(UUID id) {
    return read(id, true);
  }

  private WorkflowSubmission read(UUID id, boolean lock) {
    List<WorkflowSubmission> rows =
        jdbc.query(CURRENT_VERSION + (lock ? " for update of s" : ""), Map.of("id", id), this::map);
    require(rows.size() == 1, "UNKNOWN_SUBMISSION");
    return rows.getFirst();
  }

  List<UUID> pendingValidationIds(UUID after, int limit) {
    String cursor = after == null ? "" : " AND id>:after";
    MapSqlParameterSource parameters =
        new MapSqlParameterSource().addValue("after", after).addValue("limit", limit);
    List<UUID> selected =
        jdbc.queryForList(
            "SELECT id FROM submissions WHERE status IN ('SUBMITTED','VALIDATING')"
                + cursor
                + " ORDER BY id LIMIT :limit",
            parameters,
            UUID.class);
    if (after != null && selected.size() < limit) {
      // The wrap range is disjoint from id > after, so a batch never repeats an aggregate.
      parameters.addValue("limit", limit - selected.size());
      selected.addAll(
          jdbc.queryForList(
              """
              SELECT id FROM submissions WHERE status IN ('SUBMITTED','VALIDATING') AND id<=:after
              ORDER BY id LIMIT :limit
              """,
              parameters,
              UUID.class));
    }
    return selected;
  }

  List<UUID> lockValidatingVersionsForAsset(UUID assetId) {
    return jdbc.queryForList(
        """
        select v.id from submissions s
        join submission_versions v on v.submission_id=s.id and v.version_no=s.current_version_no
        where s.status in ('SUBMITTED','VALIDATING')
          and exists(select 1 from jsonb_array_elements(coalesce(v.payload->'attachments','[]'::jsonb)) a
                     where a->>'mediaAssetId'=:asset)
        order by s.id for update of s
        """,
        Map.of("asset", assetId.toString()),
        UUID.class);
  }

  void recordMedia(UUID versionId, WorkflowEvent.MediaCheck check) {
    jdbc.update(
        """
        insert into submission_media_checks(version_id,media_asset_id,status,asset_revision)
        values(:version,:asset,:status,:revision)
        on conflict(version_id,media_asset_id) do update
        set status=excluded.status,asset_revision=excluded.asset_revision
        where submission_media_checks.asset_revision<excluded.asset_revision
        """,
        Map.of(
            "version",
            versionId,
            "asset",
            check.assetId(),
            "status",
            check.status(),
            "revision",
            check.revision()));
  }

  List<String> mediaStatuses(UUID versionId) {
    return jdbc.queryForList(
        "select status from submission_media_checks where version_id=:version",
        Map.of("version", versionId),
        String.class);
  }

  boolean isMediaClean(UUID versionId, UUID assetId) {
    List<String> states =
        jdbc.queryForList(
            """
            select status from submission_media_checks where version_id=:version and media_asset_id=:asset
            """,
            Map.of("version", versionId, "asset", assetId),
            String.class);
    return states.size() == 1 && states.getFirst().equals("CLEAN");
  }

  boolean hasPublicationConflict(UUID submissionId, UUID versionId) {
    return Boolean.TRUE.equals(
        jdbc.queryForObject(
            """
            select exists(select 1 from submission_status_history
                          where submission_id=:id and version_id=:version
                            and reason='CATALOG_VERSION_CONFLICT' and to_status='PENDING_REVIEW')
            """,
            Map.of("id", submissionId, "version", versionId),
            Boolean.class));
  }

  void recordPublishedOffers(UUID submissionId, List<UUID> offerIds) {
    String ids = "{" + String.join(",", offerIds.stream().map(UUID::toString).toList()) + "}";
    jdbc.update(
        "update submissions set published_offer_ids=cast(:ids as uuid[]) where id=:id",
        Map.of("ids", ids, "id", submissionId));
  }

  void seedEditableDraft(WorkflowSubmission row) {
    jdbc.update(
        "update submissions set draft_payload=cast(:payload as jsonb) where id=:id",
        Map.of("payload", row.payload().toString(), "id", row.id()));
  }

  /** The parent must be locked so row version, history and aggregate sequence advance together. */
  void transition(
      WorkflowSubmission row,
      SubmissionStatus target,
      UUID actor,
      String reason,
      String correlation) {
    MapSqlParameterSource params =
        new MapSqlParameterSource()
            .addValue("id", row.id())
            .addValue("target", target.name())
            .addValue("expected", row.rowVersion())
            .addValue("now", Timestamp.from(clock.instant()));
    require(
        jdbc.update(
                """
                update submissions set status=cast(:target as submission_status),row_version=row_version+1,updated_at=:now
                where id=:id and row_version=:expected
                """,
                params)
            == 1,
        "STALE_ROW_VERSION");
    params
        .addValue("historyId", SubmissionIds.newId())
        .addValue("version", row.versionId())
        .addValue("from", row.status().name())
        .addValue("actor", actor)
        .addValue("reason", reason)
        .addValue("correlation", correlation);
    jdbc.update(
        """
        insert into submission_status_history(id,submission_id,version_id,from_status,to_status,
                                              actor_id,reason,correlation_id,occurred_at)
        values(:historyId,:id,:version,cast(:from as submission_status),cast(:target as submission_status),
               :actor,:reason,:correlation,:now)
        """,
        params);
    Long sequence =
        jdbc.queryForObject(
            """
            select coalesce(max(aggregate_sequence),0)+1 from outbox_events where submission_id=:id
            """,
            Map.of("id", row.id()),
            Long.class);
    UUID eventId = SubmissionIds.newId();
    ObjectNode event = statusChanged(row, target, actor, reason, correlation, eventId, sequence);
    params
        .addValue("eventId", eventId)
        .addValue("sequence", sequence)
        .addValue("payload", event.toString());
    jdbc.update(
        """
        insert into outbox_events(id,submission_id,aggregate_sequence,event_type,payload,occurred_at)
        values(:eventId,:id,:sequence,'SubmissionStatusChanged',cast(:payload as jsonb),:now)
        """,
        params);
  }

  double pendingValidationCount() {
    return number("select count(*) from submissions where status in ('SUBMITTED','VALIDATING')");
  }

  double oldestValidationAge() {
    return number(
        """
        select coalesce(extract(epoch from (now()-min(updated_at))),0)
        from submissions where status in ('SUBMITTED','VALIDATING')
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

  private WorkflowSubmission map(ResultSet rs, int index) throws SQLException {
    java.sql.Array published = rs.getArray("published_offer_ids");
    List<UUID> ids = published == null ? List.of() : Arrays.asList((UUID[]) published.getArray());
    return new WorkflowSubmission(
        rs.getObject("id", UUID.class),
        rs.getObject("author_id", UUID.class),
        SubmissionType.valueOf(rs.getString("type")),
        SubmissionStatus.valueOf(rs.getString("status")),
        rs.getObject("target_offer_id", UUID.class),
        rs.getInt("row_version"),
        rs.getObject("version_id", UUID.class),
        rs.getInt("version_no"),
        json.readTree(rs.getString("payload")),
        rs.getObject("base_offer_row_version", Integer.class),
        rs.getString("resolved_timezone"),
        ids);
  }

  private ObjectNode statusChanged(
      WorkflowSubmission row,
      SubmissionStatus target,
      UUID actor,
      String reason,
      String correlation,
      UUID eventId,
      long sequence) {
    ObjectNode event =
        json.createObjectNode()
            .put("eventId", eventId.toString())
            .put("eventType", "SubmissionStatusChanged")
            .put("schemaVersion", 1)
            .put("producer", "submission")
            .put("occurredAt", clock.instant().toString())
            .put("correlationId", correlation)
            .put("aggregateSequence", sequence);
    ObjectNode data =
        event
            .putObject("data")
            .put("submissionId", row.id().toString())
            .put("versionId", row.versionId().toString())
            .put("versionNo", row.versionNo())
            .put("fromStatus", row.status().name())
            .put("toStatus", target.name())
            .put("rowVersion", row.rowVersion() + 1)
            .put("expectedRowVersion", row.rowVersion() + 1);
    if (actor != null) {
      data.put("actorId", actor.toString());
    } else {
      data.putNull("actorId");
    }
    if (reason != null) {
      data.put("reason", reason);
    } else {
      data.putNull("reason");
    }
    return event;
  }
}
