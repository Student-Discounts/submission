package org.studentdiscounts.submission.submission;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Clock;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.slf4j.MDC;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;
import org.studentdiscounts.submission.domain.SubmissionIds;
import org.studentdiscounts.submission.domain.SubmissionStatus;
import org.studentdiscounts.submission.domain.SubmissionType;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;

@Repository
public class SubmissionStore {
  private final NamedParameterJdbcTemplate jdbc;
  private final ObjectMapper json;
  private final Clock clock;

  public SubmissionStore(NamedParameterJdbcTemplate jdbc, ObjectMapper json, Clock clock) {
    this.jdbc = jdbc;
    this.json = json;
    this.clock = clock;
  }

  private static Map<String, Object> params(Object... pairs) {
    Map<String, Object> result = new HashMap<>();
    for (int i = 0; i < pairs.length; i += 2) result.put((String) pairs[i], pairs[i + 1]);
    return result;
  }

  public record SubmissionRow(
      UUID id,
      SubmissionType type,
      SubmissionStatus status,
      UUID authorId,
      UUID targetPlaceId,
      UUID targetOfferId,
      JsonNode draftPayload,
      Integer currentVersionNo,
      int rowVersion,
      Integer draftBaseOfferRowVersion,
      List<UUID> publishedOfferIds,
      Instant createdAt,
      Instant updatedAt) {}

  public record VersionRow(
      UUID id,
      UUID submissionId,
      int versionNo,
      UUID previousVersionId,
      JsonNode payload,
      int payloadSchemaVersion,
      String contentSha256,
      Integer baseOfferRowVersion,
      UUID createdBy,
      Instant createdAt,
      String resolvedTimezone) {}

  private final RowMapper<SubmissionRow> submissionMapper =
      (result, index) ->
          new SubmissionRow(
              result.getObject("id", UUID.class),
              SubmissionType.valueOf(result.getString("type")),
              SubmissionStatus.valueOf(result.getString("status")),
              result.getObject("author_id", UUID.class),
              result.getObject("target_place_id", UUID.class),
              result.getObject("target_offer_id", UUID.class),
              parse(result.getString("draft_payload")),
              (Integer) result.getObject("current_version_no"),
              result.getInt("row_version"),
              (Integer) result.getObject("draft_base_offer_row_version"),
              publishedOffers(result),
              result.getTimestamp("created_at").toInstant(),
              result.getTimestamp("updated_at").toInstant());

  private List<UUID> publishedOffers(ResultSet result) throws SQLException {
    var ids = result.getArray("published_offer_ids");
    return ids == null ? null : Arrays.asList((UUID[]) ids.getArray());
  }

  private final RowMapper<VersionRow> versionMapper =
      (result, index) ->
          new VersionRow(
              result.getObject("id", UUID.class),
              result.getObject("submission_id", UUID.class),
              result.getInt("version_no"),
              result.getObject("previous_version_id", UUID.class),
              parse(result.getString("payload")),
              result.getInt("payload_schema_version"),
              result.getString("content_sha256"),
              (Integer) result.getObject("base_offer_row_version"),
              result.getObject("created_by", UUID.class),
              result.getTimestamp("created_at").toInstant(),
              result.getString("resolved_timezone"));

  private JsonNode parse(String value) {
    return value == null ? null : json.readTree(value);
  }

  public SubmissionRow find(UUID id) {
    return one(
        jdbc.query("SELECT * FROM submissions WHERE id=:id", params("id", id), submissionMapper));
  }

  public SubmissionRow lock(UUID id) {
    return one(
        jdbc.query(
            "SELECT * FROM submissions WHERE id=:id FOR UPDATE",
            params("id", id),
            submissionMapper));
  }

  private static <T> T one(List<T> values) {
    if (values.isEmpty()) throw new SubmissionException(404, "NOT_FOUND");
    return values.getFirst();
  }

  public VersionRow version(UUID submission, int number) {
    return one(
        jdbc.query(
            "SELECT * FROM submission_versions WHERE submission_id=:id AND version_no=:no",
            params("id", submission, "no", number),
            versionMapper));
  }

  public VersionRow latest(SubmissionRow row) {
    return row.currentVersionNo() == null ? null : version(row.id(), row.currentVersionNo());
  }

  public List<VersionRow> versions(UUID id, Integer before, int limit) {
    return jdbc.query(
        "SELECT * FROM submission_versions WHERE submission_id=:id AND (:before IS NULL OR"
            + " version_no<:before) ORDER BY version_no DESC LIMIT :limit",
        params("id", id, "before", before == null ? Integer.MAX_VALUE : before, "limit", limit),
        versionMapper);
  }

  public List<SubmissionRow> own(UUID actor, UUID before, int limit) {
    String cursor = before == null ? "" : " AND id<:before";
    return jdbc.query(
        "SELECT * FROM submissions WHERE author_id=:actor"
            + cursor
            + " ORDER BY id DESC LIMIT :limit",
        params("actor", actor, "before", before, "limit", limit),
        submissionMapper);
  }

  public VersionRow append(
      SubmissionRow row, JsonNode payload, String hash, Integer base, UUID actor, String zone) {
    VersionRow old = latest(row);
    int number = old == null ? 1 : old.versionNo() + 1;
    UUID id = SubmissionIds.newId();
    Instant now = clock.instant();
    jdbc.update(
        """
        INSERT INTO submission_versions(id,submission_id,version_no,previous_version_id,payload,
                payload_schema_version,content_sha256,base_offer_row_version,created_by,created_at,resolved_timezone)
        VALUES(:id,:sid,:no,:prev,CAST(:payload AS jsonb),1,:hash,:base,:actor,:now,:zone)
        """,
        params(
            "id",
            id,
            "sid",
            row.id(),
            "no",
            number,
            "prev",
            old == null ? null : old.id(),
            "payload",
            payload.toString(),
            "hash",
            hash,
            "base",
            base,
            "actor",
            actor,
            "now",
            java.sql.Timestamp.from(now),
            "zone",
            zone));
    return new VersionRow(
        id,
        row.id(),
        number,
        old == null ? null : old.id(),
        payload,
        1,
        hash,
        base,
        actor,
        now,
        zone);
  }

  public SubmissionRow update(
      SubmissionRow row, SubmissionStatus status, JsonNode draft, Integer version, Integer base) {
    int count =
        jdbc.update(
            """
            UPDATE submissions SET status=CAST(:status AS submission_status),draft_payload=CAST(:draft AS jsonb),
                    current_version_no=:no,draft_base_offer_row_version=:base,
                    row_version=row_version+1,updated_at=:now
            WHERE id=:id AND row_version=:expected
            """,
            params(
                "status",
                status.name(),
                "draft",
                draft == null ? null : draft.toString(),
                "no",
                version,
                "base",
                base,
                "now",
                java.sql.Timestamp.from(clock.instant()),
                "id",
                row.id(),
                "expected",
                row.rowVersion()));
    if (count != 1) throw new SubmissionException(412, "STALE_ROW_VERSION");
    return find(row.id());
  }

  public void history(
      SubmissionRow row,
      SubmissionStatus from,
      SubmissionStatus to,
      VersionRow version,
      UUID actor,
      String reason) {
    jdbc.update(
        """
        INSERT INTO submission_status_history(id,submission_id,version_id,from_status,to_status,
                actor_id,reason,correlation_id,occurred_at)
        VALUES(:id,:sid,:vid,CAST(:from AS submission_status),CAST(:to AS submission_status),
                :actor,:reason,:correlation,:now)
        """,
        params(
            "id",
            SubmissionIds.newId(),
            "sid",
            row.id(),
            "vid",
            version == null ? null : version.id(),
            "from",
            from == null ? null : from.name(),
            "to",
            to.name(),
            "actor",
            actor,
            "reason",
            reason,
            "correlation",
            correlation(),
            "now",
            java.sql.Timestamp.from(clock.instant())));
  }

  private void event(SubmissionRow row, String type, ObjectNode data) {
    long sequence =
        Objects.requireNonNull(
            jdbc.queryForObject(
                "SELECT COALESCE(max(aggregate_sequence),0)+1 FROM outbox_events WHERE"
                    + " submission_id=:id",
                params("id", row.id()),
                Long.class));
    UUID id = SubmissionIds.newId();
    Instant now = clock.instant();
    ObjectNode envelope = json.createObjectNode();
    envelope.put("eventId", id.toString());
    envelope.put("eventType", type);
    envelope.put("schemaVersion", 1);
    envelope.put("producer", "submission");
    envelope.put("occurredAt", now.toString());
    envelope.put("correlationId", correlation());
    envelope.put("aggregateSequence", sequence);
    envelope.set("data", data);
    jdbc.update(
        """
        INSERT INTO outbox_events(id,submission_id,aggregate_sequence,event_type,payload,occurred_at)
        VALUES(:id,:sid,:seq,:type,CAST(:payload AS jsonb),:now)
        """,
        params(
            "id",
            id,
            "sid",
            row.id(),
            "seq",
            sequence,
            "type",
            type,
            "payload",
            envelope.toString(),
            "now",
            java.sql.Timestamp.from(now)));
  }

  public void versionEvent(SubmissionRow row, VersionRow version, String eventType) {
    ObjectNode data = json.createObjectNode();
    tuple(data, row, version);
    data.set("payload", version.payload());
    data.put("payloadSchemaVersion", version.payloadSchemaVersion());
    data.put("contentSha256", version.contentSha256());
    data.set("baseOfferRowVersion", json.valueToTree(version.baseOfferRowVersion()));
    data.put("authorId", row.authorId().toString());
    event(row, eventType, data);
  }

  public void statusEvent(
      SubmissionRow row, SubmissionStatus from, VersionRow version, UUID actor, String reason) {
    ObjectNode data = json.createObjectNode();
    tuple(data, row, version);
    data.put("fromStatus", from.name());
    data.put("toStatus", row.status().name());
    if (actor != null) data.put("actorId", actor.toString());
    if (reason != null) data.put("reason", reason);
    event(row, "SubmissionStatusChanged", data);
  }

  private void tuple(ObjectNode data, SubmissionRow row, VersionRow version) {
    data.put("submissionId", row.id().toString());
    data.put("rowVersion", row.rowVersion());
    if (version != null) {
      data.put("versionId", version.id().toString());
      data.put("versionNo", version.versionNo());
    }
  }

  private static String correlation() {
    String correlation = MDC.get("correlationId");
    return correlation == null ? SubmissionIds.newId().toString() : correlation;
  }

  public String latestReason(UUID id) {
    var reasons =
        jdbc.query(
            "SELECT reason FROM submission_status_history WHERE submission_id=:id ORDER BY"
                + " occurred_at DESC,id DESC LIMIT 1",
            params("id", id),
            (row, index) -> row.getString(1));
    return reasons.isEmpty() ? null : reasons.getFirst();
  }

  public record HistoryRow(
      UUID id,
      UUID submissionId,
      UUID versionId,
      SubmissionStatus fromStatus,
      SubmissionStatus toStatus,
      UUID actorId,
      String reason,
      String correlationId,
      Instant occurredAt) {}

  public List<HistoryRow> history(UUID id, UUID before, int limit) {
    String cursor = before == null ? "" : " AND id<:before";
    return jdbc.query(
        "SELECT * FROM submission_status_history WHERE submission_id=:id"
            + cursor
            + " ORDER BY id DESC LIMIT :limit",
        params("id", id, "before", before, "limit", limit),
        (row, index) ->
            new HistoryRow(
                row.getObject("id", UUID.class),
                id,
                row.getObject("version_id", UUID.class),
                nullableStatus(row.getString("from_status")),
                SubmissionStatus.valueOf(row.getString("to_status")),
                row.getObject("actor_id", UUID.class),
                row.getString("reason"),
                row.getString("correlation_id"),
                row.getTimestamp("occurred_at").toInstant()));
  }

  private static SubmissionStatus nullableStatus(String value) {
    return value == null ? null : SubmissionStatus.valueOf(value);
  }

  public SubmissionRow create(
      SubmissionType type, UUID author, UUID place, UUID offer, JsonNode payload, Integer base) {
    UUID id = SubmissionIds.newId();
    jdbc.update(
        """
        INSERT INTO submissions(id,type,author_id,target_place_id,target_offer_id,draft_payload,
                draft_base_offer_row_version,created_at,updated_at)
        VALUES(:id,CAST(:type AS submission_type),:author,:place,:offer,CAST(:payload AS jsonb),:base,:now,:now)
        """,
        params(
            "id",
            id,
            "type",
            type.name(),
            "author",
            author,
            "place",
            place,
            "offer",
            offer,
            "payload",
            payload.toString(),
            "base",
            base,
            "now",
            java.sql.Timestamp.from(clock.instant())));
    return find(id);
  }

  public void recordMedia(VersionRow version, UUID asset, String status, long revision) {
    jdbc.update(
        """
        INSERT INTO submission_media_checks(version_id,media_asset_id,status,asset_revision)
        VALUES(:v,:id,:status,:revision)
        ON CONFLICT(version_id,media_asset_id) DO UPDATE
        SET status=EXCLUDED.status,asset_revision=EXCLUDED.asset_revision
        WHERE submission_media_checks.asset_revision<EXCLUDED.asset_revision
        """,
        params("v", version.id(), "id", asset, "status", status, "revision", revision));
  }

  public SubmissionResponse completedRequest(
      UUID actor, String operation, String key, String fingerprint) {
    var responses =
        jdbc.query(
            "SELECT request_sha256,response_status,response_body,response_etag FROM"
                + " idempotency_requests WHERE actor_id=:actor AND operation=:op AND"
                + " request_key=:key",
            params("actor", actor, "op", operation, "key", key),
            (row, index) -> {
              if (!row.getString(1).equals(fingerprint))
                throw new SubmissionException(409, "IDEMPOTENCY_KEY_REUSED");
              if (row.getObject(2) == null)
                throw new IllegalStateException("Incomplete idempotency reservation");
              return new SubmissionResponse(
                  row.getInt(2), parse(row.getString(3)), row.getString(4));
            });
    return responses.isEmpty() ? null : responses.getFirst();
  }

  public boolean reserveRequest(UUID actor, String operation, String key, String fingerprint) {
    return jdbc.update(
            "INSERT INTO idempotency_requests(actor_id,operation,request_key,request_sha256)"
                + " VALUES(:actor,:op,:key,:hash) ON CONFLICT DO NOTHING",
            params("actor", actor, "op", operation, "key", key, "hash", fingerprint))
        == 1;
  }

  public SubmissionResponse completeRequest(
      UUID actor, String operation, String key, SubmissionResponse response) {
    String recorded =
        jdbc.queryForObject(
            """
            UPDATE idempotency_requests
            SET response_status=:status,response_body=CAST(:body AS jsonb),response_etag=:etag
            WHERE actor_id=:actor AND operation=:op AND request_key=:key
            RETURNING response_body::text
            """,
            params(
                "status",
                response.status(),
                "body",
                response.body().toString(),
                "etag",
                response.etag(),
                "actor",
                actor,
                "op",
                operation,
                "key",
                key),
            String.class);
    // The initial response must use the same JSONB numeric representation as every replay.
    return new SubmissionResponse(response.status(), parse(recorded), response.etag());
  }
}
