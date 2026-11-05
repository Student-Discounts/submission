package org.studentdiscounts.submission.integration;

import java.time.Instant;
import java.util.Set;
import java.util.UUID;
import tools.jackson.databind.JsonNode;

/** Validates the transport envelope independently of database state and transition rules. */
record WorkflowEvent(UUID id, Type type, String correlationId, JsonNode data) {
  enum Type {
    ReviewStarted("moderation"),
    SubmissionChangesRequested("moderation"),
    SubmissionRejected("moderation"),
    SubmissionApproved("moderation"),
    SubmissionReviewReopened("moderation"),
    OfferPublished("catalog"),
    OfferPublicationConflicted("catalog"),
    SubmissionSuspended("catalog"),
    SubmissionArchived("catalog"),
    MediaAssetChecked("media");

    private final String producer;

    Type(String producer) {
      this.producer = producer;
    }

    boolean isModeration() {
      return producer.equals("moderation");
    }
  }

  record MediaCheck(UUID assetId, String status, long revision) {
    MediaCheck {
      require(
          Set.of("QUARANTINED", "PROCESSING", "CLEAN", "REJECTED").contains(status)
              && revision >= 0,
          "INVALID_MEDIA_STATE");
    }
  }

  static WorkflowEvent parse(JsonNode event) {
    require(event != null && event.isObject(), "INVALID_ENVELOPE");
    require(integer(event, "schemaVersion") == 1, "INVALID_SCHEMA_VERSION");
    UUID id = uuid(event, "eventId");
    String name = text(event, "eventType");
    String producer = text(event, "producer");
    Type type;
    try {
      type = Type.valueOf(name);
    } catch (IllegalArgumentException unknown) {
      throw new InvalidWorkflowEvent("UNAUTHORIZED_PRODUCER");
    }
    require(type.producer.equals(producer), "UNAUTHORIZED_PRODUCER");
    require(event.path("data").isObject(), "INVALID_EVENT_DATA");
    try {
      Instant.parse(text(event, "occurredAt"));
    } catch (RuntimeException invalid) {
      throw new InvalidWorkflowEvent("INVALID_OCCURRED_AT");
    }
    String correlation = text(event, "correlationId");
    require(correlation.matches("[A-Za-z0-9._:-]{1,64}"), "INVALID_CORRELATION_ID");
    return new WorkflowEvent(id, type, correlation, event.path("data"));
  }

  UUID submissionId() {
    return uuid(data, "submissionId");
  }

  MediaCheck mediaCheck() {
    UUID asset = uuid(data, "mediaAssetId");
    String status = text(data, "status");
    long revision = longInteger(data, "revision");
    return new MediaCheck(asset, status, revision);
  }

  UUID versionId() {
    return uuid(data, "versionId");
  }

  int versionNo() {
    return integer(data, "versionNo");
  }

  int expectedRowVersion() {
    return integer(data, "expectedRowVersion");
  }

  UUID actorId() {
    return uuid(data, "actorId");
  }

  String reason() {
    String reason = text(data, "reason");
    require(reason.length() <= 2000, "INVALID_REASON");
    return reason;
  }

  static UUID uuid(JsonNode node, String field) {
    try {
      String value = text(node, field);
      require(
          value.matches(
              "[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"),
          "INVALID_" + field);
      return UUID.fromString(value);
    } catch (RuntimeException invalid) {
      throw new InvalidWorkflowEvent("INVALID_" + field);
    }
  }

  static String text(JsonNode node, String field) {
    JsonNode value = node.path(field);
    require(value.isString() && !value.asText().isBlank(), "INVALID_" + field);
    return value.asText();
  }

  static int integer(JsonNode node, String field) {
    JsonNode value = node.path(field);
    require(value.isIntegralNumber() && value.canConvertToInt(), "INVALID_" + field);
    return value.asInt();
  }

  private static long longInteger(JsonNode node, String field) {
    JsonNode value = node.path(field);
    require(value.isIntegralNumber() && value.canConvertToLong(), "INVALID_" + field);
    return value.asLong();
  }

  static void require(boolean condition, String code) {
    if (!condition) {
      throw new InvalidWorkflowEvent(code);
    }
  }
}
