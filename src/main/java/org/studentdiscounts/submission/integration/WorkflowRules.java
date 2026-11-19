package org.studentdiscounts.submission.integration;

import static org.studentdiscounts.submission.integration.WorkflowEvent.require;
import static org.studentdiscounts.submission.integration.WorkflowEvent.uuid;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import org.studentdiscounts.submission.domain.PayloadJson;
import org.studentdiscounts.submission.domain.SubmissionStatus;
import org.studentdiscounts.submission.domain.SubmissionType;
import tools.jackson.databind.JsonNode;

/** Pure reviewed-tuple, actor, publication identity and lifecycle rules. */
final class WorkflowRules {
  private WorkflowRules() {}

  record Transition(
      SubmissionStatus target,
      UUID actorId,
      String reason,
      List<UUID> publishedOfferIds,
      boolean alreadyApplied) {}

  static Transition evaluate(WorkflowSubmission row, WorkflowEvent event) {
    JsonNode data = event.data();
    require(
        row.versionId().equals(event.versionId()) && row.versionNo() == event.versionNo(),
        "STALE_REVIEWED_VERSION");
    UUID actor = null;
    if (event.type().isModeration()) {
      actor = event.actorId();
      require(!actor.equals(row.authorId()), "SELF_REVIEW_FORBIDDEN");
    }
    if (event.type() == WorkflowEvent.Type.OfferPublished && row.status().published()) {
      require(
          new HashSet<>(publishedIds(data, row)).equals(new HashSet<>(row.publishedOfferIds())),
          "PUBLICATION_IDS_CONFLICT");
      return new Transition(SubmissionStatus.PUBLISHED, actor, null, List.of(), true);
    }
    SubmissionStatus target = target(event.type());
    // A distinct transport event for an already-completed operation adds no history.
    if (row.status() == target && row.rowVersion() == event.expectedRowVersion() + 1) {
      return new Transition(target, actor, null, List.of(), true);
    }
    require(row.rowVersion() == event.expectedRowVersion(), "STALE_ROW_VERSION");
    require(allowed(event.type(), row), "INVALID_TRANSITION");
    String reason =
        switch (event.type()) {
          case SubmissionChangesRequested,
              SubmissionRejected,
              SubmissionReviewReopened,
              SubmissionSuspended,
              SubmissionArchived,
              OfferPublicationConflicted ->
              event.reason();
          default -> null;
        };
    if (event.type() == WorkflowEvent.Type.SubmissionApproved) {
      requireReviewedPayload(row, data);
    }
    List<UUID> offerIds =
        event.type() == WorkflowEvent.Type.OfferPublished ? publishedIds(data, row) : List.of();
    if (event.type() == WorkflowEvent.Type.OfferPublicationConflicted) {
      require("CATALOG_VERSION_CONFLICT".equals(reason), "INVALID_REASON");
    }
    return new Transition(target, actor, reason, offerIds, false);
  }

  static List<UUID> assets(JsonNode payload) {
    List<UUID> result = new ArrayList<>();
    for (JsonNode attachment : payload.path("attachments")) {
      result.add(uuid(attachment, "mediaAssetId"));
    }
    return result;
  }

  private static void requireReviewedPayload(WorkflowSubmission row, JsonNode data) {
    require(
        data.path("payload").isObject()
            && PayloadJson.hash(row.payload()).equals(PayloadJson.hash(data.path("payload"))),
        "REVIEWED_PAYLOAD_MISMATCH");
    require(
        row.payload().path("place").path("location").isObject() && row.timezone() != null,
        "COORDINATES_REQUIRED");
    try {
      ZoneId.of(row.timezone());
    } catch (RuntimeException invalid) {
      throw new InvalidWorkflowEvent("INVALID_TIMEZONE");
    }
  }

  private static SubmissionStatus target(WorkflowEvent.Type event) {
    return switch (event) {
      case ReviewStarted -> SubmissionStatus.IN_REVIEW;
      case SubmissionChangesRequested -> SubmissionStatus.CHANGES_REQUESTED;
      case SubmissionRejected -> SubmissionStatus.REJECTED;
      case SubmissionApproved -> SubmissionStatus.APPROVED;
      case OfferPublished -> SubmissionStatus.PUBLISHED;
      case OfferPublicationConflicted, SubmissionReviewReopened -> SubmissionStatus.PENDING_REVIEW;
      case SubmissionSuspended -> SubmissionStatus.SUSPENDED;
      case SubmissionArchived -> SubmissionStatus.ARCHIVED;
      case MediaAssetChecked -> throw new InvalidWorkflowEvent("UNKNOWN_EVENT");
    };
  }

  private static boolean allowed(WorkflowEvent.Type event, WorkflowSubmission row) {
    return switch (event) {
      case ReviewStarted -> row.status() == SubmissionStatus.PENDING_REVIEW;
      case SubmissionChangesRequested, SubmissionRejected, SubmissionApproved ->
          row.status() == SubmissionStatus.IN_REVIEW;
      case OfferPublished -> row.status() == SubmissionStatus.APPROVED;
      case OfferPublicationConflicted ->
          row.status() == SubmissionStatus.APPROVED && row.type() == SubmissionType.CHANGE;
      case SubmissionSuspended -> row.status() == SubmissionStatus.PUBLISHED;
      case SubmissionArchived ->
          row.status() == SubmissionStatus.PUBLISHED || row.status() == SubmissionStatus.SUSPENDED;
      case SubmissionReviewReopened -> row.status() == SubmissionStatus.SUSPENDED;
      case MediaAssetChecked -> false;
    };
  }

  private static List<UUID> publishedIds(JsonNode data, WorkflowSubmission row) {
    JsonNode ids = data.path("offerIds");
    require(
        ids.isArray() && ids.size() == row.payload().path("offers").size(),
        "PUBLICATION_IDS_INVALID");
    List<UUID> result = new ArrayList<>();
    for (JsonNode id : ids) {
      try {
        require(id.isString(), "PUBLICATION_IDS_INVALID");
        result.add(UUID.fromString(id.asText()));
      } catch (IllegalArgumentException invalid) {
        throw new InvalidWorkflowEvent("PUBLICATION_IDS_INVALID");
      }
    }
    require(new HashSet<>(result).size() == result.size(), "PUBLICATION_IDS_INVALID");
    return result;
  }
}
