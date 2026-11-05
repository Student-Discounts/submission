package org.studentdiscounts.submission.integration;

import java.util.List;
import java.util.UUID;
import org.studentdiscounts.submission.domain.SubmissionStatus;
import org.studentdiscounts.submission.domain.SubmissionType;
import tools.jackson.databind.JsonNode;

/** Current parent state together with its immutable reviewed version. */
record WorkflowSubmission(
    UUID id,
    UUID authorId,
    SubmissionType type,
    SubmissionStatus status,
    UUID targetOfferId,
    int rowVersion,
    UUID versionId,
    int versionNo,
    JsonNode payload,
    Integer baseOfferRowVersion,
    String timezone,
    List<UUID> publishedOfferIds) {}
