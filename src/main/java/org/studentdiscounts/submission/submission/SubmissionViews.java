package org.studentdiscounts.submission.submission;

import org.springframework.stereotype.Component;
import org.studentdiscounts.submission.submission.SubmissionStore.SubmissionRow;
import org.studentdiscounts.submission.submission.SubmissionStore.VersionRow;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;

/** HTTP projections shared by commands and queries; persistence returns only typed rows. */
@Component
public class SubmissionViews {
  private final SubmissionStore store;
  private final ObjectMapper json;

  public SubmissionViews(SubmissionStore store, ObjectMapper json) {
    this.store = store;
    this.json = json;
  }

  public SubmissionResponse metadata(int status, SubmissionRow row) {
    ObjectNode body = json.valueToTree(row);
    body.remove("currentVersionNo");
    body.set("currentVersion", json.valueToTree(store.latest(row)));
    body.set("latestStatusReason", json.valueToTree(store.latestReason(row.id())));
    return new SubmissionResponse(status, body, SubmissionResponse.etag(row.rowVersion()));
  }

  public SubmissionResponse submitted(int status, SubmissionRow row, VersionRow version) {
    ObjectNode body = json.createObjectNode();
    body.put("submissionId", row.id().toString());
    body.put("status", row.status().name());
    body.put("versionId", version.id().toString());
    body.put("versionNo", version.versionNo());
    body.put("contentSha256", version.contentSha256());
    body.put("rowVersion", row.rowVersion());
    return new SubmissionResponse(status, body, SubmissionResponse.etag(row.rowVersion()));
  }

  public SubmissionResponse version(int status, SubmissionRow row, VersionRow version) {
    return new SubmissionResponse(
        status, json.valueToTree(version), SubmissionResponse.etag(row.rowVersion()));
  }
}
