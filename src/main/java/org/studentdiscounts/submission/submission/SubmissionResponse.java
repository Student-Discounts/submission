package org.studentdiscounts.submission.submission;

import tools.jackson.databind.JsonNode;

public record SubmissionResponse(int status, JsonNode body, String etag) {
  public static String etag(int rowVersion) {
    return "\"" + rowVersion + "\"";
  }
}
