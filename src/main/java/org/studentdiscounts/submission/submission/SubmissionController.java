package org.studentdiscounts.submission.submission;

import java.net.URI;
import java.util.EnumSet;
import java.util.UUID;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.studentdiscounts.submission.domain.ActorRole;
import org.studentdiscounts.submission.domain.SubmissionActor;
import tools.jackson.databind.JsonNode;

@RestController
@RequestMapping("/api/v1")
public class SubmissionController {
  private final AuthorSubmissionCommands commands;
  private final SubmissionQueries queries;
  private final ReviewCorrections corrections;

  public SubmissionController(
      AuthorSubmissionCommands commands, SubmissionQueries queries, ReviewCorrections corrections) {
    this.commands = commands;
    this.queries = queries;
    this.corrections = corrections;
  }

  public static int expectedVersion(String value) {
    if (value == null) throw new SubmissionException(428, "IF_MATCH_REQUIRED");
    if (!value.matches("\"[0-9]+\"")) throw new SubmissionException(400, "INVALID_IF_MATCH");
    try {
      return Integer.parseInt(value.substring(1, value.length() - 1));
    } catch (NumberFormatException ex) {
      throw new SubmissionException(400, "INVALID_IF_MATCH");
    }
  }

  private ResponseEntity<JsonNode> response(SubmissionResponse response) {
    return ResponseEntity.status(response.status()).eTag(response.etag()).body(response.body());
  }

  private SubmissionActor actor(Jwt jwt) {
    EnumSet<ActorRole> roles = EnumSet.noneOf(ActorRole.class);
    for (String role : jwt.getClaimAsStringList("roles")) {
      roles.add(ActorRole.valueOf(role));
    }
    return new SubmissionActor(UUID.fromString(jwt.getSubject()), roles);
  }

  @PostMapping("/submissions")
  public ResponseEntity<JsonNode> create(
      @AuthenticationPrincipal Jwt jwt,
      @RequestHeader(value = "Idempotency-Key", required = false) String key,
      @RequestBody JsonNode body) {
    var r = commands.create(actor(jwt), key, body);
    return ResponseEntity.status(r.status())
        .eTag(r.etag())
        .location(URI.create("/api/v1/submissions/" + r.body().path("id").asText()))
        .body(r.body());
  }

  @PatchMapping(value = "/submissions/{id}", consumes = "application/merge-patch+json")
  public ResponseEntity<JsonNode> patch(
      @AuthenticationPrincipal Jwt jwt,
      @PathVariable UUID id,
      @RequestHeader(value = "If-Match", required = false) String etag,
      @RequestBody JsonNode body) {
    return response(commands.patch(actor(jwt), id, expectedVersion(etag), body));
  }

  @PostMapping("/submissions/{id}/submit")
  public ResponseEntity<JsonNode> submit(
      @AuthenticationPrincipal Jwt jwt,
      @PathVariable UUID id,
      @RequestHeader(value = "If-Match", required = false) String etag,
      @RequestHeader(value = "Idempotency-Key", required = false) String key,
      @RequestBody(required = false) JsonNode body) {
    if (body != null) throw new SubmissionException(400, "BODY_NOT_ALLOWED");
    return response(commands.submit(actor(jwt), id, expectedVersion(etag), key));
  }

  @PostMapping("/submissions/{id}/cancel")
  public ResponseEntity<JsonNode> cancel(
      @AuthenticationPrincipal Jwt jwt,
      @PathVariable UUID id,
      @RequestHeader(value = "If-Match", required = false) String etag,
      @RequestBody(required = false) JsonNode body) {
    if (body != null) throw new SubmissionException(400, "BODY_NOT_ALLOWED");
    return response(commands.cancel(actor(jwt), id, expectedVersion(etag)));
  }

  @GetMapping("/submissions/{id}")
  public ResponseEntity<JsonNode> get(@AuthenticationPrincipal Jwt jwt, @PathVariable UUID id) {
    return response(queries.get(actor(jwt), id));
  }

  @GetMapping("/submissions")
  public JsonNode list(
      @AuthenticationPrincipal Jwt jwt,
      @RequestParam(required = false) UUID before,
      @RequestParam(defaultValue = "20") int limit) {
    return queries.list(actor(jwt), before, limit);
  }

  @GetMapping("/submissions/{id}/versions")
  public JsonNode versions(
      @AuthenticationPrincipal Jwt jwt,
      @PathVariable UUID id,
      @RequestParam(required = false) Integer before,
      @RequestParam(defaultValue = "20") int limit) {
    return queries.versions(actor(jwt), id, before, limit);
  }

  @GetMapping("/submissions/{id}/versions/{versionNo}")
  public JsonNode version(
      @AuthenticationPrincipal Jwt jwt, @PathVariable UUID id, @PathVariable int versionNo) {
    return queries.version(actor(jwt), id, versionNo);
  }

  @GetMapping("/submissions/{id}/history")
  public JsonNode history(
      @AuthenticationPrincipal Jwt jwt,
      @PathVariable UUID id,
      @RequestParam(required = false) UUID before,
      @RequestParam(defaultValue = "20") int limit) {
    return queries.history(actor(jwt), id, before, limit);
  }

  @GetMapping("/submissions/{id}/comparison")
  public JsonNode comparison(@AuthenticationPrincipal Jwt jwt, @PathVariable UUID id) {
    return queries.comparison(actor(jwt), id);
  }

  @PostMapping("/submissions/{id}/versions")
  public ResponseEntity<JsonNode> correct(
      @AuthenticationPrincipal Jwt jwt,
      @PathVariable UUID id,
      @RequestHeader(value = "If-Match", required = false) String etag,
      @RequestHeader(value = "Idempotency-Key", required = false) String key,
      @RequestBody JsonNode body) {
    return response(corrections.correct(actor(jwt), id, expectedVersion(etag), key, body));
  }

  @GetMapping("/location-suggestions")
  public JsonNode suggestions(@RequestParam String city, @RequestParam String addressLine) {
    return queries.suggestions(city, addressLine);
  }
}
