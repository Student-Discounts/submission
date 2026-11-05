package org.studentdiscounts.submission.smoke;

import com.nimbusds.jose.JOSEObjectType;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.jwk.gen.RSAKeyGenerator;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.studentdiscounts.submission.domain.PayloadJson;
import org.studentdiscounts.submission.domain.SubmissionType;
import org.studentdiscounts.submission.submission.PayloadValidation;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;

/**
 * Actual executable-jar smoke test. All non-Submission business actors below are explicit fixtures.
 */
public final class SubmissionSmoke implements AutoCloseable {
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final String APP = "http://127.0.0.1:18081";
  private static final String WORKFLOW = "submission.workflow.v1";
  private static final String EVENTS = "submission.events.v1";
  private static final String DLT = WORKFLOW + ".DLT";
  private static final Duration DEADLINE = Duration.ofSeconds(30);
  private final HttpClient http =
      HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
  private final String run = UUID.randomUUID().toString();
  private final String schema = "smoke_" + run.replace("-", "");
  private final String group = "submission-smoke-" + run;
  private final String category = "smoke-" + run;
  private final String stubs = env("SD_SMOKE_STUBS_URL", "http://localhost:8090");
  private final String broker = env("SD_SUBMISSION_KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
  private final String db =
      env("SD_SUBMISSION_DB_URL", "jdbc:postgresql://localhost:5433/submission");
  private final String dbUser = env("SD_SUBMISSION_DB_USER", "submission");
  private final String dbPassword = env("SD_SUBMISSION_DB_PASSWORD", "submission");
  private final List<UUID> mappings = new ArrayList<>();
  private final List<JsonNode> outgoing = new ArrayList<>();
  private final List<JsonNode> deadLetters = new ArrayList<>();
  private final Map<String, List<UUID>> catalogAllocations = new LinkedHashMap<>();
  private final Map<String, CatalogConfirmation> catalogTriggers = new LinkedHashMap<>();
  private final String mode;
  private Process child;
  private Path log;
  private HttpServer jwks;
  private RSAKey key;
  private String issuer;
  private String jwkUri;
  private Actor student;
  private Actor other;
  private Actor moderator;
  private boolean schemaCreated;
  private KafkaProducer<String, String> producer;
  private KafkaConsumer<String, String> consumer;

  private record Actor(UUID id, List<String> roles, String accessToken) {}

  private record Reply(int status, JsonNode body, String etag, String contentType) {}

  private SubmissionSmoke(String mode) {
    this.mode = mode;
  }

  public static void main(String[] args) throws Exception {
    String mode = args.length == 0 ? "contract" : args[0];
    require(
        args.length <= 1 && Set.of("contract", "auth").contains(mode),
        "Usage: gradle smoke --args=contract|auth");
    try (SubmissionSmoke smoke = new SubmissionSmoke(mode)) {
      smoke.prepare();
      smoke.run();
      System.out.println(
          "PASS smoke "
              + mode
              + ": actual executable jar, PostgreSQL, Kafka and explicitly labeled business"
              + " fixtures; owned resources cleaned on exit");
    }
  }

  private void prepare() throws Exception {
    refuseOccupied(18081);
    if (mode.equals("contract")) {
      refuseOccupied(18090);
      key = new RSAKeyGenerator(2048).keyID(run).generate();
      issuer = "http://localhost:18090/" + run;
      jwkUri = "http://localhost:18090/.well-known/jwks.json";
      byte[] document = new JWKSet(key.toPublicJWK()).toString().getBytes(StandardCharsets.UTF_8);
      jwks = HttpServer.create(new InetSocketAddress("127.0.0.1", 18090), 0);
      jwks.createContext(
          "/.well-known/jwks.json",
          exchange -> {
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, document.length);
            try (var body = exchange.getResponseBody()) {
              body.write(document);
            }
          });
      jwks.start();
      student = new Actor(UUID.randomUUID(), List.of("STUDENT"), null);
      other = new Actor(UUID.randomUUID(), List.of("STUDENT"), null);
      moderator = new Actor(UUID.randomUUID(), List.of("MODERATOR"), null);
      System.out.println(
          "AUTH FIXTURE: fresh test-only RS256 JWKS, exact auth sub/sid/roles/aud contract; not the"
              + " real auth service");
    } else {
      String auth = env("SD_AUTH_BASE_URL", "http://localhost:8080");
      issuer = env("SD_SUBMISSION_AUTH_ISSUER", "http://localhost:8080");
      jwkUri = auth + "/.well-known/jwks.json";
      external("GET", jwkUri, null, 200);
      student = realLogin(auth, "SD_SMOKE_STUDENT_EMAIL", "SD_SMOKE_STUDENT_PASSWORD");
      other = realLogin(auth, "SD_SMOKE_OTHER_EMAIL", "SD_SMOKE_OTHER_PASSWORD");
      require(!student.id.equals(other.id), "Real auth accounts must be distinct");
      System.out.println(
          "AUTH REAL: register/login endpoints only; no role administration, database access, or"
              + " fixture-key fallback");
    }
    try (Connection c = connection(false);
        Statement s = c.createStatement()) {
      s.execute("CREATE SCHEMA " + schema);
      schemaCreated = true;
    }
    try (AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", broker))) {
      Set<String> existing = admin.listTopics().names().get(10, TimeUnit.SECONDS);
      List<NewTopic> missing = new ArrayList<>();
      for (String topic : List.of(WORKFLOW, EVENTS, DLT))
        if (!existing.contains(topic)) missing.add(new NewTopic(topic, 1, (short) 1));
      if (!missing.isEmpty()) admin.createTopics(missing).all().get(10, TimeUnit.SECONDS);
    }
    Properties pp = new Properties();
    pp.put("bootstrap.servers", broker);
    pp.put("key.serializer", StringSerializer.class.getName());
    pp.put("value.serializer", StringSerializer.class.getName());
    pp.put("acks", "all");
    pp.put("enable.idempotence", "true");
    pp.put("delivery.timeout.ms", "10000");
    pp.put("request.timeout.ms", "5000");
    pp.put("max.block.ms", "10000");
    producer = new KafkaProducer<>(pp);
    Properties cp = new Properties();
    cp.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
    cp.put(ConsumerConfig.GROUP_ID_CONFIG, group + "-observer");
    cp.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    cp.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    cp.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    consumer = new KafkaConsumer<>(cp);
    List<TopicPartition> partitions =
        List.of(new TopicPartition(EVENTS, 0), new TopicPartition(DLT, 0));
    consumer.assign(partitions);
    consumer.seekToEnd(partitions);
    for (TopicPartition p : partitions) consumer.position(p, Duration.ofSeconds(10));
    mapping(
        "GET",
        "/catalog/v1/categories/" + category,
        null,
        200,
        obj("code", category, "isActive", true));
    mapping(
        "GET",
        "/geo/v1/timezone",
        obj("lat", "59.9571", "lon", "30.3083"),
        200,
        obj("timezone", "Europe/Moscow"));
    start(false);
  }

  private void run() throws Exception {
    System.out.println(
        "BUSINESS FIXTURES: explicit Moderation human-decision events, Media metadata/revision"
            + " events, Catalog read/confirmation contracts, Geo/timezone and organization"
            + " responses; none are production business services");
    // Relay-disabled launch proves durable committed work before any outgoing event can trigger
    // Catalog.
    Reply empty = create(student, "NEW_PLACE", null, null, obj());
    String id = text(empty.body, "id");
    require(UUID.fromString(id).version() == 7, "Submission identifier must be UUIDv7");
    require(
        text(empty.body, "status").equals("DRAFT") && empty.body.path("currentVersion").isNull(),
        "Incomplete create is versionless DRAFT");
    require(get(student, id + "/versions").body.path("items").isEmpty(), "Draft has no versions");
    Reply invalid = request(student, "POST", route(id, "submit"), null, empty.etag, unique(), 422);
    require(
        invalid.contentType.startsWith("application/problem+json"),
        "Validation is RFC9457 problem JSON");
    require(
        text(invalid.body, "title").equals("Заявка не прошла проверку"),
        "Aggregated validation title");
    Set<String> missing = new LinkedHashSet<>();
    invalid.body.path("errors").forEach(e -> missing.add(text(e, "field")));
    require(
        missing.containsAll(
            List.of(
                "place.name", "place.city", "place.addressLine", "place.categoryCode", "offers")),
        "All missing fields aggregated: " + missing);
    require(
        sqlLong("SELECT count(*) FROM submission_versions WHERE submission_id='" + id + "'") == 0,
        "Invalid submit created no version");
    require(
        sqlLong(
                "SELECT count(*) FROM outbox_events WHERE submission_id='"
                    + id
                    + "' AND event_type='SubmissionSubmitted'")
            == 0,
        "Invalid submit created no submit event");
    Reply full = patch(student, id, empty.etag, payload(true, 2), 200);
    String originalEtag = full.etag;
    String submitKey = unique();
    Reply sent = request(student, "POST", route(id, "submit"), null, originalEtag, submitKey, 202);
    require(sent.body.path("versionNo").asInt() == 1, "First freeze creates version 1");
    require(
        UUID.fromString(text(sent.body, "versionId")).version() == 7,
        "Version identifier is UUIDv7");
    require(
        text(sent.body, "contentSha256").matches("[0-9a-f]{64}"),
        "Frozen SHA256 canonical encoding");
    String stableEvent =
        sqlText(
            "SELECT id::text FROM outbox_events WHERE submission_id='"
                + id
                + "' AND event_type='SubmissionSubmitted'");
    require(
        sqlLong(
                "SELECT count(*) FROM outbox_events WHERE submission_id='"
                    + id
                    + "' AND published_at IS NOT NULL")
            == 0,
        "Disabled relay leaves durable pending outbox");
    quiet(Duration.ofSeconds(2));
    require(
        events(id, "SubmissionSubmitted").isEmpty(), "Disabled relay emitted no submission event");
    stop();
    start(true);
    await(
        "pending outbox delivers stable eventId after restart",
        () -> {
          pump();
          return events(id, "SubmissionSubmitted").stream()
              .anyMatch(e -> text(e, "eventId").equals(stableEvent));
        });
    Reply review = awaitStatus(id, "PENDING_REVIEW");
    require(
        events(id, "SubmissionStatusChanged").stream()
            .noneMatch(
                e -> Set.of("APPROVED", "PUBLISHED").contains(text(e.path("data"), "toStatus"))),
        "No automatic approval or publication");
    checkCrossUser(id);
    concurrentReplay(id, originalEtag, submitKey, sent);
    request(student, "POST", route(id, "submit"), null, "\"999999\"", submitKey, 409);
    Reply existing =
        request(student, "POST", route(id, "submit"), null, review.etag, unique(), 200);
    require(
        text(existing.body, "versionId").equals(text(sent.body, "versionId")),
        "Different-key repeat reuses frozen version");
    require(
        sqlLong("SELECT count(*) FROM submission_versions WHERE submission_id='" + id + "'") == 1,
        "Concurrent retries create one version");
    require(
        sqlLong(
                "SELECT count(*) FROM outbox_events WHERE submission_id='"
                    + id
                    + "' AND event_type='SubmissionSubmitted'")
            == 1,
        "Concurrent retries create one submit event");
    if (mode.equals("auth")) {
      Reply otherDraft = create(other, "NEW_PLACE", null, null, payload(true, 1));
      String otherId = text(otherDraft.body, "id");
      request(other, "POST", route(otherId, "submit"), null, otherDraft.etag, unique(), 202);
      require(
          text(get(other, otherId).body, "authorId").equals(other.id.toString()),
          "Second real subject owns its submitted draft");
      request(student, "GET", route(otherId, ""), null, null, null, 404);
      authFixtureLifecycle(id);
      restartDurability(id, "PUBLISHED");
      return;
    }
    boundaryRules();
    stalePatchRace();
    reviewCycles(id, sent);
    missingCoordinatesAndGeoFailure();
    scheduledValidationFairness();
    mediaScenarios();
    changeScenario();
    publicationScenario(id);
    restartDurability(id, "PUBLISHED");
    authFailures(id);
    System.out.println(
        "PASS contract scenarios: validation, idempotency/concurrency, review/corrections, media"
            + " revisions/outage, CHANGE conflicts, committed-APPROVED publication, DLT, restart"
            + " durability, scheduled validation fairness and Clock boundaries");
  }

  private void concurrentReplay(String id, String etag, String key, Reply original)
      throws Exception {
    try (var executor = Executors.newFixedThreadPool(2)) {
      Callable<Reply> replay =
          () -> request(student, "POST", route(id, "submit"), null, etag, key, 202);
      var a = executor.submit(replay);
      var b = executor.submit(replay);
      for (Reply r : List.of(a.get(10, TimeUnit.SECONDS), b.get(10, TimeUnit.SECONDS))) {
        require(
            r.body.equals(original.body) && r.etag.equals(original.etag),
            "Concurrent original-key replay is exact original response/ETag");
      }
    }
  }

  private void stalePatchRace() throws Exception {
    Reply draft = create(student, "NEW_PLACE", null, null, obj());
    String id = text(draft.body, "id");
    try (var executor = Executors.newFixedThreadPool(2)) {
      var a =
          executor.submit(() ->
              request(
                  student,
                  "PATCH",
                  route(id, ""),
                  obj("place", obj("name", "A")),
                  draft.etag,
                  null));
      var b =
          executor.submit(() ->
              request(
                  student,
                  "PATCH",
                  route(id, ""),
                  obj("place", obj("name", "B")),
                  draft.etag,
                  null));
      List<Integer> codes =
          List.of(a.get(10, TimeUnit.SECONDS).status, b.get(10, TimeUnit.SECONDS).status);
      require(
          codes.contains(200) && codes.contains(412),
          "Shared-ETag patch race has one winner/one stale: " + codes);
    }
  }

  private void reviewCycles(String id, Reply firstSubmit) throws Exception {
    JsonNode immutable = get(student, id + "/versions/1").body;
    Reply state = awaitStatus(id, "PENDING_REVIEW");
    emit("ReviewStarted", "moderation", tuple(state.body, obj("actorId", moderator.id)));
    state = awaitStatus(id, "IN_REVIEW");
    request(
        student,
        "POST",
        route(id, "versions"),
        obj(
            "reviewedVersionId",
            version(state.body).path("id"),
            "payload",
            version(state.body).path("payload")),
        state.etag,
        unique(),
        403);
    emit(
        "SubmissionChangesRequested",
        "moderation",
        tuple(state.body, obj("actorId", moderator.id, "reason", "Уточните условия")));
    state = awaitStatus(id, "CHANGES_REQUESTED");
    Reply unchanged =
        request(student, "POST", route(id, "submit"), null, state.etag, unique(), 202);
    require(
        text(unchanged.body, "versionId").equals(text(firstSubmit.body, "versionId")),
        "Unchanged requested-changes cycle reuses version");
    awaitStatus(id, "PENDING_REVIEW");
    require(
        sqlLong(
                "SELECT count(*) FROM outbox_events WHERE submission_id='"
                    + id
                    + "' AND event_type='SubmissionSubmitted'")
            == 2,
        "Unchanged requested-changes cycle emits new review-cycle event");
    state = get(student, id);
    emit("ReviewStarted", "moderation", tuple(state.body, obj("actorId", moderator.id)));
    state = awaitStatus(id, "IN_REVIEW");
    emit(
        "SubmissionChangesRequested",
        "moderation",
        tuple(state.body, obj("actorId", moderator.id, "reason", "Добавьте пояснение")));
    state = awaitStatus(id, "CHANGES_REQUESTED");
    ObjectNode changed = (ObjectNode) version(state.body).path("payload").deepCopy();
    ((ObjectNode) changed.path("offers").get(0))
        .put("conditions", "Студенческий обед за 500 рублей, по студенческому билету");
    state = patch(student, id, state.etag, changed, 200);
    request(student, "POST", route(id, "submit"), null, state.etag, unique(), 202);
    state = awaitStatus(id, "PENDING_REVIEW");
    require(
        version(state.body).path("versionNo").asInt() == 2, "Edited resubmit creates version 2");
    require(
        text(version(state.body), "previousVersionId").equals(text(immutable, "id")),
        "Version chain predecessor points to version 1");
    require(
        get(student, id + "/versions/1").body.equals(immutable),
        "Version 1 and original hash are immutable");
    Map<String, Object> stale =
        tuple(
            state.body,
            obj("actorId", moderator.id, "payload", version(state.body).path("payload")));
    stale.put("versionId", text(firstSubmit.body, "versionId"));
    stale.put("versionNo", 1);
    assertDltNoChange("SubmissionApproved", "moderation", stale, id);
    emit("ReviewStarted", "moderation", tuple(state.body, obj("actorId", moderator.id)));
    state = awaitStatus(id, "IN_REVIEW");
    assertDltNoChange(
        "SubmissionApproved",
        "moderation",
        tuple(
            state.body, obj("actorId", student.id, "payload", version(state.body).path("payload"))),
        id);
    Map<String, Object> oldCycle =
        tuple(
            state.body,
            obj("actorId", moderator.id, "payload", version(state.body).path("payload")));
    oldCycle.put("expectedRowVersion", firstSubmit.body.path("rowVersion").asLong());
    assertDltNoChange("SubmissionApproved", "moderation", oldCycle, id);
    ObjectNode corrected = (ObjectNode) version(state.body).path("payload").deepCopy();
    ((ObjectNode) corrected.path("offers").get(1))
        .put("conditions", "Ужин за 600 рублей, проверено модератором");
    request(
        moderator,
        "POST",
        route(id, "versions"),
        obj("reviewedVersionId", version(state.body).path("id"), "payload", corrected),
        state.etag,
        unique(),
        201);
    state = get(student, id);
    require(
        version(state.body).path("versionNo").asInt() == 3,
        "Moderator correction creates third immutable version");
    require(
        text(version(state.body), "createdBy").equals(moderator.id.toString()),
        "Corrected version records actual moderator");
  }

  private void missingCoordinatesAndGeoFailure() throws Exception {
    JsonNode noPoint = JSON.valueToTree(payload(false, 1));
    Reply draft = create(student, "NEW_PLACE", null, null, noPoint);
    String id = text(draft.body, "id");
    request(student, "POST", route(id, "submit"), null, draft.etag, unique(), 202);
    Reply state = awaitStatus(id, "PENDING_REVIEW");
    emit("ReviewStarted", "moderation", tuple(state.body, obj("actorId", moderator.id)));
    state = awaitStatus(id, "IN_REVIEW");
    assertDltNoChange(
        "SubmissionApproved",
        "moderation",
        tuple(
            state.body,
            obj("actorId", moderator.id, "payload", version(state.body).path("payload"))),
        id);
    ObjectNode corrected = (ObjectNode) version(state.body).path("payload").deepCopy();
    ((ObjectNode) corrected.path("place"))
        .set("location", JSON.valueToTree(obj("lat", 59.9571, "lon", 30.3083)));
    request(
        moderator,
        "POST",
        route(id, "versions"),
        obj("reviewedVersionId", version(state.body).path("id"), "payload", corrected),
        state.etag,
        unique(),
        201);
    state = get(student, id);
    require(
        version(state.body).path("versionNo").asInt() == 2
            && text(version(state.body), "resolvedTimezone").equals("Europe/Moscow"),
        "Moderator coordinate correction freezes resolved zone");
    ObjectNode outage = (ObjectNode) JSON.valueToTree(payload(true, 1));
    ((ObjectNode) outage.path("place"))
        .set("location", JSON.valueToTree(obj("lat", 51.5007, "lon", -0.1246)));
    ((ObjectNode) outage.path("offers").get(0)).remove("validUntil");
    UUID mapping =
        mapping(
            "GET",
            "/geo/v1/timezone",
            obj("lat", "51.5007", "lon", "-0.1246"),
            503,
            obj("error", "fixture Geo outage"));
    draft = create(student, "NEW_PLACE", null, null, outage);
    String failedId = text(draft.body, "id");
    request(student, "POST", route(failedId, "submit"), null, draft.etag, unique(), 503);
    require(
        get(student, failedId).body.path("currentVersion").isNull(),
        "Timezone outage without dates rolls back version atomically");
    deleteMapping(mapping);
    String city = "NoMatch-" + run;
    mapping(
        "GET",
        "/geo/v1/suggestions",
        obj("city", city, "addressLine", "Unknown"),
        200,
        obj("candidates", List.of()));
    Reply suggestions =
        request(
            student,
            "GET",
            "/api/v1/location-suggestions?city=" + encode(city) + "&addressLine=Unknown",
            null,
            null,
            null,
            200);
    require(
        suggestions.body.path("candidates").isEmpty(),
        "No-match geocoder returns no invented coordinates");
  }

  private void scheduledValidationFairness() throws Exception {
    UUID firstAsset = UUID.randomUUID();
    UUID secondAsset = UUID.randomUUID();
    media(firstAsset, student.id, "QUARANTINED", 1, 200);
    media(secondAsset, student.id, "QUARANTINED", 1, 200);
    String firstHead = mediaSubmission(firstAsset, true);
    awaitStatus(firstHead, "VALIDATING");
    String secondHead = mediaSubmission(secondAsset, true);
    awaitStatus(secondHead, "VALIDATING");

    // Both earlier UUIDs remain pending; only the actual scheduled worker can advance the tail.
    Reply readyDraft = create(student, "NEW_PLACE", null, null, payload(true, 1));
    String readyId = text(readyDraft.body, "id");
    request(student, "POST", route(readyId, "submit"), null, readyDraft.etag, unique(), 202);
    awaitStatus(readyId, "PENDING_REVIEW");
    require(
        text(get(student, firstHead).body, "status").equals("VALIDATING")
            && text(get(student, secondHead).body, "status").equals("VALIDATING"),
        "Batch-size2 scheduler advances the third submission while both quarantined heads remain"
            + " VALIDATING");
    System.out.println(
        "PASS scheduled fairness: validation-batch-size=2, two quarantined heads remain VALIDATING,"
            + " third submission reaches PENDING_REVIEW");
  }

  private void mediaScenarios() throws Exception {
    UUID asset = UUID.randomUUID();
    UUID mediaMapping = media(asset, student.id, "QUARANTINED", 1, 200);
    String pending = mediaSubmission(asset, true);
    Reply validating = awaitStatus(pending, "VALIDATING");
    quiet(Duration.ofSeconds(1));
    require(
        text(get(student, pending).body, "status").equals("VALIDATING"),
        "Quarantined proof-only asset holds validation");
    emitAsset(asset, "CLEAN", 2);
    awaitStatus(pending, "PENDING_REVIEW");
    emitAsset(asset, "REJECTED", 1);
    quiet(Duration.ofSeconds(1));
    require(
        text(get(student, pending).body, "status").equals("PENDING_REVIEW"),
        "Stale media revision is no-op");
    UUID rejected = UUID.randomUUID();
    media(rejected, student.id, "REJECTED", 1, 200);
    String rejectedId = mediaSubmission(rejected, false);
    Reply rejection = awaitStatus(rejectedId, "REJECTED");
    require(
        text(rejection.body, "latestStatusReason").contains("MEDIA_REJECTED"),
        "Rejected file has visible safe reason even with source");
    UUID foreign = UUID.randomUUID();
    UUID foreignMapping = media(foreign, student.id, "CLEAN", 1, 200);
    Reply draft = create(student, "NEW_PLACE", null, null, attachedPayload(foreign, false));
    String foreignId = text(draft.body, "id");
    deleteMapping(foreignMapping);
    media(foreign, other.id, "CLEAN", 2, 200);
    request(student, "POST", route(foreignId, "submit"), null, draft.etag, unique(), 403);
    require(
        get(student, foreignId).body.path("currentVersion").isNull(),
        "Foreign attachment denied before version creation");
    UUID down = UUID.randomUUID();
    UUID downMapping = media(down, student.id, "QUARANTINED", 1, 200);
    String downId = mediaSubmission(down, true);
    awaitStatus(downId, "VALIDATING");
    deleteMapping(downMapping);
    media(down, student.id, "QUARANTINED", 1, 503);
    quiet(Duration.ofSeconds(6));
    require(
        text(get(student, downId).body, "status").equals("VALIDATING"),
        "Media outage preserves pending validation, never defaults CLEAN");
    deleteMapping(mediaMapping);
  }

  private void changeScenario() throws Exception {
    UUID place = UUID.randomUUID();
    UUID offer = UUID.randomUUID();
    ObjectNode baseline = (ObjectNode) JSON.valueToTree(payload(true, 1));
    ((ObjectNode) baseline.path("place")).put("id", place.toString());
    ((ObjectNode) baseline.path("offers").get(0)).put("id", offer.toString());
    mapping(
        "GET",
        "/catalog/v1/places/" + place,
        null,
        200,
        obj(
            "id",
            place,
            "name",
            "Кофейня «Зерно»",
            "city",
            "Санкт-Петербург",
            "addressLine",
            "Кронверкский пр., 49",
            "location",
            obj("lat", 59.9571, "lon", 30.3083),
            "categoryCode",
            category,
            "timezone",
            "Europe/Moscow",
            "rowVersion",
            7));
    UUID offerMapping = catalogOffer(offer, place, 7, baseline);
    Reply draft = create(student, "CHANGE", place, offer, obj());
    String id = text(draft.body, "id");
    require(
        text(draft.body.path("draftPayload").path("place"), "addressLine")
            .equals("Кронверкский пр., 49"),
        "CHANGE seeds complete public address");
    Reply patched =
        patch(
            student,
            id,
            draft.etag,
            obj("offers", List.of(mergeOffer(baseline.path("offers").get(0), "Уточнение автора"))),
            200);
    require(
        text(
                get(student, id + "/comparison").body.path("publicSnapshot").path("offers").get(0),
                "conditions")
            .equals(text(baseline.path("offers").get(0), "conditions")),
        "Pending edit did not mutate public fixture");
    request(student, "POST", route(id, "submit"), null, patched.etag, unique(), 202);
    Reply state = awaitStatus(id, "PENDING_REVIEW");
    emit("ReviewStarted", "moderation", tuple(state.body, obj("actorId", moderator.id)));
    state = awaitStatus(id, "IN_REVIEW");
    require(
        get(moderator, id + "/comparison").body.path("catalogRowVersion").asInt() == 7,
        "Moderator explicitly observes Catalog revision 7");
    deleteMapping(offerMapping);
    offerMapping = catalogOffer(offer, place, 8, baseline);
    Map<String, Object> correction =
        obj(
            "reviewedVersionId",
            version(state.body).path("id"),
            "payload",
            version(state.body).path("payload"),
            "expectedCatalogRowVersion",
            7);
    request(moderator, "POST", route(id, "versions"), correction, state.etag, unique(), 409);
    require(
        version(get(student, id).body).path("versionNo").asInt() == 1,
        "Stale explicit catalog base created no version");
    assertDltNoChange(
        "SubmissionApproved",
        "moderation",
        tuple(
            state.body,
            obj("actorId", moderator.id, "payload", version(state.body).path("payload"))),
        id);
    require(
        get(moderator, id + "/comparison").body.path("catalogRowVersion").asInt() == 8,
        "Moderator rereads current Catalog revision 8");
    correction.put("expectedCatalogRowVersion", 8);
    request(moderator, "POST", route(id, "versions"), correction, state.etag, unique(), 201);
    state = get(student, id);
    require(
        version(state.body).path("versionNo").asInt() == 2
            && version(state.body).path("baseOfferRowVersion").asInt() == 8,
        "Explicit revised base creates new version even if payload unchanged");
    emit(
        "SubmissionApproved",
        "moderation",
        tuple(
            state.body,
            obj("actorId", moderator.id, "payload", version(state.body).path("payload"))));
    state = awaitStatus(id, "APPROVED");
    emit(
        "OfferPublicationConflicted",
        "catalog",
        tuple(state.body, obj("reason", "CATALOG_VERSION_CONFLICT")));
    state = awaitStatus(id, "PENDING_REVIEW");
    Map<String, Object> old =
        tuple(
            state.body,
            obj("actorId", moderator.id, "payload", version(state.body).path("payload")));
    old.put("versionId", text(get(student, id + "/versions/1").body, "id"));
    old.put("versionNo", 1);
    assertDltNoChange("SubmissionApproved", "moderation", old, id);
    deleteMapping(offerMapping);
    catalogOffer(offer, place, 9, baseline);
    emit("ReviewStarted", "moderation", tuple(state.body, obj("actorId", moderator.id)));
    state = awaitStatus(id, "IN_REVIEW");
    assertDltNoChange(
        "SubmissionApproved",
        "moderation",
        tuple(
            state.body,
            obj("actorId", moderator.id, "payload", version(state.body).path("payload"))),
        id);
  }

  private void publicationScenario(String id) throws Exception {
    Reply state = get(student, id);
    require(
        text(state.body, "status").equals("IN_REVIEW"),
        "Main submission remains in review before explicit approval");
    assertDltNoChange(
        "OfferPublished",
        "catalog",
        tuple(state.body, obj("offerIds", List.of(UUID.randomUUID(), UUID.randomUUID()))),
        id);
    Map<String, Object> approval =
        tuple(
            state.body,
            obj("actorId", moderator.id, "payload", version(state.body).path("payload")));
    String completedApproval = emit("SubmissionApproved", "moderation", approval);
    Reply approved = awaitStatus(id, "APPROVED");
    quiet(Duration.ofSeconds(1));
    require(
        text(get(student, id).body, "status").equals("APPROVED"),
        "Approval is not publication and remains APPROVED without confirmation");
    CatalogConfirmation confirmation = committedCatalogConfirmation(id, approved);
    require(
        committedCatalogConfirmation(id, approved).data.equals(confirmation.data),
        "Catalog trigger replay reuses committed allocation");
    String confirmationEvent = emit("OfferPublished", "catalog", confirmation.data);
    Reply published = awaitStatus(id, "PUBLISHED");
    require(
        published.body.path("publishedOfferIds").size() == 2,
        "One distinct public offer ID per submitted offer");
    require(
        !published
            .body
            .path("publishedOfferIds")
            .get(0)
            .equals(published.body.path("publishedOfferIds").get(1)),
        "Catalog IDs distinct");
    long history =
        sqlLong("SELECT count(*) FROM submission_status_history WHERE submission_id='" + id + "'");
    emit("OfferPublished", "catalog", confirmation.data);
    emitEnvelope(confirmationEvent, "OfferPublished", "catalog", confirmation.data);
    emitEnvelope(completedApproval, "SubmissionApproved", "moderation", approval);
    quiet(Duration.ofSeconds(2));
    Reply repeated = get(student, id);
    require(
        repeated.body.path("publishedOfferIds").equals(published.body.path("publishedOfferIds")),
        "Duplicate confirmation preserves committed offer IDs");
    require(
        sqlLong("SELECT count(*) FROM submission_status_history WHERE submission_id='" + id + "'")
            == history,
        "Duplicate and completed-event replay add no history");
    Map<String, Object> conflicting = new LinkedHashMap<>(confirmation.data);
    conflicting.put("offerIds", List.of(UUID.randomUUID(), UUID.randomUUID()));
    assertDltNoChange("OfferPublished", "catalog", conflicting, id);
    emit(
        "SubmissionSuspended",
        "catalog",
        tuple(repeated.body, obj("reason", "fixture temporary suspension")));
    Reply suspended = awaitStatus(id, "SUSPENDED");
    emitEnvelope(confirmationEvent, "OfferPublished", "catalog", confirmation.data);
    quiet(Duration.ofSeconds(1));
    require(
        text(get(student, id).body, "status").equals("SUSPENDED"),
        "Completed publication replay after later transition is no-op");
    emit(
        "SubmissionReviewReopened",
        "moderation",
        tuple(suspended.body, obj("actorId", moderator.id, "reason", "fixture recheck")));
    state = awaitStatus(id, "PENDING_REVIEW");
    emit("ReviewStarted", "moderation", tuple(state.body, obj("actorId", moderator.id)));
    state = awaitStatus(id, "IN_REVIEW");
    emit(
        "SubmissionApproved",
        "moderation",
        tuple(
            state.body,
            obj("actorId", moderator.id, "payload", version(state.body).path("payload"))));
    approved = awaitStatus(id, "APPROVED");
    // The simulated Catalog reuses its already-allocated IDs for this immutable version.
    CatalogConfirmation again = committedCatalogConfirmation(id, approved);
    require(
        again.data.get("offerIds").equals(confirmation.data.get("offerIds")),
        "Catalog reuses allocated IDs for same immutable version");
    emit("OfferPublished", "catalog", again.data);
    awaitStatus(id, "PUBLISHED");
  }

  private record CatalogConfirmation(Map<String, Object> data) {}

  private CatalogConfirmation committedCatalogConfirmation(String id, Reply approved)
      throws Exception {
    // Never called from the raw incoming moderation approval. Only committed outgoing state is a
    // trigger.
    await(
        "Catalog fixture sees committed APPROVED event and preceding immutable version",
        () -> {
          pump();
          return events(id, "SubmissionStatusChanged").stream()
                  .anyMatch(e ->
                      text(e.path("data"), "toStatus").equals("APPROVED")
                          && e.path("data").path("rowVersion").asLong()
                              == approved.body.path("rowVersion").asLong())
              && outgoing.stream()
                  .anyMatch(e ->
                      Set.of("SubmissionSubmitted", "SubmissionVersionCreated")
                              .contains(text(e, "eventType"))
                          && text(e.path("data"), "versionId")
                              .equals(text(version(approved.body), "id")));
        });
    JsonNode committed =
        events(id, "SubmissionStatusChanged").stream()
            .filter(e ->
                text(e.path("data"), "toStatus").equals("APPROVED")
                    && e.path("data").path("rowVersion").asLong()
                        == approved.body.path("rowVersion").asLong())
            .findFirst()
            .orElseThrow();
    JsonNode snapshot =
        outgoing.stream()
            .filter(e ->
                Set.of("SubmissionSubmitted", "SubmissionVersionCreated")
                        .contains(text(e, "eventType"))
                    && text(e.path("data"), "versionId").equals(text(version(approved.body), "id")))
            .findFirst()
            .orElseThrow();
    require(
        snapshot.path("data").path("payload").equals(version(approved.body).path("payload")),
        "Catalog correlates exact immutable outgoing snapshot");
    require(
        committed.path("aggregateSequence").asLong() > snapshot.path("aggregateSequence").asLong(),
        "Committed approval follows immutable snapshot event sequence");
    String triggerId = text(committed, "eventId");
    CatalogConfirmation replay = catalogTriggers.get(triggerId);
    if (replay != null) return replay;
    List<UUID> ids =
        catalogAllocations.computeIfAbsent(
            text(version(approved.body), "id"),
            ignored -> {
              List<UUID> allocated = new ArrayList<>();
              for (int i = 0; i < snapshot.path("data").path("payload").path("offers").size(); i++)
                allocated.add(UUID.randomUUID());
              return List.copyOf(allocated);
            });
    System.out.println(
        "CATALOG FIXTURE: consuming committed SubmissionStatusChanged(APPROVED), reusing "
            + ids.size()
            + " simulated offer IDs; no actual public Catalog");
    CatalogConfirmation confirmation =
        new CatalogConfirmation(tuple(approved.body, obj("offerIds", ids)));
    catalogTriggers.put(triggerId, confirmation);
    return confirmation;
  }

  private void authFixtureLifecycle(String id) throws Exception {
    UUID human = UUID.randomUUID();
    Reply state = get(student, id);
    emit("ReviewStarted", "moderation", tuple(state.body, obj("actorId", human)));
    state = awaitStatus(id, "IN_REVIEW");
    emit(
        "SubmissionApproved",
        "moderation",
        tuple(state.body, obj("actorId", human, "payload", version(state.body).path("payload"))));
    Reply approved = awaitStatus(id, "APPROVED");
    CatalogConfirmation confirmation = committedCatalogConfirmation(id, approved);
    emit("OfferPublished", "catalog", confirmation.data);
    awaitStatus(id, "PUBLISHED");
  }

  private void restartDurability(String id, String status) throws Exception {
    Reply before = get(student, id);
    JsonNode history = get(student, id + "/history").body;
    stop();
    start(true);
    Reply after = get(student, id);
    require(
        text(after.body, "status").equals(status), "Status persists across actual process restart");
    require(
        after.body.path("currentVersion").equals(before.body.path("currentVersion")),
        "Immutable current version survives process restart");
    require(
        get(student, id + "/history").body.equals(history),
        "Status history survives process restart");
    checkCrossUser(id);
  }

  private void authFailures(String id) throws Exception {
    requestToken(
        signed(student, Instant.now().minusSeconds(600), Instant.now().minusSeconds(120)),
        "GET",
        route(id, ""),
        null,
        null,
        null,
        401);
    String token = token(student);
    String[] parts = token.split("\\.");
    String signature = (parts[2].charAt(0) == 'A' ? "B" : "A") + parts[2].substring(1);
    requestToken(
        parts[0] + "." + parts[1] + "." + signature, "GET", route(id, ""), null, null, null, 401);
  }

  private void boundaryRules() throws Exception {
    // Pure rules have their Clock injected, while every workflow above/below uses actual
    // HTTP/Kafka.
    Clock fixed = Clock.fixed(Instant.parse("2026-11-05T21:30:00Z"), ZoneOffset.UTC);
    PayloadValidation validator = new PayloadValidation(fixed);
    ObjectNode today = (ObjectNode) JSON.valueToTree(payload(true, 1));
    ((ObjectNode) today.path("offers").get(0)).put("validUntil", "2026-11-06");
    require(
        validator
            .validate(today, SubmissionType.NEW_PLACE, true, ZoneId.of("Europe/Moscow"))
            .isEmpty(),
        "Moscow local today accepted despite different UTC date");
    ((ObjectNode) today.path("offers").get(0)).put("validUntil", "2026-11-05");
    require(
        validator
            .validate(today, SubmissionType.NEW_PLACE, true, ZoneId.of("Europe/Moscow"))
            .stream()
            .anyMatch(e -> e.code().equals("DATE_RANGE_INVALID")),
        "Moscow yesterday rejected at local midnight");
    require(
        validator.validate(today, SubmissionType.NEW_PLACE, true, ZoneOffset.UTC).isEmpty(),
        "Same date still valid in UTC before UTC midnight");
    for (Object value :
        List.of(
            0, 101, new java.math.BigDecimal("1.001"), new java.math.BigDecimal("100000000.00"))) {
      ObjectNode p = (ObjectNode) JSON.valueToTree(payload(true, 1));
      ObjectNode offer = (ObjectNode) p.path("offers").get(0);
      offer.put("discountType", "PERCENT");
      offer.set("discountValue", JSON.valueToTree(value));
      require(
          !validator
              .validate(p, SubmissionType.NEW_PLACE, true, ZoneId.of("Europe/Moscow"))
              .isEmpty(),
          "Invalid numeric boundary rejected without rounding: " + value);
    }
    ObjectNode schedules = (ObjectNode) JSON.valueToTree(payload(true, 1));
    ((ObjectNode) schedules.path("offers").get(0))
        .set(
            "schedule",
            JSON.valueToTree(
                List.of(interval(5, "20:00", "22:00"), interval(5, "22:00", "24:00"))));
    require(
        validator
            .validate(schedules, SubmissionType.NEW_PLACE, true, ZoneId.of("Europe/Moscow"))
            .isEmpty(),
        "Adjacent intervals and end 24:00 accepted");
    for (List<Map<String, Object>> bad :
        List.of(
            List.of(interval(5, "20:00", "23:00"), interval(5, "22:00", "24:00")),
            List.of(interval(5, "23:00", "02:00")),
            List.of(interval(5, "22:00", "24:01")))) {
      ((ObjectNode) schedules.path("offers").get(0)).set("schedule", JSON.valueToTree(bad));
      require(
          !validator
              .validate(schedules, SubmissionType.NEW_PLACE, true, ZoneId.of("Europe/Moscow"))
              .isEmpty(),
          "Overlap/midnight crossing/end after24 rejected");
    }
    JsonNode decimalA =
        JSON.readTree("{\"offers\":[{\"discountValue\":10.0}],\"place\":{\"name\":\" Test \"}}");
    JsonNode decimalB =
        JSON.readTree("{\"place\":{\"name\":\"Test\"},\"offers\":[{\"discountValue\":10.00}]}");
    require(
        PayloadJson.hash(decimalA).equals(PayloadJson.hash(decimalB)),
        "Canonical hash ignores object order, decimal scale and trimmed whitespace");
  }

  private void checkCrossUser(String id) throws Exception {
    request(other, "GET", route(id, ""), null, null, null, 404);
    request(other, "GET", route(id, "versions"), null, null, null, 404);
    require(
        text(get(student, id).body, "authorId").equals(student.id.toString()),
        "Actual authenticated subject retained as author");
  }

  private String mediaSubmission(UUID asset, boolean proofOnly) throws Exception {
    Reply draft = create(student, "NEW_PLACE", null, null, attachedPayload(asset, proofOnly));
    String id = text(draft.body, "id");
    request(student, "POST", route(id, "submit"), null, draft.etag, unique(), 202);
    return id;
  }

  private JsonNode attachedPayload(UUID asset, boolean proofOnly) {
    ObjectNode p = (ObjectNode) JSON.valueToTree(payload(true, 1));
    p.set(
        "attachments",
        JSON.valueToTree(List.of(obj("mediaAssetId", asset, "purpose", "PROOF", "offerIndex", 0))));
    if (proofOnly) ((ObjectNode) p.path("offers").get(0)).remove("source");
    return p;
  }

  private UUID media(UUID id, UUID owner, String status, long revision, int httpStatus)
      throws Exception {
    return mapping(
        "GET",
        "/media/v1/assets/" + id,
        null,
        httpStatus,
        obj("id", id, "ownerId", owner, "status", status, "revision", revision));
  }

  private UUID catalogOffer(UUID offer, UUID place, int revision, JsonNode payload)
      throws Exception {
    return mapping(
        "GET",
        "/catalog/v1/offers/" + offer,
        null,
        200,
        obj(
            "id",
            offer,
            "placeId",
            place,
            "status",
            "ACTIVE",
            "rowVersion",
            revision,
            "payload",
            payload));
  }

  private static JsonNode mergeOffer(JsonNode existing, String terms) {
    ObjectNode copy = (ObjectNode) existing.deepCopy();
    copy.put("conditions", terms);
    return copy;
  }

  private Map<String, Object> payload(boolean coordinates, int offersCount) {
    Map<String, Object> place =
        obj(
            "name",
            "Кофейня «Зерно»",
            "city",
            "Санкт-Петербург",
            "addressLine",
            "Кронверкский пр., 49",
            "categoryCode",
            category);
    if (coordinates) place.put("location", obj("lat", 59.9571, "lon", 30.3083));
    List<Map<String, Object>> offers = new ArrayList<>();
    for (int i = 0; i < offersCount; i++)
      offers.add(
          obj(
              "conditions",
              i == 0 ? "Студенческий обед за 500 рублей" : "Студенческий ужин за 600 рублей",
              "discountType",
              "SPECIAL_PRICE",
              "discountValue",
              i == 0 ? 500 : 600,
              "source",
              obj("type", "WEBSITE", "url", "https://example.com/students/" + run),
              "validUntil",
              LocalDate.now(ZoneId.of("Europe/Moscow")).plusDays(30).toString(),
              "schedule",
              List.of(interval(5, "22:00", "24:00"), interval(6, "00:00", "02:00"))));
    return obj("place", place, "offers", offers, "attachments", List.of());
  }

  private static Map<String, Object> interval(int day, String from, String until) {
    return obj("dayOfWeek", day, "timeFrom", from, "timeUntil", until);
  }

  private Reply create(Actor actor, String type, UUID place, UUID offer, Object payload)
      throws Exception {
    Map<String, Object> body = obj("type", type, "payload", payload);
    if (place != null) body.put("targetPlaceId", place);
    if (offer != null) body.put("targetOfferId", offer);
    Reply reply = request(actor, "POST", "/api/v1/submissions", body, null, unique(), 201);
    return reply;
  }

  private Reply patch(Actor actor, String id, String etag, Object body, int expected)
      throws Exception {
    return request(actor, "PATCH", route(id, ""), body, etag, null, expected);
  }

  private Reply get(Actor actor, String suffix) throws Exception {
    return request(actor, "GET", "/api/v1/submissions/" + suffix, null, null, null, 200);
  }

  private Reply awaitStatus(String id, String expected) throws Exception {
    Reply[] state = new Reply[1];
    await(
        "submission " + id + " reaches " + expected,
        () -> {
          try {
            state[0] = get(student, id);
            return text(state[0].body, "status").equals(expected);
          } catch (Exception e) {
            throw new IllegalStateException(e);
          }
        });
    return state[0];
  }

  private Map<String, Object> tuple(JsonNode state, Map<String, Object> extras) {
    JsonNode v = version(state);
    Map<String, Object> data =
        obj(
            "submissionId",
            text(state, "id"),
            "versionId",
            text(v, "id"),
            "versionNo",
            v.path("versionNo").asInt(),
            "expectedRowVersion",
            state.path("rowVersion").asLong());
    data.putAll(extras);
    return data;
  }

  private static JsonNode version(JsonNode state) {
    require(state.path("currentVersion").isObject(), "Expected current immutable version");
    return state.path("currentVersion");
  }

  private void emitAsset(UUID asset, String status, long revision) throws Exception {
    Map<String, Object> envelope =
        envelope(
            unique(),
            "MediaAssetChecked",
            "media",
            obj("mediaAssetId", asset, "status", status, "revision", revision));
    producer
        .send(new ProducerRecord<>(WORKFLOW, asset.toString(), JSON.writeValueAsString(envelope)))
        .get(10, TimeUnit.SECONDS);
  }

  private String emit(String eventType, String actor, Map<String, Object> data) throws Exception {
    String id = unique();
    emitEnvelope(id, eventType, actor, data);
    return id;
  }

  private void emitEnvelope(String id, String type, String actor, Map<String, Object> data)
      throws Exception {
    producer
        .send(
            new ProducerRecord<>(
                WORKFLOW,
                data.get("submissionId").toString(),
                JSON.writeValueAsString(envelope(id, type, actor, data))))
        .get(10, TimeUnit.SECONDS);
  }

  private Map<String, Object> envelope(String id, String type, String actor, Object data) {
    return obj(
        "eventId",
        id,
        "eventType",
        type,
        "schemaVersion",
        1,
        "producer",
        actor,
        "occurredAt",
        Instant.now().toString(),
        "correlationId",
        "smoke-" + run,
        "data",
        data);
  }

  private void assertDltNoChange(
      String type, String producerName, Map<String, Object> data, String id) throws Exception {
    Reply before = get(student, id);
    String eventId = emit(type, producerName, data);
    await(
        type + " invalid fixture enters DLT",
        () -> {
          pump();
          return deadLetters.stream().anyMatch(e -> text(e, "eventId").equals(eventId));
        });
    Reply after = get(student, id);
    require(after.body.equals(before.body), "DLT event cannot mutate submission: " + type);
  }

  private void pump() {
    for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(100))) {
      JsonNode event = JSON.readTree(record.value());
      if (record.topic().equals(DLT)) deadLetters.add(event);
      else outgoing.add(event);
    }
  }

  private List<JsonNode> events(String submissionId, String eventType) {
    return outgoing.stream()
        .filter(e ->
            text(e, "eventType").equals(eventType)
                && text(e.path("data"), "submissionId").equals(submissionId))
        .toList();
  }

  private void quiet(Duration duration) throws Exception {
    long end = System.nanoTime() + duration.toNanos();
    while (System.nanoTime() < end) {
      pump();
      Thread.sleep(100);
    }
  }

  private void await(String description, BooleanSupplier condition) throws Exception {
    long end = System.nanoTime() + DEADLINE.toNanos();
    while (System.nanoTime() < end) {
      if (child != null && !child.isAlive())
        throw new AssertionError("Owned service exited (" + child.exitValue() + "), log: " + log);
      if (condition.getAsBoolean()) return;
      Thread.sleep(100);
    }
    throw new AssertionError(
        "30-second assertion deadline: " + description + "; child log: " + log);
  }

  private Reply request(
      Actor actor,
      String method,
      String path,
      Object body,
      String etag,
      String requestKey,
      int... expected)
      throws Exception {
    return requestToken(token(actor), method, path, body, etag, requestKey, expected);
  }

  private Reply requestToken(
      String accessToken,
      String method,
      String path,
      Object body,
      String etag,
      String requestKey,
      int... expected)
      throws Exception {
    HttpRequest.Builder builder =
        HttpRequest.newBuilder(URI.create(APP + path)).timeout(Duration.ofSeconds(5));
    if (accessToken != null) builder.header("Authorization", "Bearer " + accessToken);
    builder.header("X-Correlation-ID", "smoke-" + run);
    if (etag != null) builder.header("If-Match", etag);
    if (requestKey != null) builder.header("Idempotency-Key", requestKey);
    if (body != null)
      builder.header(
          "Content-Type",
          method.equals("PATCH") ? "application/merge-patch+json" : "application/json");
    builder.method(
        method,
        body == null
            ? HttpRequest.BodyPublishers.noBody()
            : HttpRequest.BodyPublishers.ofString(JSON.writeValueAsString(body)));
    HttpResponse<String> response =
        http.send(builder.build(), HttpResponse.BodyHandlers.ofString());
    JsonNode parsed = response.body().isBlank() ? JSON.nullNode() : JSON.readTree(response.body());
    Reply reply =
        new Reply(
            response.statusCode(),
            parsed,
            response.headers().firstValue("ETag").orElse(null),
            response.headers().firstValue("Content-Type").orElse(""));
    for (int code : expected) if (reply.status == code) return reply;
    if (expected.length > 0)
      throw new AssertionError(
          method
              + " "
              + path
              + " expected "
              + java.util.Arrays.toString(expected)
              + " but got "
              + reply.status
              + ": "
              + parsed);
    return reply;
  }

  private JsonNode external(String method, String url, Object body, int expected) throws Exception {
    HttpRequest.Builder request =
        HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(5));
    if (body != null) request.header("Content-Type", "application/json");
    request.method(
        method,
        body == null
            ? HttpRequest.BodyPublishers.noBody()
            : HttpRequest.BodyPublishers.ofString(JSON.writeValueAsString(body)));
    HttpResponse<String> response =
        http.send(request.build(), HttpResponse.BodyHandlers.ofString());
    require(
        response.statusCode() == expected,
        "External "
            + method
            + " "
            + url
            + " expected "
            + expected
            + " got "
            + response.statusCode());
    return response.body().isBlank() ? JSON.nullNode() : JSON.readTree(response.body());
  }

  private UUID mapping(
      String method, String path, Map<String, Object> query, int status, Object body)
      throws Exception {
    UUID id = UUID.randomUUID();
    Map<String, Object> request = obj("method", method, "urlPath", path);
    if (query != null) {
      Map<String, Object> parameters = obj();
      query.forEach((k, v) -> parameters.put(k, obj("equalTo", v.toString())));
      request.put("queryParameters", parameters);
    }
    external(
        "POST",
        stubs + "/__admin/mappings",
        obj(
            "id",
            id,
            "priority",
            1,
            "request",
            request,
            "response",
            obj(
                "status",
                status,
                "headers",
                obj("Content-Type", "application/json"),
                "jsonBody",
                body)),
        201);
    mappings.add(id);
    return id;
  }

  private void deleteMapping(UUID id) throws Exception {
    external("DELETE", stubs + "/__admin/mappings/" + id, null, 200);
    mappings.remove(id);
  }

  private Actor realLogin(String auth, String emailEnv, String passwordEnv) throws Exception {
    String email = System.getenv(emailEnv);
    String password = System.getenv(passwordEnv);
    require(
        (email == null) == (password == null),
        emailEnv + " and " + passwordEnv + " must both be supplied");
    if (email == null) {
      email = "submission-smoke-" + unique() + "@example.com";
      password = UUID.randomUUID().toString().replace("-", "").substring(0, 24);
      external(
          "POST",
          auth + "/api/v1/auth/register",
          obj("email", email, "password", password, "displayName", "Submission smoke"),
          201);
    }
    JsonNode login =
        external(
            "POST", auth + "/api/v1/auth/login", obj("email", email, "password", password), 200);
    String access = text(login, "accessToken");
    SignedJWT jwt = SignedJWT.parse(access);
    UUID subject = UUID.fromString(jwt.getJWTClaimsSet().getSubject());
    return new Actor(subject, jwt.getJWTClaimsSet().getStringListClaim("roles"), access);
  }

  private String token(Actor actor) throws Exception {
    if (actor == null) return null;
    if (actor.accessToken != null) return actor.accessToken;
    return signed(actor, Instant.now().minusSeconds(1), Instant.now().plusSeconds(300));
  }

  private String signed(Actor actor, Instant issued, Instant expires) throws Exception {
    SignedJWT jwt =
        new SignedJWT(
            new JWSHeader.Builder(JWSAlgorithm.RS256)
                .keyID(key.getKeyID())
                .type(JOSEObjectType.JWT)
                .build(),
            new JWTClaimsSet.Builder()
                .subject(actor.id.toString())
                .issuer(issuer)
                .audience("student-discounts-api")
                .claim("sid", UUID.randomUUID().toString())
                .claim("roles", actor.roles)
                .issueTime(Date.from(issued))
                .expirationTime(Date.from(expires))
                .build());
    jwt.sign(new RSASSASigner(key));
    return jwt.serialize();
  }

  private void start(boolean relay) throws Exception {
    refuseOccupied(18081);
    Files.createDirectories(Path.of("build", "reports", "smoke"));
    log =
        Path.of(
            "build",
            "reports",
            "smoke",
            run + "-" + (relay ? "relay" : "paused") + "-" + System.nanoTime() + ".log");
    String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
    List<String> command =
        new ArrayList<>(
            List.of(
                java,
                "-jar",
                "build/libs/submission-service.jar",
                "--spring.profiles.active=local",
                "--server.port=18081",
                "--spring.datasource.url=" + schemaUrl(),
                "--spring.datasource.username=" + dbUser,
                "--spring.datasource.password=" + dbPassword,
                "--spring.flyway.schemas=" + schema,
                "--spring.flyway.default-schema=" + schema,
                "--spring.kafka.bootstrap-servers=" + broker,
                "--spring.kafka.consumer.group-id=" + group,
                "--submission.kafka.group-id=" + group,
                "--spring.security.oauth2.resourceserver.jwt.issuer-uri=" + issuer,
                "--spring.security.oauth2.resourceserver.jwt.jwk-set-uri=" + jwkUri,
                "--submission.limits.requests-per-minute=10000",
                "--submission.workers.outbox-enabled=" + relay,
                "--submission.workers.validation-enabled=true",
                "--submission.workers.validation-batch-size=2"));
    // Explicit environment contract avoids reliance on whichever defaults the local operator chose.
    ProcessBuilder builder =
        new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(log.toFile());
    Map<String, String> environment = builder.environment();
    environment.put("SD_SUBMISSION_DB_URL", schemaUrl());
    environment.put("SD_SUBMISSION_DB_USER", dbUser);
    environment.put("SD_SUBMISSION_DB_PASSWORD", dbPassword);
    environment.put("SD_SUBMISSION_KAFKA_BOOTSTRAP_SERVERS", broker);
    environment.put("SD_SUBMISSION_AUTH_ISSUER", issuer);
    environment.put("SD_SUBMISSION_AUTH_JWK_SET_URI", jwkUri);
    for (String business : List.of("CATALOG", "MEDIA", "GEO", "ORGANIZATIONS"))
      environment.put("SD_SUBMISSION_" + business + "_BASE_URL", stubs);
    child = builder.start();
    await(
        "owned executable jar readiness is UP",
        () -> {
          try {
            HttpResponse<String> response =
                http.send(
                    HttpRequest.newBuilder(URI.create(APP + "/actuator/health/readiness"))
                        .timeout(Duration.ofSeconds(2))
                        .GET()
                        .build(),
                    HttpResponse.BodyHandlers.ofString());
            return response.statusCode() == 200
                && text(JSON.readTree(response.body()), "status").equals("UP");
          } catch (IOException e) {
            return false;
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
          }
        });
  }

  private void stop() throws Exception {
    if (child == null) return;
    child.destroy();
    if (!child.waitFor(10, TimeUnit.SECONDS)) {
      child.destroyForcibly();
      require(child.waitFor(5, TimeUnit.SECONDS), "Owned child process must terminate");
    }
    child = null;
  }

  private Connection connection(boolean isolated) throws Exception {
    Properties properties = new Properties();
    properties.setProperty("user", dbUser);
    properties.setProperty("password", dbPassword);
    properties.setProperty("connectTimeout", "2");
    properties.setProperty("socketTimeout", "5");
    return DriverManager.getConnection(isolated ? schemaUrl() : db, properties);
  }

  private String schemaUrl() {
    return db + (db.contains("?") ? "&" : "?") + "currentSchema=" + schema;
  }

  private long sqlLong(String sql) throws Exception {
    try (Connection c = connection(true);
        Statement s = c.createStatement();
        ResultSet r = s.executeQuery(sql)) {
      require(r.next(), "Expected SQL proof row");
      return r.getLong(1);
    }
  }

  private String sqlText(String sql) throws Exception {
    try (Connection c = connection(true);
        Statement s = c.createStatement();
        ResultSet r = s.executeQuery(sql)) {
      require(r.next(), "Expected SQL proof row");
      return r.getString(1);
    }
  }

  @Override
  public void close() throws Exception {
    Exception cleanupFailure = null;
    try {
      stop();
    } catch (Exception e) {
      cleanupFailure = e;
    }
    try {
      if (consumer != null) consumer.close(Duration.ofSeconds(5));
    } catch (Exception e) {
      if (cleanupFailure == null) cleanupFailure = e;
      else cleanupFailure.addSuppressed(e);
    }
    try {
      if (producer != null) producer.close(Duration.ofSeconds(5));
    } catch (Exception e) {
      if (cleanupFailure == null) cleanupFailure = e;
      else cleanupFailure.addSuppressed(e);
    }
    try {
      if (jwks != null) jwks.stop(0);
    } catch (Exception e) {
      if (cleanupFailure == null) cleanupFailure = e;
      else cleanupFailure.addSuppressed(e);
    }
    for (UUID mapping : List.copyOf(mappings)) {
      try {
        deleteMapping(mapping);
      } catch (Exception e) {
        if (cleanupFailure == null) cleanupFailure = e;
        else cleanupFailure.addSuppressed(e);
      }
    }
    if (schemaCreated) {
      require(schema.matches("smoke_[0-9a-f]{32}"), "Refuse cleanup of unowned schema");
      try (Connection c = connection(false);
          Statement s = c.createStatement()) {
        s.execute("DROP SCHEMA " + schema + " CASCADE");
      } catch (Exception e) {
        if (cleanupFailure == null) cleanupFailure = e;
        else cleanupFailure.addSuppressed(e);
      }
    }
    if (cleanupFailure != null) throw cleanupFailure;
  }

  private static void refuseOccupied(int port) throws IOException {
    try (ServerSocket socket = new ServerSocket()) {
      // Permit only closed connections' TIME_WAIT, never a live listener or SO_REUSEPORT.
      socket.setReuseAddress(true);
      socket.bind(new InetSocketAddress("127.0.0.1", port));
    } catch (IOException e) {
      throw new IOException(
          "Refusing occupied smoke port "
              + port
              + "; never terminating another application's process",
          e);
    }
  }

  private static String route(String id, String suffix) {
    return "/api/v1/submissions/" + id + (suffix.isEmpty() ? "" : "/" + suffix);
  }

  private static String env(String name, String defaultValue) {
    String value = System.getenv(name);
    return value == null || value.isBlank() ? defaultValue : value;
  }

  private static String unique() {
    return UUID.randomUUID().toString();
  }

  private static String encode(String value) {
    return URLEncoder.encode(value, StandardCharsets.UTF_8);
  }

  private static String text(JsonNode node, String field) {
    return node.path(field).asText("");
  }

  private static Map<String, Object> obj(Object... entries) {
    require(entries.length % 2 == 0, "Map entries must be key/value pairs");
    Map<String, Object> map = new LinkedHashMap<>();
    for (int i = 0; i < entries.length; i += 2) map.put(entries[i].toString(), entries[i + 1]);
    return map;
  }

  private static void require(boolean condition, String message) {
    if (!condition) throw new AssertionError(message);
  }
}
