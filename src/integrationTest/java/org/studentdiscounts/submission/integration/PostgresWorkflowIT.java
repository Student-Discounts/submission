package org.studentdiscounts.submission.integration;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.*;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.studentdiscounts.submission.domain.ActorRole;
import org.studentdiscounts.submission.domain.SubmissionActor;
import org.studentdiscounts.submission.domain.SubmissionStatus;
import org.studentdiscounts.submission.submission.*;
import org.testcontainers.postgresql.PostgreSQLContainer;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

class PostgresWorkflowIT {
  static PostgreSQLContainer db = new PostgreSQLContainer("postgres:17-bookworm");
  SubmissionStore store;
  AuthorSubmissionCommands commands;
  SubmissionQueries queries;
  ReviewCorrections corrections;
  BusinessClients clients;
  SubmissionActor author;
  NamedParameterJdbcTemplate jdbc;
  JsonMapper mapper;
  Clock clock;
  DataSourceTransactionManager transactionManager;
  DriverManagerDataSource dataSource;

  @BeforeAll
  static void start() {
    db.start();
  }

  @AfterAll
  static void stop() {
    db.stop();
  }

  @BeforeEach
  void setup() {
    dataSource = new DriverManagerDataSource(db.getJdbcUrl(), db.getUsername(), db.getPassword());
    String schema = "test_" + UUID.randomUUID().toString().replace("-", "");
    new org.springframework.jdbc.core.JdbcTemplate(dataSource).execute("CREATE SCHEMA " + schema);
    dataSource.setUrl(
        db.getJdbcUrl() + (db.getJdbcUrl().contains("?") ? "&" : "?") + "currentSchema=" + schema);
    Flyway.configure().dataSource(dataSource).locations("classpath:db/migration").load().migrate();
    mapper = JsonMapper.builder().build();
    clock = Clock.fixed(Instant.parse("2026-11-05T10:00:00Z"), ZoneOffset.UTC);
    jdbc = new NamedParameterJdbcTemplate(dataSource);
    transactionManager = new DataSourceTransactionManager(dataSource);
    store = new SubmissionStore(jdbc, mapper, clock);
    clients = mock(BusinessClients.class);
    var permissions = new SubmissionPermissions(clients);
    var preparation = new SubmissionPreparation(new PayloadValidation(clock), clients, mapper);
    var transactions =
        new org.springframework.transaction.support.TransactionTemplate(transactionManager);
    var idempotency = new IdempotentCommands(store, transactions, mapper);
    var views = new SubmissionViews(store, mapper);
    commands =
        new AuthorSubmissionCommands(
            store, permissions, preparation, idempotency, views, transactions, mapper, clients);
    queries = new SubmissionQueries(store, permissions, views, mapper, clients);
    corrections =
        new ReviewCorrections(store, permissions, preparation, idempotency, views, clients);
    when(clients.category("cafe")).thenReturn(new BusinessClients.Category("cafe", true));
    when(clients.timezone(any())).thenReturn(ZoneId.of("Europe/Moscow"));
    author = new SubmissionActor(UUID.randomUUID(), Set.of(ActorRole.STUDENT));
  }

  JsonNode parse(String s) {
    return mapper.readTree(s);
  }

  JsonNode full() {
    return parse(
        """
        {"place":{"name":"Кофейня","city":"Санкт-Петербург","addressLine":"Кронверкский пр., 49","categoryCode":"cafe","location":{"lat":59.9571,"lon":30.3083}},"offers":[{"conditions":"Цена 500","discountType":"SPECIAL_PRICE","discountValue":500,"source":{"type":"WEBSITE","url":"https://example.com"}}]}
        """);
  }

  @Test
  void incompleteDraftAndAtomicImmutableVersionSurviveReplay() {
    var create =
        commands.create(author, "create", parse("{\"type\":\"NEW_PLACE\",\"payload\":{}}"));
    UUID id = UUID.fromString(create.body().path("id").asText());
    assertThat(create.status()).isEqualTo(201);
    assertThat(store.find(id).currentVersionNo()).isNull();
    assertThatThrownBy(() -> commands.submit(author, id, 0, "bad"))
        .isInstanceOf(SubmissionException.class);
    assertThat(
            jdbc.queryForObject(
                "SELECT count(*) FROM submission_versions", Map.of(), Integer.class))
        .isZero();
    commands.patch(author, id, 0, full());
    var sent = commands.submit(author, id, 1, "send");
    assertThat(sent.status()).isEqualTo(202);
    assertThat(sent.body().path("versionNo").asInt()).isEqualTo(1);
    assertThat(commands.submit(author, id, 1, "send")).isEqualTo(sent);
    var existing = commands.submit(author, id, 2, "another");
    assertThat(existing.status()).isEqualTo(200);
    assertThat(
            jdbc.queryForObject(
                "SELECT count(*) FROM submission_versions", Map.of(), Integer.class))
        .isEqualTo(1);
    assertThat(
            jdbc.queryForObject(
                "SELECT count(*) FROM outbox_events WHERE event_type='SubmissionSubmitted'",
                Map.of(),
                Integer.class))
        .isEqualTo(1);
    assertThatThrownBy(() -> jdbc.update("UPDATE submission_versions SET payload='{}'", Map.of()))
        .hasRootCauseInstanceOf(java.sql.SQLException.class);
    assertThatThrownBy(() -> jdbc.update("DELETE FROM submission_versions", Map.of()))
        .hasRootCauseInstanceOf(java.sql.SQLException.class);
    assertThatThrownBy(() -> jdbc.update("TRUNCATE submission_versions CASCADE", Map.of()))
        .hasRootCauseInstanceOf(java.sql.SQLException.class);
  }

  @Test
  void concurrentCommandsOnlyFreezeOneVersion() throws Exception {
    var created =
        commands.create(
            author, "create", parse("{\"type\":\"NEW_PLACE\",\"payload\":" + full() + "}"));
    UUID id = UUID.fromString(created.body().path("id").asText());
    try (var pool = Executors.newFixedThreadPool(2)) {
      var a = pool.submit(() -> commands.submit(author, id, 0, "send"));
      var b = pool.submit(() -> commands.submit(author, id, 0, "send"));
      assertThat(a.get()).isEqualTo(b.get());
    }
    assertThat(
            jdbc.queryForObject(
                "SELECT count(*) FROM submission_versions", Map.of(), Integer.class))
        .isEqualTo(1);
  }

  @Test
  void outboxFailureRollsBackVersionStateAndIdempotency() {
    var created =
        commands.create(
            author, "create", parse("{\"type\":\"NEW_PLACE\",\"payload\":" + full() + "}"));
    UUID id = UUID.fromString(created.body().path("id").asText());
    jdbc.getJdbcTemplate()
        .execute(
            "ALTER TABLE outbox_events ADD CONSTRAINT fail_submit CHECK(event_type <>"
                + " 'SubmissionSubmitted')");
    assertThatThrownBy(() -> commands.submit(author, id, 0, "send"))
        .isInstanceOf(RuntimeException.class);
    assertThat(store.find(id).status()).isEqualTo(SubmissionStatus.DRAFT);
    assertThat(
            jdbc.queryForObject(
                "SELECT count(*) FROM submission_versions", Map.of(), Integer.class))
        .isZero();
    assertThat(
            jdbc.queryForObject(
                "SELECT count(*) FROM idempotency_requests WHERE request_key='send'",
                Map.of(),
                Integer.class))
        .isZero();
  }

  @Test
  void privacyAndOptimisticPatchAreEnforced() {
    var created =
        commands.create(author, "create", parse("{\"type\":\"NEW_PLACE\",\"payload\":{}}"));
    UUID id = UUID.fromString(created.body().path("id").asText());
    commands.patch(author, id, 0, parse("{\"place\":{\"name\":\"one\"}}"));
    assertThatThrownBy(() -> commands.patch(author, id, 0, parse("{}")))
        .isInstanceOf(SubmissionException.class)
        .extracting("status")
        .isEqualTo(412);
    SubmissionActor other = new SubmissionActor(UUID.randomUUID(), Set.of(ActorRole.STUDENT));
    assertThatThrownBy(() -> queries.get(other, id))
        .isInstanceOf(SubmissionException.class)
        .extracting("status")
        .isEqualTo(404);
    assertThat(queries.list(other, null, 20).path("items")).isEmpty();
  }

  @Test
  void unknownNullMetadataCannotBeNormalizedAway() {
    var payload = (tools.jackson.databind.node.ObjectNode) full();
    payload.putNull("status");
    assertThatThrownBy(() ->
            commands.create(
                author, "unknown", parse("{\"type\":\"NEW_PLACE\",\"payload\":" + payload + "}")))
        .isInstanceOf(SubmissionException.class)
        .extracting("status")
        .isEqualTo(422);
  }

  UUID createFull(String key) {
    return UUID.fromString(
        commands
            .create(author, key, parse("{\"type\":\"NEW_PLACE\",\"payload\":" + full() + "}"))
            .body()
            .path("id")
            .asText());
  }

  SubmissionActor moderator() {
    return new SubmissionActor(UUID.randomUUID(), Set.of(ActorRole.MODERATOR));
  }

  void fixtureStatus(UUID id, SubmissionStatus status) {
    var statusUpdate =
        new org.springframework.transaction.support.TransactionTemplate(transactionManager);
    statusUpdate.execute(t -> {
      var row = store.lock(id);
      store.update(
          row,
          status,
          status == SubmissionStatus.CHANGES_REQUESTED ? store.latest(row).payload() : null,
          row.currentVersionNo(),
          row.draftBaseOfferRowVersion());
      return null;
    });
  }

  @Test
  void replayAvoidsAllDependencyReadsAndKeyReuseConflicts() {
    UUID id = createFull("create");
    var first = commands.submit(author, id, 0, "submit");
    clearInvocations(clients);
    assertThat(commands.submit(author, id, 0, "submit")).isEqualTo(first);
    verifyNoInteractions(clients);
    assertThatThrownBy(() -> commands.submit(author, id, 1, "submit"))
        .isInstanceOf(SubmissionException.class)
        .extracting("code")
        .isEqualTo("IDEMPOTENCY_KEY_REUSED");
    assertThatThrownBy(() ->
            commands.create(author, "create", parse("{\"type\":\"NEW_PLACE\",\"payload\":{}}")))
        .isInstanceOf(SubmissionException.class)
        .extracting("code")
        .isEqualTo("IDEMPOTENCY_KEY_REUSED");
  }

  @Test
  void unchangedChangesRequestedCycleReusesImmutableVersion() {
    UUID id = createFull("create");
    commands.submit(author, id, 0, "submit");
    var original = store.latest(store.find(id));
    fixtureStatus(id, SubmissionStatus.CHANGES_REQUESTED);
    int expected = store.find(id).rowVersion();
    var sent = commands.submit(author, id, expected, "resubmit");
    assertThat(sent.status()).isEqualTo(202);
    assertThat(sent.body().path("versionId").asText()).isEqualTo(original.id().toString());
    assertThat(
            jdbc.queryForObject(
                "SELECT count(*) FROM submission_versions", Map.of(), Integer.class))
        .isEqualTo(1);
    assertThat(
            jdbc.queryForObject(
                "SELECT count(*) FROM outbox_events WHERE event_type='SubmissionSubmitted'",
                Map.of(),
                Integer.class))
        .isEqualTo(2);
  }

  @Test
  void correctionsFreezeNewSnapshotAndExplicitChangeBase() {
    UUID place = UUID.randomUUID(), offer = UUID.randomUUID();
    JsonNode publicSnapshot = full();
    when(clients.offer(offer))
        .thenReturn(new BusinessClients.Offer(offer, place, "ACTIVE", 7, publicSnapshot));
    var created =
        commands.create(
            author,
            "change",
            parse(
                "{\"type\":\"CHANGE\",\"targetPlaceId\":\""
                    + place
                    + "\",\"targetOfferId\":\""
                    + offer
                    + "\",\"payload\":{}}"));
    UUID id = UUID.fromString(created.body().path("id").asText());
    commands.submit(author, id, 0, "submit");
    var first = store.latest(store.find(id));
    fixtureStatus(id, SubmissionStatus.IN_REVIEW);
    SubmissionActor reviewer = moderator();
    int rowVersion = store.find(id).rowVersion();
    var body =
        (tools.jackson.databind.node.ObjectNode)
            parse(
                "{\"reviewedVersionId\":\""
                    + first.id()
                    + "\",\"payload\":"
                    + first.payload()
                    + ",\"expectedCatalogRowVersion\":7}");
    when(clients.offer(offer))
        .thenReturn(new BusinessClients.Offer(offer, place, "ACTIVE", 8, publicSnapshot));
    assertThat(queries.comparison(reviewer, id).path("catalogRowVersion").asInt()).isEqualTo(8);
    assertThatThrownBy(() -> corrections.correct(reviewer, id, rowVersion, "stale", body))
        .isInstanceOf(SubmissionException.class)
        .extracting("code")
        .isEqualTo("CATALOG_VERSION_CONFLICT");
    assertThat(store.latest(store.find(id)).id()).isEqualTo(first.id());
    body.put("expectedCatalogRowVersion", 8);
    var corrected = corrections.correct(reviewer, id, rowVersion, "fresh", body);
    assertThat(corrected.status()).isEqualTo(201);
    var second = store.latest(store.find(id));
    assertThat(second.previousVersionId()).isEqualTo(first.id());
    assertThat(second.versionNo()).isEqualTo(2);
    assertThat(second.baseOfferRowVersion()).isEqualTo(8);
    assertThat(second.createdBy()).isEqualTo(reviewer.id());
    assertThat(store.version(id, 1)).isEqualTo(first);
    assertThat(second.resolvedTimezone()).isEqualTo("Europe/Moscow");
    assertThat(corrections.correct(reviewer, id, rowVersion, "fresh", body)).isEqualTo(corrected);
  }

  @Test
  void correctingUnknownNullMetadataIs422EvenWithResolvedCoordinates() {
    UUID id = createFull("create");
    commands.submit(author, id, 0, "send");
    fixtureStatus(id, SubmissionStatus.IN_REVIEW);
    var v = store.latest(store.find(id));
    var payload = (tools.jackson.databind.node.ObjectNode) v.payload().deepCopy();
    payload.putNull("status");
    var body = parse("{\"reviewedVersionId\":\"" + v.id() + "\",\"payload\":" + payload + "}");
    assertThatThrownBy(() ->
            corrections.correct(moderator(), id, store.find(id).rowVersion(), "correct", body))
        .isInstanceOf(SubmissionException.class)
        .extracting("status")
        .isEqualTo(422);
    assertThat(store.latest(store.find(id)).versionNo()).isEqualTo(1);
  }

  @Test
  void moderatorCannotCorrectOwnSubmission() {
    UUID id = createFull("create");
    commands.submit(author, id, 0, "send");
    fixtureStatus(id, SubmissionStatus.IN_REVIEW);
    SubmissionActor ownModerator =
        new SubmissionActor(author.id(), Set.of(ActorRole.STUDENT, ActorRole.MODERATOR));
    assertThatThrownBy(() ->
            corrections.correct(ownModerator, id, store.find(id).rowVersion(), "own", parse("{}")))
        .isInstanceOf(SubmissionException.class)
        .extracting("status")
        .isEqualTo(403);
  }

  @Test
  void foreignAssetsAndMixedRepresentativeRoleNeverBypassAuthorization() {
    UUID media = UUID.randomUUID();
    when(clients.media(media))
        .thenReturn(new BusinessClients.MediaAsset(media, UUID.randomUUID(), "CLEAN", 1));
    var payload = (tools.jackson.databind.node.ObjectNode) full();
    payload.set(
        "attachments", parse("[{\"mediaAssetId\":\"" + media + "\",\"purpose\":\"PHOTO\"}]"));
    assertThatThrownBy(() ->
            commands.create(
                author, "foreign", parse("{\"type\":\"NEW_PLACE\",\"payload\":" + payload + "}")))
        .isInstanceOf(SubmissionException.class)
        .extracting("status")
        .isEqualTo(403);
    UUID place = UUID.randomUUID();
    SubmissionActor rep =
        new SubmissionActor(author.id(), Set.of(ActorRole.STUDENT, ActorRole.ORG_REPRESENTATIVE));
    when(clients.authorize(author.id(), place))
        .thenReturn(new BusinessClients.Authorization(false, true));
    assertThatThrownBy(() ->
            commands.create(
                rep,
                "rep",
                parse(
                    "{\"type\":\"NEW_OFFER\",\"targetPlaceId\":\"" + place + "\",\"payload\":{}}")))
        .isInstanceOf(SubmissionException.class)
        .extracting("status")
        .isEqualTo(403);
  }

  @Test
  void timezoneOutageCannotFreezeCoordinateBearingVersion() {
    UUID id = createFull("create");
    when(clients.timezone(any())).thenThrow(new BusinessClients.DependencyUnavailable());
    assertThatThrownBy(() -> commands.submit(author, id, 0, "submit"))
        .isInstanceOf(BusinessClients.DependencyUnavailable.class);
    assertThat(store.find(id).status()).isEqualTo(SubmissionStatus.DRAFT);
    assertThat(store.find(id).currentVersionNo()).isNull();
    assertThat(
            jdbc.queryForObject(
                "SELECT count(*) FROM idempotency_requests WHERE request_key='submit'",
                Map.of(),
                Integer.class))
        .isZero();
  }

  @Test
  void cancellationPreservesSnapshotAndDisallowsLaterCancellation() {
    UUID id = createFull("create");
    commands.submit(author, id, 0, "submit");
    var original = store.latest(store.find(id));
    assertThat(commands.cancel(author, id, 1).body().path("status").asText())
        .isEqualTo("CANCELLED");
    assertThat(store.latest(store.find(id))).isEqualTo(original);
    assertThatThrownBy(() -> commands.cancel(author, id, 2))
        .isInstanceOf(SubmissionException.class)
        .extracting("code")
        .isEqualTo("INVALID_TRANSITION");
  }

  @Test
  void paginationUsesExclusiveCursorsAndVersionSnapshotsSurviveNewStore() {
    UUID first = createFull("one"), second = createFull("two");
    commands.submit(author, first, 0, "send");
    JsonNode page = queries.list(author, null, 1);
    assertThat(page.path("items").size()).isEqualTo(1);
    assertThat(page.path("items").get(0).path("id").asText()).isEqualTo(second.toString());
    assertThat(
            queries
                .list(author, UUID.fromString(page.path("nextCursor").asText()), 1)
                .path("items")
                .get(0)
                .path("id")
                .asText())
        .isEqualTo(first.toString());
    var restarted = new SubmissionStore(new NamedParameterJdbcTemplate(dataSource), mapper, clock);
    assertThat(restarted.latest(restarted.find(first))).isEqualTo(store.latest(store.find(first)));
    assertThat(queries.versions(author, first, 1, 20).path("items")).isEmpty();
    assertThatThrownBy(() -> queries.versions(author, first, 0, 20))
        .isInstanceOf(SubmissionException.class);
  }

  @Test
  void versionsRejectCrossSubmissionReferencesAndReportRestrictSqlstate() {
    UUID one = createFull("one"), two = createFull("two");
    commands.submit(author, one, 0, "send-one");
    commands.submit(author, two, 0, "send-two");
    var previous = store.latest(store.find(one));
    assertThatThrownBy(() ->
            jdbc.update(
                "UPDATE submissions SET current_version_no=2 WHERE id=:id", Map.of("id", two)))
        .hasRootCauseInstanceOf(java.sql.SQLException.class);
    assertThatThrownBy(() ->
            jdbc.update(
                "INSERT INTO"
                    + " submission_versions(id,submission_id,version_no,previous_version_id,payload,payload_schema_version,content_sha256,created_by)"
                    + " VALUES(:id,:sid,2,:prev,'{}',1,:hash,:actor)",
                Map.of(
                    "id",
                    UUID.randomUUID(),
                    "sid",
                    two,
                    "prev",
                    previous.id(),
                    "hash",
                    "a".repeat(64),
                    "actor",
                    author.id())))
        .hasRootCauseInstanceOf(java.sql.SQLException.class);
    try {
      jdbc.update("UPDATE submission_versions SET payload='{}'", Map.of());
      fail("append-only update accepted");
    } catch (org.springframework.dao.DataAccessException failure) {
      Throwable cause = failure;
      while (cause.getCause() != null) cause = cause.getCause();
      assertThat(((java.sql.SQLException) cause).getSQLState()).isEqualTo("23001");
    }
  }
}
