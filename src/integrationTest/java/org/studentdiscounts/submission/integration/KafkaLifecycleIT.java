package org.studentdiscounts.submission.integration;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.support.TransactionTemplate;
import org.studentdiscounts.submission.config.SubmissionProperties;
import org.studentdiscounts.submission.domain.PayloadJson;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;

@Testcontainers
class KafkaLifecycleIT {
  @Container static PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:17-bookworm");
  @Container static KafkaContainer kafka = new KafkaContainer("apache/kafka:4.1.0");
  static NamedParameterJdbcTemplate jdbc;
  static TransactionTemplate tx;
  static final ObjectMapper json = new ObjectMapper();
  static final Clock clock = Clock.systemUTC();

  @BeforeAll
  static void database() {
    var ds =
        new DriverManagerDataSource(
            postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
    Flyway.configure().dataSource(ds).load().migrate();
    jdbc = new NamedParameterJdbcTemplate(ds);
    tx = new TransactionTemplate(new DataSourceTransactionManager(ds));
  }

  @Test
  void approvalIsNotPublicationAndInboxReplayCannotUndoLaterState() {
    var fixture = fixture("IN_REVIEW", "[]");
    BusinessClients clients = mock(BusinessClients.class);
    WorkflowService service =
        new WorkflowService(new WorkflowRepository(jdbc, json, clock), tx, clients);
    ObjectNode approved = event("SubmissionApproved", "moderation", fixture, 0);
    approved
        .withObject("data")
        .put("actorId", UUID.randomUUID().toString())
        .set("payload", fixture.payload());
    service.process(approved);
    assertEquals("APPROVED", status(fixture.id()));
    assertEquals(1, count("outbox_events", fixture.id()));
    ObjectNode published = event("OfferPublished", "catalog", fixture, 1);
    published
        .withObject("data")
        .putArray("offerIds")
        .add(UUID.randomUUID().toString())
        .add(UUID.randomUUID().toString());
    service.process(published);
    assertEquals("PUBLISHED", status(fixture.id()));
    service.process(approved);
    service.process(published);
    assertEquals("PUBLISHED", status(fixture.id()));
    assertEquals(2, count("submission_status_history", fixture.id()));
    ObjectNode duplicate = published.deepCopy();
    duplicate.put("eventId", UUID.randomUUID().toString());
    service.process(duplicate);
    assertEquals(2, count("submission_status_history", fixture.id()));
    ObjectNode conflict = published.deepCopy();
    conflict.put("eventId", UUID.randomUUID().toString());
    conflict
        .withObject("data")
        .putArray("offerIds")
        .add(UUID.randomUUID().toString())
        .add(UUID.randomUUID().toString());
    assertThrows(InvalidWorkflowEvent.class, () -> service.process(conflict));
    assertEquals("PUBLISHED", status(fixture.id()));
  }

  @Test
  void staleTupleSelfReviewAndPrematurePublicationAreAtomicFailures() {
    var fixture = fixture("PENDING_REVIEW", "[]");
    WorkflowService service =
        new WorkflowService(
            new WorkflowRepository(jdbc, json, clock), tx, mock(BusinessClients.class));
    var self = event("ReviewStarted", "moderation", fixture, 0);
    self.withObject("data").put("actorId", fixture.author().toString());
    assertThrows(InvalidWorkflowEvent.class, () -> service.process(self));
    var stale = self.deepCopy();
    stale.put("eventId", UUID.randomUUID().toString());
    stale
        .withObject("data")
        .put("actorId", UUID.randomUUID().toString())
        .put("expectedRowVersion", 9);
    assertThrows(InvalidWorkflowEvent.class, () -> service.process(stale));
    var publication = event("OfferPublished", "catalog", fixture, 0);
    publication
        .withObject("data")
        .putArray("offerIds")
        .add(UUID.randomUUID().toString())
        .add(UUID.randomUUID().toString());
    assertThrows(InvalidWorkflowEvent.class, () -> service.process(publication));
    assertEquals("PENDING_REVIEW", status(fixture.id()));
    assertEquals(0, count("outbox_events", fixture.id()));
    assertEquals(
        0,
        jdbc.queryForObject(
            "select count(*) from inbox_events where event_id=:id",
            Map.of("id", UUID.fromString(self.path("eventId").asText())),
            Integer.class));
  }

  @Test
  void pendingMediaSurvivesWorkerRestartAndNewerEventOverridesOlderHttpObservation() {
    UUID asset = UUID.randomUUID();
    var fixture =
        fixture("SUBMITTED", "[{\"mediaAssetId\":\"" + asset + "\",\"purpose\":\"PROOF\"}]");
    var clients = mock(BusinessClients.class);
    when(clients.media(asset))
        .thenReturn(new BusinessClients.MediaAsset(asset, fixture.author(), "QUARANTINED", 1));
    var service = new WorkflowService(new WorkflowRepository(jdbc, json, clock), tx, clients);
    new ValidationWorker(
            new WorkflowRepository(jdbc, json, clock),
            tx,
            clients,
            clock,
            properties("submission.events.v1"))
        .runBatch();
    assertEquals("VALIDATING", status(fixture.id()));
    ObjectNode checked =
        json.createObjectNode()
            .put("eventId", UUID.randomUUID().toString())
            .put("eventType", "MediaAssetChecked")
            .put("schemaVersion", 1)
            .put("producer", "media")
            .put("occurredAt", Instant.now().toString())
            .put("correlationId", "test");
    checked
        .putObject("data")
        .put("mediaAssetId", asset.toString())
        .put("status", "CLEAN")
        .put("revision", 2);
    service.process(checked);
    new ValidationWorker(
            new WorkflowRepository(jdbc, json, clock),
            tx,
            clients,
            clock,
            properties("submission.events.v1"))
        .runBatch();
    assertEquals("PENDING_REVIEW", status(fixture.id()));
    checked.put("eventId", UUID.randomUUID().toString());
    checked.withObject("data").put("status", "REJECTED").put("revision", 1);
    service.process(checked);
    assertEquals("PENDING_REVIEW", status(fixture.id()));
  }

  @Test
  void relayPublishesCommittedStableEnvelopeInSequenceAndCanResume() throws Exception {
    var fixture = fixture("IN_REVIEW", "[]");
    var service =
        new WorkflowService(
            new WorkflowRepository(jdbc, json, clock), tx, mock(BusinessClients.class));
    var approved = event("SubmissionApproved", "moderation", fixture, 0);
    approved
        .withObject("data")
        .put("actorId", UUID.randomUUID().toString())
        .set("payload", fixture.payload());
    service.process(approved);
    var published = event("OfferPublished", "catalog", fixture, 1);
    published
        .withObject("data")
        .putArray("offerIds")
        .add(UUID.randomUUID().toString())
        .add(UUID.randomUUID().toString());
    service.process(published);
    var suspended = event("SubmissionSuspended", "catalog", fixture, 2);
    suspended.withObject("data").put("reason", "CATALOG_SUSPENDED");
    service.process(suspended);
    String topic = "submission.events." + UUID.randomUUID();
    var producerFactory =
        new DefaultKafkaProducerFactory<String, String>(
            Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                kafka.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class,
                ProducerConfig.ACKS_CONFIG,
                "all",
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,
                true));
    var template = new KafkaTemplate<>(producerFactory);
    var relay = new OutboxRelay(new OutboxRepository(jdbc, clock), tx, template, properties(topic));
    assertTrue(count("outbox_events", fixture.id()) > 0);
    try (var executor = java.util.concurrent.Executors.newFixedThreadPool(2)) {
      var other =
          new OutboxRelay(new OutboxRepository(jdbc, clock), tx, template, properties(topic));
      var first = executor.submit(relay::relayBatch);
      var second = executor.submit(other::relayBatch);
      first.get();
      second.get();
      relay.relayBatch();
    }
    try (var consumer =
        new KafkaConsumer<String, String>(
            Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                kafka.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG,
                UUID.randomUUID().toString(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class))) {
      consumer.subscribe(List.of(topic));
      long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
      var sequences = new java.util.ArrayList<Integer>();
      var states = new java.util.ArrayList<String>();
      while (sequences.size() < 3 && System.nanoTime() < deadline)
        for (var record : consumer.poll(Duration.ofMillis(250))) {
          var value = json.readTree(record.value());
          if (fixture.id().toString().equals(value.path("data").path("submissionId").asText())) {
            sequences.add(value.path("aggregateSequence").asInt());
            states.add(value.path("data").path("toStatus").asText());
          }
        }
      assertEquals(List.of(1, 2, 3), sequences, "two relays cannot overtake an aggregate sequence");
      assertEquals(List.of("APPROVED", "PUBLISHED", "SUSPENDED"), states);
    } finally {
      producerFactory.destroy();
    }
    assertEquals(
        0,
        jdbc.queryForObject(
            "select count(*) from outbox_events where published_at is null",
            Map.of(),
            Integer.class));
  }

  @Test
  void kafkaPoisonIsDltAndTransientDatabaseFailureGetsThreeTotalAttempts() throws Exception {
    String topic = "workflow." + UUID.randomUUID();
    var properties =
        Map.<String, Object>of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
            kafka.getBootstrapServers(),
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            StringSerializer.class,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            StringSerializer.class,
            ProducerConfig.ACKS_CONFIG,
            "all");
    var producerFactory = new DefaultKafkaProducerFactory<String, String>(properties);
    var template = new KafkaTemplate<String, String>(producerFactory);
    try (var admin =
        org.apache.kafka.clients.admin.AdminClient.create(
            Map.of("bootstrap.servers", kafka.getBootstrapServers()))) {
      admin
          .createTopics(
              List.of(
                  new org.apache.kafka.clients.admin.NewTopic(topic, 1, (short) 1),
                  new org.apache.kafka.clients.admin.NewTopic(topic + ".DLT", 1, (short) 1)))
          .all()
          .get();
    }
    var consumers =
        new org.springframework.kafka.core.DefaultKafkaConsumerFactory<String, String>(
            Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                kafka.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG,
                UUID.randomUUID().toString(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "earliest",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                false,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class));
    var registry = new io.micrometer.core.instrument.simple.SimpleMeterRegistry();
    var service = mock(WorkflowService.class);
    var attempts = new java.util.concurrent.atomic.AtomicInteger();
    var completed = new java.util.concurrent.CountDownLatch(1);
    doAnswer(
            invocation -> {
              var value = (tools.jackson.databind.JsonNode) invocation.getArgument(0);
              if (value.path("eventType").asText().equals("transient")) {
                if (attempts.incrementAndGet() < 3)
                  throw new org.springframework.dao.DataAccessResourceFailureException("temporary");
                completed.countDown();
                return null;
              }
              throw new InvalidWorkflowEvent("POISON");
            })
        .when(service)
        .process(any());
    var factory =
        new WorkflowKafkaConfiguration()
            .workflowKafkaListenerContainerFactory(
                consumers, template, registry, properties(topic));
    var container = factory.createContainer(topic);
    var listener = new WorkflowListener(service, json, registry);
    container
        .getContainerProperties()
        .setMessageListener(
            (org.springframework.kafka.listener.MessageListener<String, String>)
                record -> listener.consume(record.value()));
    container.start();
    try (var consumer =
        new KafkaConsumer<String, String>(
            Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                kafka.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG,
                UUID.randomUUID().toString(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class))) {
      consumer.subscribe(List.of(topic + ".DLT"));
      template.send(topic, "poison", "not json").get();
      template.send(topic, "illegal", "{\"eventType\":\"illegal\"}").get();
      template.send(topic, "transient", "{\"eventType\":\"transient\"}").get();
      assertTrue(completed.await(20, java.util.concurrent.TimeUnit.SECONDS));
      assertEquals(3, attempts.get());
      var keys = new java.util.HashSet<String>();
      long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
      while (keys.size() < 2 && System.nanoTime() < deadline)
        for (var record : consumer.poll(Duration.ofMillis(250))) keys.add(record.key());
      assertEquals(java.util.Set.of("poison", "illegal"), keys);
      assertEquals(2, registry.get("submission.workflow.dlt.published").counter().count());
    } finally {
      container.stop();
      producerFactory.destroy();
      registry.close();
    }
  }

  @Test
  void approvalReadsCurrentMediaRevisionAndCannotTrustAnOldCleanObservation() {
    UUID asset = UUID.randomUUID();
    var fixture =
        fixture("IN_REVIEW", "[{\"mediaAssetId\":\"" + asset + "\",\"purpose\":\"PROOF\"}]");
    jdbc.update(
        "insert into submission_media_checks(version_id,media_asset_id,status,asset_revision)"
            + " values(:version,:asset,'CLEAN',1)",
        Map.of("version", fixture.version(), "asset", asset));
    var clients = mock(BusinessClients.class);
    when(clients.media(asset))
        .thenReturn(new BusinessClients.MediaAsset(asset, fixture.author(), "REJECTED", 2));
    var service = new WorkflowService(new WorkflowRepository(jdbc, json, clock), tx, clients);
    var approved = event("SubmissionApproved", "moderation", fixture, 0);
    approved
        .withObject("data")
        .put("actorId", UUID.randomUUID().toString())
        .set("payload", fixture.payload());
    assertThrows(InvalidWorkflowEvent.class, () -> service.process(approved));
    assertEquals("IN_REVIEW", status(fixture.id()));
    assertEquals(0, count("outbox_events", fixture.id()));
  }

  @Test
  void approvalCannotReuseMediaReadsFromTheVersionBeforeAConcurrentCorrection() {
    UUID previousAsset = UUID.randomUUID();
    UUID correctedAsset = UUID.randomUUID();
    UUID correctedVersion = UUID.randomUUID();
    var fixture =
        fixture(
            "IN_REVIEW", "[{\"mediaAssetId\":\"" + previousAsset + "\",\"purpose\":\"PROOF\"}]");
    ObjectNode correctedPayload = fixture.payload().deepCopy();
    correctedPayload
        .putArray("attachments")
        .addObject()
        .put("mediaAssetId", correctedAsset.toString())
        .put("purpose", "PROOF");
    var clients = mock(BusinessClients.class);
    when(clients.media(previousAsset))
        .thenAnswer(ignored -> {
          // A separately committed correction lands after preparation reads v1, before approval
          // locks v2.
          tx.executeWithoutResult(transaction -> {
            jdbc.update(
                """
                INSERT INTO submission_versions(id,submission_id,version_no,previous_version_id,
                        payload,payload_schema_version,content_sha256,created_by,resolved_timezone)
                VALUES(:version,:id,2,:previous,CAST(:payload AS jsonb),1,:hash,:author,'Europe/Moscow')
                """,
                Map.of(
                    "version",
                    correctedVersion,
                    "id",
                    fixture.id(),
                    "previous",
                    fixture.version(),
                    "payload",
                    correctedPayload.toString(),
                    "hash",
                    PayloadJson.hash(correctedPayload),
                    "author",
                    fixture.author()));
            jdbc.update(
                "UPDATE submissions SET current_version_no=2,row_version=1 WHERE id=:id",
                Map.of("id", fixture.id()));
            jdbc.update(
                """
                INSERT INTO submission_media_checks(version_id,media_asset_id,status,asset_revision)
                VALUES(:version,:asset,'CLEAN',1)
                """,
                Map.of("version", correctedVersion, "asset", correctedAsset));
          });
          return new BusinessClients.MediaAsset(previousAsset, fixture.author(), "CLEAN", 1);
        });
    var service = new WorkflowService(new WorkflowRepository(jdbc, json, clock), tx, clients);
    var approval = event("SubmissionApproved", "moderation", fixture, 1);
    approval
        .withObject("data")
        .put("versionId", correctedVersion.toString())
        .put("versionNo", 2)
        .put("actorId", UUID.randomUUID().toString())
        .set("payload", correctedPayload);

    var failure = assertThrows(InvalidWorkflowEvent.class, () -> service.process(approval));
    assertEquals("STALE_REVIEWED_VERSION", failure.getMessage());
    assertEquals("IN_REVIEW", status(fixture.id()));
    assertEquals(0, count("outbox_events", fixture.id()));
    assertEquals(0, count("submission_status_history", fixture.id()));
    assertEquals(
        0,
        jdbc.queryForObject(
            "SELECT count(*) FROM inbox_events WHERE event_id=:id",
            Map.of("id", UUID.fromString(approval.path("eventId").asText())),
            Integer.class));
    verify(clients, never()).media(correctedAsset);
  }

  @Test
  void publicationConflictRequiresAnExplicitNewImmutableVersionEvenIfCatalogReadIsStale() {
    var fixture = fixture("IN_REVIEW", "[]");
    UUID target = UUID.randomUUID(), place = UUID.randomUUID();
    jdbc.update(
        "update submissions set type='CHANGE',target_place_id=:place,target_offer_id=:offer where"
            + " id=:id",
        Map.of("place", place, "offer", target, "id", fixture.id()));
    // Build a genuine CHANGE snapshot without altering an immutable version.
    var clients = mock(BusinessClients.class);
    when(clients.offer(target))
        .thenReturn(new BusinessClients.Offer(target, place, "ACTIVE", 7, fixture.payload()));
    // A public base must be captured at version insertion; fixture's null base cannot approve.
    // Start at APPROVED to exercise the Catalog-confirmed conflict path specifically.
    jdbc.update(
        "update submissions set status='APPROVED' where id=:id", Map.of("id", fixture.id()));
    var service = new WorkflowService(new WorkflowRepository(jdbc, json, clock), tx, clients);
    var conflict = event("OfferPublicationConflicted", "catalog", fixture, 0);
    conflict.withObject("data").put("reason", "CATALOG_VERSION_CONFLICT");
    service.process(conflict);
    assertEquals("PENDING_REVIEW", status(fixture.id()));
    var start = event("ReviewStarted", "moderation", fixture, 1);
    start.withObject("data").put("actorId", UUID.randomUUID().toString());
    service.process(start);
    var approval = event("SubmissionApproved", "moderation", fixture, 2);
    approval
        .withObject("data")
        .put("actorId", UUID.randomUUID().toString())
        .set("payload", fixture.payload());
    var failure = assertThrows(InvalidWorkflowEvent.class, () -> service.process(approval));
    assertEquals("NEW_REVIEWED_VERSION_REQUIRED", failure.getMessage());
    assertEquals("IN_REVIEW", status(fixture.id()));
  }

  @Test
  void rejectedAttachmentCannotBypassValidationAndOutageStaysPending() {
    UUID asset = UUID.randomUUID();
    var fixture =
        fixture("SUBMITTED", "[{\"mediaAssetId\":\"" + asset + "\",\"purpose\":\"PHOTO\"}]");
    var clients = mock(BusinessClients.class);
    when(clients.media(asset)).thenThrow(new BusinessClients.DependencyUnavailable());
    var worker =
        new ValidationWorker(
            new WorkflowRepository(jdbc, json, clock),
            tx,
            clients,
            clock,
            properties("submission.events.v1"));
    worker.runBatch();
    assertEquals("VALIDATING", status(fixture.id()));
    assertEquals(1, worker.failureCount());
    doReturn(new BusinessClients.MediaAsset(asset, fixture.author(), "REJECTED", 2))
        .when(clients)
        .media(asset);
    new ValidationWorker(
            new WorkflowRepository(jdbc, json, clock),
            tx,
            clients,
            clock,
            properties("submission.events.v1"))
        .runBatch();
    assertEquals("REJECTED", status(fixture.id()));
    assertEquals(
        "MEDIA_REJECTED",
        jdbc.queryForObject(
            "select reason from submission_status_history where submission_id=:id and"
                + " to_status='REJECTED'",
            Map.of("id", fixture.id()),
            String.class));
  }

  @Test
  void malformedUuidTextIsNotAcceptedAsAWorkflowSchemaUuid() {
    var fixture = fixture("PENDING_REVIEW", "[]");
    var service =
        new WorkflowService(
            new WorkflowRepository(jdbc, json, clock), tx, mock(BusinessClients.class));
    var start = event("ReviewStarted", "moderation", fixture, 0);
    start.put("eventId", "1-1-1-1-1");
    start.withObject("data").put("actorId", UUID.randomUUID().toString());
    assertThrows(InvalidWorkflowEvent.class, () -> service.process(start));
    assertEquals("PENDING_REVIEW", status(fixture.id()));
  }

  private static SubmissionProperties properties(String eventsTopic) {
    return new Binder(
            new MapConfigurationPropertySource(
                Map.of(
                    "submission.dependencies.catalog-base-url", "http://localhost:8090",
                    "submission.dependencies.media-base-url", "http://localhost:8090",
                    "submission.dependencies.geo-base-url", "http://localhost:8090",
                    "submission.kafka.workflow-topic", eventsTopic,
                    "submission.kafka.events-topic", eventsTopic)))
        .bindOrCreate("submission", SubmissionProperties.class);
  }

  record Fixture(UUID id, UUID author, UUID version, ObjectNode payload) {}

  static Fixture fixture(String state, String attachments) {
    UUID id = UUID.randomUUID(), author = UUID.randomUUID(), version = UUID.randomUUID();
    ObjectNode payload =
        (ObjectNode)
            json.readTree(
                "{\"place\":{\"location\":{\"lat\":59.9,\"lon\":30.3}},\"offers\":[{},{}],\"attachments\":"
                    + attachments
                    + "}");
    jdbc.update(
        "insert into submissions(id,type,author_id,draft_payload)"
            + " values(:id,'NEW_PLACE',:author,cast(:payload as jsonb))",
        Map.of("id", id, "author", author, "payload", payload.toString()));
    jdbc.update(
        "insert into"
            + " submission_versions(id,submission_id,version_no,payload,payload_schema_version,content_sha256,created_by,resolved_timezone)"
            + " values(:version,:id,1,cast(:payload as jsonb),1,:hash,:author,'Europe/Moscow')",
        Map.of(
            "version",
            version,
            "id",
            id,
            "payload",
            payload.toString(),
            "hash",
            PayloadJson.hash(payload),
            "author",
            author));
    jdbc.update(
        "update submissions set status=cast(:state as submission_status),current_version_no=1 where"
            + " id=:id",
        Map.of("state", state, "id", id));
    return new Fixture(id, author, version, payload);
  }

  static ObjectNode event(String type, String producer, Fixture f, int row) {
    ObjectNode event =
        json.createObjectNode()
            .put("eventId", UUID.randomUUID().toString())
            .put("eventType", type)
            .put("schemaVersion", 1)
            .put("producer", producer)
            .put("occurredAt", Instant.now().toString())
            .put("correlationId", "test");
    event
        .putObject("data")
        .put("submissionId", f.id().toString())
        .put("versionId", f.version().toString())
        .put("versionNo", 1)
        .put("expectedRowVersion", row);
    return event;
  }

  static String status(UUID id) {
    return jdbc.queryForObject(
        "select status::text from submissions where id=:id", Map.of("id", id), String.class);
  }

  static int count(String table, UUID id) {
    return jdbc.queryForObject(
        "select count(*) from " + table + " where submission_id=:id",
        Map.of("id", id),
        Integer.class);
  }
}
