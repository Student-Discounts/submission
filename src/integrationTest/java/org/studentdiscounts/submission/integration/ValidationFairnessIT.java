package org.studentdiscounts.submission.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.UUID;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.support.TransactionTemplate;
import org.studentdiscounts.submission.config.SubmissionProperties;
import org.studentdiscounts.submission.domain.PayloadJson;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import tools.jackson.databind.json.JsonMapper;

@Testcontainers
class ValidationFairnessIT {
  @Container static PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:17-bookworm");

  private final Clock clock = Clock.fixed(Instant.parse("2026-11-05T10:00:00Z"), ZoneOffset.UTC);
  private NamedParameterJdbcTemplate jdbc;
  private TransactionTemplate transactions;

  @BeforeEach
  void isolatedDatabase() {
    var dataSource =
        new DriverManagerDataSource(
            postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
    String schema = "validation_" + UUID.randomUUID().toString().replace("-", "");
    new NamedParameterJdbcTemplate(dataSource).getJdbcTemplate().execute("CREATE SCHEMA " + schema);
    dataSource.setUrl(
        postgres.getJdbcUrl()
            + (postgres.getJdbcUrl().contains("?") ? "&" : "?")
            + "currentSchema="
            + schema);
    Flyway.configure().dataSource(dataSource).load().migrate();
    jdbc = new NamedParameterJdbcTemplate(dataSource);
    transactions = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void pendingOrRetryDelayedProofsCannotStarveALaterCompleteSubmission(boolean mediaUnavailable) {
    UUID first = UUID.fromString("00000000-0000-7000-8000-000000000001");
    UUID second = UUID.fromString("00000000-0000-7000-8000-000000000002");
    UUID ready = UUID.fromString("00000000-0000-7000-8000-000000000003");
    UUID firstAsset = UUID.randomUUID();
    UUID secondAsset = UUID.randomUUID();
    submission(first, "VALIDATING", firstAsset);
    submission(second, "VALIDATING", secondAsset);
    submission(ready, "SUBMITTED", null);
    var clients = mock(BusinessClients.class);
    if (mediaUnavailable) {
      when(clients.media(any())).thenThrow(new BusinessClients.DependencyUnavailable());
    } else {
      when(clients.media(firstAsset))
          .thenReturn(
              new BusinessClients.MediaAsset(firstAsset, UUID.randomUUID(), "QUARANTINED", 1));
      when(clients.media(secondAsset))
          .thenReturn(
              new BusinessClients.MediaAsset(secondAsset, UUID.randomUUID(), "PROCESSING", 1));
    }
    var properties =
        new Binder(
                new MapConfigurationPropertySource(
                    Map.of(
                        "submission.dependencies.catalog-base-url", "http://localhost:8090",
                        "submission.dependencies.media-base-url", "http://localhost:8090",
                        "submission.dependencies.geo-base-url", "http://localhost:8090",
                        "submission.workers.validation-batch-size", "2")))
            .bindOrCreate("submission", SubmissionProperties.class);
    var repository = new WorkflowRepository(jdbc, JsonMapper.builder().build(), clock);
    var worker = new ValidationWorker(repository, transactions, clients, clock, properties);

    worker.runBatch();
    assertThat(status(first)).isEqualTo("VALIDATING");
    assertThat(status(second)).isEqualTo("VALIDATING");
    assertThat(status(ready)).isEqualTo("SUBMITTED");
    worker.runBatch();
    assertThat(status(ready)).isEqualTo("PENDING_REVIEW");
    assertThat(status(first)).isEqualTo("VALIDATING");
    assertThat(status(second)).isEqualTo("VALIDATING");
    assertThat(
            jdbc.queryForObject(
                "SELECT count(*) FROM submission_status_history WHERE submission_id=:id AND"
                    + " to_status='PENDING_REVIEW'",
                Map.of("id", ready),
                Integer.class))
        .isEqualTo(1);
    assertThat(repository.pendingValidationIds(first, 2)).containsExactly(second, first);
    assertThat(repository.pendingValidationIds(second, 2)).containsExactly(first, second);
  }

  private String status(UUID id) {
    return jdbc.queryForObject(
        "SELECT status::text FROM submissions WHERE id=:id", Map.of("id", id), String.class);
  }

  private void submission(UUID id, String status, UUID asset) {
    UUID version = UUID.randomUUID();
    UUID author = UUID.randomUUID();
    String attachments =
        asset == null ? "[]" : "[{\"mediaAssetId\":\"" + asset + "\",\"purpose\":\"PROOF\"}]";
    var payload =
        PayloadJson.parse("{\"place\":{},\"offers\":[{}],\"attachments\":" + attachments + "}");
    jdbc.update(
        """
        INSERT INTO submissions(id,type,author_id,draft_payload)
        VALUES(:id,'NEW_PLACE',:author,CAST(:payload AS jsonb))
        """,
        Map.of("id", id, "author", author, "payload", payload.toString()));
    jdbc.update(
        """
        INSERT INTO submission_versions(id,submission_id,version_no,payload,payload_schema_version,content_sha256,created_by)
        VALUES(:version,:id,1,CAST(:payload AS jsonb),1,:hash,:author)
        """,
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
        """
        UPDATE submissions SET status=CAST(:status AS submission_status),current_version_no=1,
            updated_at=TIMESTAMPTZ '2026-11-05 09:00:00+00'
        WHERE id=:id
        """,
        Map.of("status", status, "id", id));
  }
}
