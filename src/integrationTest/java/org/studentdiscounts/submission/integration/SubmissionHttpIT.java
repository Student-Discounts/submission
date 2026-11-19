package org.studentdiscounts.submission.integration;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.studentdiscounts.submission.SubmissionServiceApplication;
import org.studentdiscounts.submission.submission.SubmissionStore;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import tools.jackson.databind.ObjectMapper;

@Testcontainers
@SpringBootTest(
    classes = SubmissionServiceApplication.class,
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class SubmissionHttpIT {
  @Container static PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:17-bookworm");

  @Container static KafkaContainer kafka = new KafkaContainer("apache/kafka:4.1.0");

  @DynamicPropertySource
  static void applicationProperties(DynamicPropertyRegistry properties) throws Exception {
    AuthContractIT.startJwks();
    properties.add("spring.datasource.url", postgres::getJdbcUrl);
    properties.add("spring.datasource.username", postgres::getUsername);
    properties.add("spring.datasource.password", postgres::getPassword);
    properties.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    properties.add(
        "spring.security.oauth2.resourceserver.jwt.issuer-uri", () -> AuthContractIT.issuer);
    properties.add(
        "spring.security.oauth2.resourceserver.jwt.jwk-set-uri",
        () -> AuthContractIT.issuer + "/.well-known/jwks.json");
    properties.add("submission.kafka.local-topics-enabled", () -> true);
    properties.add("submission.workers.validation-enabled", () -> false);
    properties.add("submission.workers.outbox-enabled", () -> false);
    // An incomplete NEW_PLACE does not call these dependencies; any accidental call must fail.
    properties.add("submission.dependencies.catalog-base-url", () -> "http://127.0.0.1:1");
    properties.add("submission.dependencies.media-base-url", () -> "http://127.0.0.1:1");
    properties.add("submission.dependencies.geo-base-url", () -> "http://127.0.0.1:1");
    properties.add("submission.dependencies.organizations-base-url", () -> "");
  }

  @AfterAll
  static void stopJwks() {
    AuthContractIT.stopJwks();
  }

  @LocalServerPort int port;

  @Autowired SubmissionStore store;

  @Autowired ObjectMapper json;

  @Autowired JdbcTemplate jdbc;

  @Test
  void createsAndReadsDraftThroughRealHttpAndProxiedRepository() throws Exception {
    // Direct construction cannot detect field access on an uninitialized repository proxy.
    assertThat(AopUtils.isAopProxy(store)).isTrue();
    String token = AuthContractIT.sign(AuthContractIT.claims().build(), AuthContractIT.key);
    URI collection = URI.create("http://127.0.0.1:" + port + "/api/v1/submissions");
    try (var http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()) {
      var request =
          HttpRequest.newBuilder(collection)
              .timeout(Duration.ofSeconds(10))
              .header("Authorization", "Bearer " + token)
              .header("Idempotency-Key", "http-create-" + UUID.randomUUID())
              .header("Content-Type", "application/json")
              .POST(
                  HttpRequest.BodyPublishers.ofString(
                      """
                      {"type":"NEW_PLACE","payload":{"place":{"name":"HTTP draft"}}}
                      """))
              .build();
      var response = http.send(request, HttpResponse.BodyHandlers.ofString());
      assertThat(response.statusCode()).as(response.body()).isEqualTo(201);
      assertThat(response.headers().firstValue("Content-Type"))
          .hasValueSatisfying(value -> assertThat(value).startsWith("application/json"));
      assertThat(response.headers().firstValue("ETag")).hasValue("\"0\"");

      var created = json.readTree(response.body());
      UUID id = UUID.fromString(created.path("id").asText());
      String location = "/api/v1/submissions/" + id;
      assertThat(response.headers().firstValue("Location")).hasValue(location);
      assertThat(created.path("authorId").asText()).isEqualTo(AuthContractIT.USER.toString());
      assertThat(created.path("type").asText()).isEqualTo("NEW_PLACE");
      assertThat(created.path("status").asText()).isEqualTo("DRAFT");
      assertThat(created.path("rowVersion").asInt()).isZero();
      assertThat(created.has("currentVersion")).isTrue();
      assertThat(created.path("currentVersion").isNull()).isTrue();
      assertThat(created.has("currentVersionNo")).isFalse();
      assertThat(created.path("draftPayload").path("place").path("name").asText())
          .isEqualTo("HTTP draft");
      assertThat(created.path("createdAt").asText()).isNotBlank();
      assertThat(created.path("updatedAt").asText()).isNotBlank();
      assertThat(
              jdbc.queryForObject(
                  "SELECT count(*) FROM submissions WHERE id = ? AND author_id = ?",
                  Integer.class,
                  id,
                  AuthContractIT.USER))
          .isEqualTo(1);
      assertThat(
              jdbc.queryForObject(
                  "SELECT count(*) FROM submission_versions WHERE submission_id = ?",
                  Integer.class,
                  id))
          .isZero();

      var get =
          HttpRequest.newBuilder(collection.resolve(location))
              .timeout(Duration.ofSeconds(10))
              .header("Authorization", "Bearer " + token)
              .GET()
              .build();
      var fetched = http.send(get, HttpResponse.BodyHandlers.ofString());
      assertThat(fetched.statusCode()).as(fetched.body()).isEqualTo(200);
      assertThat(fetched.headers().firstValue("ETag")).hasValue("\"0\"");
      assertThat(json.readTree(fetched.body())).isEqualTo(created);
    }
  }
}
