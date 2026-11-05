package org.studentdiscounts.submission.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;
import org.studentdiscounts.submission.config.SubmissionProperties;
import org.studentdiscounts.submission.domain.PayloadJson;

class BusinessClientsTest {
  private HttpServer server;
  private BusinessClients clients;
  private final AtomicReference<String> body = new AtomicReference<>("{}");
  private final AtomicReference<Integer> status = new AtomicReference<>(200);
  private final AtomicReference<String> correlation = new AtomicReference<>();

  @BeforeEach
  void start() throws Exception {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/",
        exchange -> {
          byte[] data = body.get().getBytes(StandardCharsets.UTF_8);
          correlation.set(exchange.getRequestHeaders().getFirst("X-Correlation-ID"));
          exchange.getResponseHeaders().set("Content-Type", "application/json");
          exchange.sendResponseHeaders(status.get(), data.length);
          exchange.getResponseBody().write(data);
          exchange.close();
        });
    server.start();
    String base = "http://127.0.0.1:" + server.getAddress().getPort();
    clients = new BusinessClients(properties(base, true));
  }

  private static SubmissionProperties properties(String base, boolean organizationConfigured) {
    Map<String, String> endpoints = new HashMap<>();
    endpoints.put("submission.dependencies.catalog-base-url", base);
    endpoints.put("submission.dependencies.media-base-url", base);
    endpoints.put("submission.dependencies.geo-base-url", base);
    if (organizationConfigured)
      endpoints.put("submission.dependencies.organizations-base-url", base);
    return new Binder(new MapConfigurationPropertySource(endpoints))
        .bindOrCreate("submission", SubmissionProperties.class);
  }

  @AfterEach
  void stop() {
    server.stop(0);
  }

  @Test
  void categoryComesFromRealHttpAndInactiveIsNotSilentlyActivated() {
    body.set("{\"code\":\"cafe\",\"isActive\":false}");
    assertThat(clients.category("cafe").isActive()).isFalse();
  }

  @Test
  void categoryAndMediaMissingReferencesAreDistinctFromOutages() {
    status.set(404);
    assertThatThrownBy(() -> clients.category("missing"))
        .isInstanceOf(BusinessClients.ReferenceNotFound.class);
    status.set(503);
    assertThatThrownBy(() -> clients.media(UUID.randomUUID()))
        .isInstanceOf(BusinessClients.DependencyUnavailable.class);
  }

  @Test
  void invalidOrMissingTimezoneNeverFallsBackToUtc() {
    body.set("{\"timezone\":\"Europe/Moscow\"}");
    assertThat(clients.timezone(PayloadJson.parse("{\"lat\":59.9,\"lon\":30.3}")))
        .isEqualTo(ZoneId.of("Europe/Moscow"));
    body.set("{\"timezone\":\"fictional/zone\"}");
    assertThatThrownBy(() -> clients.timezone(PayloadJson.parse("{\"lat\":59.9,\"lon\":30.3}")))
        .isInstanceOf(BusinessClients.DependencyUnavailable.class);
  }

  @Test
  void noMatchSuggestionsAreNotInventedCoordinates() {
    body.set("{\"candidates\":[]}");
    assertThat(clients.suggestions("unknown", "unknown").path("candidates")).isEmpty();
  }

  @Test
  void malformedDependencyResponsesFailClosed() {
    body.set("{}");
    assertThatThrownBy(() -> clients.category("cafe"))
        .isInstanceOf(BusinessClients.DependencyUnavailable.class);
    assertThatThrownBy(() -> clients.authorize(UUID.randomUUID(), UUID.randomUUID()))
        .isInstanceOf(BusinessClients.DependencyUnavailable.class);
    assertThatThrownBy(() ->
            new BusinessClients(properties("http://localhost:8090", false))
                .authorize(UUID.randomUUID(), UUID.randomUUID()))
        .isInstanceOf(BusinessClients.DependencyUnavailable.class);
  }

  @Test
  void permissionDenialIsNeverConvertedToSuccess() {
    body.set("{\"allowed\":false,\"verified\":false}");
    assertThat(clients.authorize(UUID.randomUUID(), UUID.randomUUID()).allowed()).isFalse();
  }

  @Test
  void redirectsNeverCountAsSuccessfulDependencyReads() {
    status.set(302);
    body.set("{\"code\":\"cafe\",\"isActive\":true}");
    assertThatThrownBy(() -> clients.category("cafe"))
        .isInstanceOf(BusinessClients.DependencyUnavailable.class);
  }

  @Test
  void missingGeoAndOrganizationEndpointsAreDependencyFailuresNotMissingAssets() {
    status.set(404);
    assertThatThrownBy(() -> clients.suggestions("city", "street"))
        .isInstanceOf(BusinessClients.DependencyUnavailable.class);
    assertThatThrownBy(() -> clients.authorize(UUID.randomUUID(), UUID.randomUUID()))
        .isInstanceOf(BusinessClients.DependencyUnavailable.class);
  }

  @Test
  void typedCatalogAndMediaRecordsPreserveExternalIdentityAndRevisions() {
    UUID id = UUID.randomUUID(), place = UUID.randomUUID(), owner = UUID.randomUUID();
    body.set(
        "{\"id\":\""
            + id
            + "\",\"placeId\":\""
            + place
            + "\",\"status\":\"ACTIVE\",\"rowVersion\":7,\"payload\":{\"place\":{},\"offers\":[{}]}}");
    assertThat(clients.offer(id).rowVersion()).isEqualTo(7);
    assertThat(clients.offer(id).placeId()).isEqualTo(place);
    body.set(
        "{\"id\":\""
            + id
            + "\",\"name\":\"Cafe\",\"city\":\"City\",\"addressLine\":\"Street\",\"location\":{\"lat\":59.9,\"lon\":30.3},\"categoryCode\":\"cafe\",\"timezone\":\"Europe/Moscow\",\"rowVersion\":8}");
    assertThat(clients.place(id).rowVersion()).isEqualTo(8);
    body.set(
        "{\"id\":\""
            + id
            + "\",\"ownerId\":\""
            + owner
            + "\",\"status\":\"QUARANTINED\",\"revision\":9}");
    assertThat(clients.media(id).ownerId()).isEqualTo(owner);
    assertThat(clients.media(id).status()).isEqualTo("QUARANTINED");
    assertThat(clients.media(id).revision()).isEqualTo(9);
    assertThatThrownBy(() -> clients.media(UUID.randomUUID()))
        .isInstanceOf(BusinessClients.DependencyUnavailable.class);
  }

  @Test
  void catalogRowVersionZeroIsValidAndNegativeIsRejected() {
    UUID id = UUID.randomUUID(), place = UUID.randomUUID();
    body.set(
        "{\"id\":\""
            + id
            + "\",\"placeId\":\""
            + place
            + "\",\"status\":\"ACTIVE\",\"rowVersion\":0,\"payload\":{\"place\":{},\"offers\":[{}]}}");
    assertThat(clients.offer(id).rowVersion()).isZero();
    body.set(
        "{\"id\":\""
            + id
            + "\",\"name\":\"Cafe\",\"city\":\"City\",\"addressLine\":\"Street\",\"location\":{\"lat\":59.9,\"lon\":30.3},\"categoryCode\":\"cafe\",\"timezone\":\"Europe/Moscow\",\"rowVersion\":0}");
    assertThat(clients.place(id).rowVersion()).isZero();
    body.set(body.get().replace("\"rowVersion\":0", "\"rowVersion\":-1"));
    assertThatThrownBy(() -> clients.place(id))
        .isInstanceOf(BusinessClients.DependencyUnavailable.class);
  }

  @Test
  void everyBusinessClientPropagatesCurrentCorrelationIdAtRequestTime() {
    UUID id = UUID.randomUUID();
    try {
      MDC.put("correlationId", "business-request-one");
      body.set("{\"code\":\"cafe\",\"isActive\":true}");
      clients.category("cafe");
      assertThat(correlation.get()).isEqualTo("business-request-one");
      MDC.put("correlationId", "business-request-two");
      body.set(
          "{\"id\":\"" + id + "\",\"ownerId\":\"" + id + "\",\"status\":\"CLEAN\",\"revision\":0}");
      clients.media(id);
      assertThat(correlation.get()).isEqualTo("business-request-two");
      body.set("{\"timezone\":\"Europe/Moscow\"}");
      clients.timezone(PayloadJson.parse("{\"lat\":59.9,\"lon\":30.3}"));
      assertThat(correlation.get()).isEqualTo("business-request-two");
      body.set("{\"allowed\":true,\"verified\":true}");
      clients.authorize(id, id);
      assertThat(correlation.get()).isEqualTo("business-request-two");
      MDC.remove("correlationId");
      body.set("{\"code\":\"cafe\",\"isActive\":true}");
      clients.category("cafe");
      assertThat(correlation.get()).isNull();
    } finally {
      MDC.remove("correlationId");
    }
  }
}
