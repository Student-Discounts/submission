package org.studentdiscounts.submission.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.web.cors.CorsConfigurationSource;

class SubmissionPropertiesTest {
  private final ApplicationContextRunner context =
      new ApplicationContextRunner()
          .withUserConfiguration(SubmissionConfiguration.class, SubmissionCorsConfiguration.class)
          .withPropertyValues(
              "submission.dependencies.catalog-base-url=http://catalog.invalid/api",
              "submission.dependencies.media-base-url=http://media.invalid",
              "submission.dependencies.geo-base-url=http://geo.invalid");

  @ParameterizedTest
  @ValueSource(
      strings = {
        "submission.limits.max-body-bytes=0",
        "submission.limits.max-body-bytes=2147483647",
        "submission.limits.requests-per-minute=-1",
        "submission.workers.validation-batch-size=0",
        "submission.workers.outbox-batch-size=-1",
        "submission.workers.validation-delay-ms=0",
        "submission.workers.outbox-delay-ms=-1",
        "submission.workers.media-retry-ms=0",
        "submission.workers.outbox-send-timeout-ms=0.1ms",
        "submission.security.audience=",
        "submission.kafka.workflow-topic=",
        "submission.kafka.group-id="
      })
  void invalidLimitsAndSchedulingCannotStartTheService(String setting) {
    context.withPropertyValues(setting).run(started -> assertThat(started).hasFailed());
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "file:///tmp/catalog",
        "http://user:password@catalog.invalid",
        "http://catalog.invalid?query=x",
        "http://catalog.invalid#fragment",
        "relative/path"
      })
  void unsafeDependencyBasesFailAtStartupNotAtFirstRequest(String endpoint) {
    context
        .withPropertyValues("submission.dependencies.catalog-base-url=" + endpoint)
        .run(started -> assertThat(started).hasFailed());
  }

  @Test
  void productionCannotStartWithoutRequiredBusinessProviders() {
    new ApplicationContextRunner()
        .withUserConfiguration(SubmissionConfiguration.class)
        .run(started -> assertThat(started).hasFailed());
  }

  @Test
  void unconfiguredOrganizationProviderRemainsAbsentAndStartupDoesNotResolveDns() {
    context
        .withPropertyValues("submission.dependencies.organizations-base-url=")
        .run(started -> {
          assertThat(started).hasNotFailed();
          assertThat(
                  started.getBean(SubmissionProperties.class).dependencies().organizationsBaseUrl())
              .isNull();
        });
  }

  @Test
  void legacyNumericMillisecondsDoNotBecomeSeconds() {
    context
        .withPropertyValues(
            "submission.workers.validation-delay-ms=500",
            "submission.workers.media-retry-ms=5000",
            "submission.workers.outbox-send-timeout-ms=10000")
        .run(started -> {
          assertThat(started).hasNotFailed();
          var workers = started.getBean(SubmissionProperties.class).workers();
          assertThat(workers.validationDelayMs()).isEqualTo(Duration.ofMillis(500));
          assertThat(workers.mediaRetryMs()).isEqualTo(Duration.ofSeconds(5));
          assertThat(workers.outboxSendTimeoutMs()).isEqualTo(Duration.ofSeconds(10));
        });
  }

  @Test
  void blankCorsDisablesCrossOriginAccessAndCsvRetainsExplicitOrigins() {
    context
        .withPropertyValues("submission.cors.allowed-origins=")
        .run(started -> {
          assertThat(started).hasNotFailed();
          var cors =
              started
                  .getBean(CorsConfigurationSource.class)
                  .getCorsConfiguration(new MockHttpServletRequest());
          assertThat(cors.checkOrigin("http://localhost:5173")).isNull();
        });
    context
        .withPropertyValues(
            "submission.cors.allowed-origins=http://localhost:5173, https://ui.example")
        .run(started -> {
          assertThat(started).hasNotFailed();
          var cors =
              started
                  .getBean(CorsConfigurationSource.class)
                  .getCorsConfiguration(new MockHttpServletRequest());
          assertThat(cors.checkOrigin("https://ui.example")).isEqualTo("https://ui.example");
          assertThat(cors.checkOrigin("https://other.example")).isNull();
        });
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "*",
        "https://ui.example/path",
        "https://ui.example?query=x",
        "https://user:password@ui.example"
      })
  void corsNeverAcceptsWildcardCredentialsOrUrlPaths(String origin) {
    context
        .withPropertyValues("submission.cors.allowed-origins=" + origin)
        .run(started -> assertThat(started).hasFailed());
  }
}
