package org.studentdiscounts.submission.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import java.net.URI;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.boot.convert.DurationUnit;
import org.springframework.validation.annotation.Validated;

/**
 * Submission-owned settings. Standard datasource, OAuth and Kafka client settings stay Boot-owned.
 */
@Validated
@ConfigurationProperties("submission")
public record SubmissionProperties(
    @Valid @DefaultValue Security security,
    @Valid @DefaultValue Limits limits,
    @Valid @DefaultValue Cors cors,
    @Valid @DefaultValue Dependencies dependencies,
    @Valid @DefaultValue Workers workers,
    @Valid @DefaultValue Kafka kafka) {

  public record Security(@NotBlank @DefaultValue("student-discounts-api") String audience) {}

  public record Limits(
      @Positive @Max(2147483646) @DefaultValue("262144") int maxBodyBytes,
      @Positive @DefaultValue("120") int requestsPerMinute) {}

  public record Cors(@DefaultValue List<URI> allowedOrigins) {
    public Cors {
      allowedOrigins = allowedOrigins == null ? List.of() : List.copyOf(allowedOrigins);
      for (URI origin : allowedOrigins) {
        requireHttp(origin, "CORS origin");
        if (origin.getRawQuery() != null
            || origin.getRawFragment() != null
            || (origin.getRawPath() != null && !origin.getRawPath().isEmpty())) {
          throw new IllegalArgumentException("CORS origins must be explicit HTTP(S) origins");
        }
      }
    }
  }

  public record Dependencies(
      @NotNull URI catalogBaseUrl,
      @NotNull URI mediaBaseUrl,
      @NotNull URI geoBaseUrl,
      URI organizationsBaseUrl) {
    public Dependencies {
      for (URI endpoint :
          new URI[] {catalogBaseUrl, mediaBaseUrl, geoBaseUrl, organizationsBaseUrl}) {
        if (endpoint != null) {
          requireHttp(endpoint, "Business base URL");
          if (endpoint.getRawQuery() != null || endpoint.getRawFragment() != null) {
            throw new IllegalArgumentException(
                "Business base URLs cannot contain query or fragment");
          }
        }
      }
    }
  }

  public record Workers(
      @DefaultValue("true") boolean validationEnabled,
      @DefaultValue("true") boolean outboxEnabled,
      @DurationUnit(ChronoUnit.MILLIS) @DefaultValue("500ms") Duration validationDelayMs,
      @Positive @DefaultValue("20") int validationBatchSize,
      @DurationUnit(ChronoUnit.MILLIS) @DefaultValue("5000ms") Duration mediaRetryMs,
      @DurationUnit(ChronoUnit.MILLIS) @DefaultValue("500ms") Duration outboxDelayMs,
      @Positive @DefaultValue("50") int outboxBatchSize,
      @DurationUnit(ChronoUnit.MILLIS) @DefaultValue("10000ms") Duration outboxSendTimeoutMs) {
    public Workers {
      positiveMilliseconds(validationDelayMs, "validation-delay-ms");
      positiveMilliseconds(mediaRetryMs, "media-retry-ms");
      positiveMilliseconds(outboxDelayMs, "outbox-delay-ms");
      positiveMilliseconds(outboxSendTimeoutMs, "outbox-send-timeout-ms");
    }
  }

  public record Kafka(
      @NotBlank @DefaultValue("submission.workflow.v1") String workflowTopic,
      @NotBlank @DefaultValue("submission.events.v1") String eventsTopic,
      @NotBlank @DefaultValue("submission-workflow-v1") String groupId,
      @DefaultValue("false") boolean localTopicsEnabled) {}

  static URI requireHttp(URI endpoint, String name) {
    if (endpoint == null
        || !("http".equalsIgnoreCase(endpoint.getScheme())
            || "https".equalsIgnoreCase(endpoint.getScheme()))
        || endpoint.getHost() == null
        || endpoint.getRawUserInfo() != null) {
      throw new IllegalArgumentException(
          name + " must be an absolute HTTP(S) URI without credentials");
    }
    return endpoint;
  }

  private static void positiveMilliseconds(Duration value, String name) {
    if (value == null || value.isNegative() || value.isZero() || value.toMillis() < 1) {
      throw new IllegalArgumentException(name + " must be at least one millisecond");
    }
  }
}
