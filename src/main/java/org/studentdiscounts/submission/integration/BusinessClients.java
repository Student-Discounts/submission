package org.studentdiscounts.submission.integration;

import java.math.BigDecimal;
import java.net.URI;
import java.net.http.HttpClient;
import java.time.Duration;
import java.time.ZoneId;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import org.slf4j.MDC;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientResponseException;
import org.studentdiscounts.submission.config.SubmissionProperties;
import tools.jackson.databind.JsonNode;

/** Consumer contracts for external business services, never an authentication client. */
@Component
public class BusinessClients {
  public record Category(String code, boolean isActive) {}

  public record Place(
      UUID id,
      String name,
      String city,
      String addressLine,
      JsonNode location,
      String categoryCode,
      String timezone,
      int rowVersion) {}

  public record Offer(UUID id, UUID placeId, String status, int rowVersion, JsonNode payload) {}

  public record MediaAsset(UUID id, UUID ownerId, String status, long revision) {}

  public record Authorization(boolean allowed, boolean verified) {}

  public static class ReferenceNotFound extends RuntimeException {
    public ReferenceNotFound() {
      super("External reference not found");
    }
  }

  public static class DependencyUnavailable extends RuntimeException {
    public DependencyUnavailable() {
      super("Business dependency unavailable");
    }

    public DependencyUnavailable(Throwable cause) {
      super("Business dependency unavailable", cause);
    }
  }

  private static final Set<String> MEDIA_STATES =
      Set.of("QUARANTINED", "PROCESSING", "CLEAN", "REJECTED");
  private static final Set<String> OFFER_STATES =
      Set.of("ACTIVE", "EXPIRED", "SUSPENDED", "ARCHIVED");
  private static final Pattern UUID_FORMAT =
      Pattern.compile(
          "[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}");
  private final RestClient catalog;
  private final RestClient media;
  private final RestClient geo;
  private final RestClient organizations;

  public BusinessClients(SubmissionProperties properties) {
    HttpClient http =
        HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(2))
            .followRedirects(HttpClient.Redirect.NEVER)
            .build();
    JdkClientHttpRequestFactory factory = new JdkClientHttpRequestFactory(http);
    factory.setReadTimeout(Duration.ofSeconds(3));
    var dependencies = properties.dependencies();
    catalog = client(dependencies.catalogBaseUrl(), factory);
    media = client(dependencies.mediaBaseUrl(), factory);
    geo = client(dependencies.geoBaseUrl(), factory);
    organizations = client(dependencies.organizationsBaseUrl(), factory);
  }

  private static RestClient client(URI base, JdkClientHttpRequestFactory factory) {
    if (base == null) return null;
    return RestClient.builder()
        .baseUrl(base.toString())
        .requestFactory(factory)
        .requestInterceptor((request, body, execution) -> {
          String correlationId = MDC.get("correlationId");
          if (correlationId != null) {
            request.getHeaders().set("X-Correlation-ID", correlationId);
          }
          return execution.execute(request, body);
        })
        .build();
  }

  public Category category(String code) {
    return checked(() -> {
      JsonNode category = get(catalog, "/catalog/v1/categories/{code}", code);
      String actualCode = text(category, "code");
      if (!actualCode.equals(code)) throw new IllegalArgumentException();
      return new Category(actualCode, bool(category, "isActive"));
    });
  }

  public Place place(UUID id) {
    return checked(() -> {
      JsonNode place = get(catalog, "/catalog/v1/places/{id}", id);
      UUID actualId = uuid(place, "id");
      if (!actualId.equals(id)) throw new IllegalArgumentException();
      JsonNode point = place.path("location");
      if (!point.isNull() && !point.isMissingNode()) location(point);
      String timezone = text(place, "timezone");
      ZoneId.of(timezone);
      return new Place(
          actualId,
          text(place, "name"),
          text(place, "city"),
          text(place, "addressLine"),
          point,
          text(place, "categoryCode"),
          timezone,
          nonnegativeInt(place, "rowVersion"));
    });
  }

  public Offer offer(UUID id) {
    return checked(() -> {
      JsonNode offer = get(catalog, "/catalog/v1/offers/{id}", id);
      UUID actualId = uuid(offer, "id");
      JsonNode payload = offer.path("payload");
      if (!actualId.equals(id) || !payload.isObject()) throw new IllegalArgumentException();
      String status = text(offer, "status");
      if (!OFFER_STATES.contains(status)) throw new IllegalArgumentException();
      return new Offer(
          actualId, uuid(offer, "placeId"), status, nonnegativeInt(offer, "rowVersion"), payload);
    });
  }

  public MediaAsset media(UUID id) {
    return checked(() -> {
      JsonNode asset = get(media, "/media/v1/assets/{id}", id);
      UUID actualId = uuid(asset, "id");
      String status = text(asset, "status");
      if (!actualId.equals(id) || !MEDIA_STATES.contains(status))
        throw new IllegalArgumentException();
      long revision = nonnegativeLong(asset, "revision");
      return new MediaAsset(actualId, uuid(asset, "ownerId"), status, revision);
    });
  }

  public JsonNode suggestions(String city, String addressLine) {
    return checked(() -> {
      JsonNode suggestions =
          getQuery(geo, "/geo/v1/suggestions", "city", city, "addressLine", addressLine);
      JsonNode candidates = suggestions.path("candidates");
      if (!candidates.isArray()) throw new IllegalArgumentException();
      for (JsonNode candidate : candidates) {
        location(candidate.path("location"));
        ZoneId.of(text(candidate, "timezone"));
      }
      return suggestions;
    });
  }

  /** This is always called for coordinate-bearing immutable versions, even with no dates. */
  public ZoneId timezone(JsonNode point) {
    return checked(() -> {
      location(point);
      JsonNode response =
          getQuery(
              geo,
              "/geo/v1/timezone",
              "lat",
              point.path("lat").decimalValue().toPlainString(),
              "lon",
              point.path("lon").decimalValue().toPlainString());
      return ZoneId.of(text(response, "timezone"));
    });
  }

  public Authorization authorize(UUID userId, UUID placeId) {
    return checked(() -> {
      JsonNode authorization =
          getQuery(
              organizations,
              "/organizations/v1/authorizations",
              "userId",
              userId,
              "placeId",
              placeId,
              "action",
              "PROPOSE_CHANGE");
      return new Authorization(bool(authorization, "allowed"), bool(authorization, "verified"));
    });
  }

  private static JsonNode get(RestClient client, String path, Object... variables) {
    if (client == null) throw new DependencyUnavailable();
    return response(client.get().uri(path, variables));
  }

  private static JsonNode getQuery(RestClient client, String path, Object... params) {
    if (client == null) throw new DependencyUnavailable();
    try {
      return response(
          client
              .get()
              .uri(builder -> {
                builder.path(path);
                for (int i = 0; i < params.length; i += 2) {
                  builder.queryParam(params[i].toString(), params[i + 1]);
                }
                return builder.build();
              }));
    } catch (ReferenceNotFound missingEndpoint) {
      // Query endpoints are capabilities, not individual business references.
      throw new DependencyUnavailable(missingEndpoint);
    }
  }

  private static JsonNode response(RestClient.RequestHeadersSpec<?> request) {
    JsonNode body =
        request
            .retrieve()
            .onStatus(
                status -> !status.is2xxSuccessful(),
                (ignored, response) -> {
                  if (response.getStatusCode().value() == 404) throw new ReferenceNotFound();
                  throw new DependencyUnavailable();
                })
            .body(JsonNode.class);
    if (body == null || !body.isObject()) throw new DependencyUnavailable();
    return body;
  }

  private static <T> T checked(Supplier<T> action) {
    try {
      return action.get();
    } catch (ReferenceNotFound | DependencyUnavailable expected) {
      throw expected;
    } catch (RestClientResponseException failure) {
      if (failure.getStatusCode().value() == 404) throw new ReferenceNotFound();
      throw new DependencyUnavailable(failure);
    } catch (RuntimeException failure) {
      throw new DependencyUnavailable(failure);
    }
  }

  private static String text(JsonNode parent, String field) {
    JsonNode value = parent.path(field);
    if (!value.isString() || value.asText().isBlank()) throw new IllegalArgumentException();
    return value.asText();
  }

  private static boolean bool(JsonNode parent, String field) {
    JsonNode value = parent.path(field);
    if (!value.isBoolean()) throw new IllegalArgumentException();
    return value.booleanValue();
  }

  private static UUID uuid(JsonNode parent, String field) {
    String value = text(parent, field);
    if (!UUID_FORMAT.matcher(value).matches()) throw new IllegalArgumentException();
    return UUID.fromString(value);
  }

  private static int nonnegativeInt(JsonNode parent, String field) {
    JsonNode value = parent.path(field);
    if (!value.isIntegralNumber() || !value.canConvertToInt() || value.intValue() < 0) {
      throw new IllegalArgumentException();
    }
    return value.intValue();
  }

  private static long nonnegativeLong(JsonNode parent, String field) {
    JsonNode value = parent.path(field);
    if (!value.isIntegralNumber() || !value.canConvertToLong() || value.longValue() < 0) {
      throw new IllegalArgumentException();
    }
    return value.longValue();
  }

  private static void location(JsonNode point) {
    if (!point.isObject()
        || !point.path("lat").isNumber()
        || !point.path("lon").isNumber()
        || point.path("lat").decimalValue().abs().compareTo(BigDecimal.valueOf(90)) > 0
        || point.path("lon").decimalValue().abs().compareTo(BigDecimal.valueOf(180)) > 0) {
      throw new IllegalArgumentException();
    }
  }
}
