package org.studentdiscounts.submission.submission;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.springframework.stereotype.Component;
import org.studentdiscounts.submission.domain.PayloadJson;
import org.studentdiscounts.submission.domain.SubmissionType;
import org.studentdiscounts.submission.integration.BusinessClients;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;

/** Prepares immutable observations before any command transaction or aggregate lock. */
@Component
public class SubmissionPreparation {
  private final PayloadValidation validation;
  private final BusinessClients clients;
  private final ObjectMapper json;

  public SubmissionPreparation(
      PayloadValidation validation, BusinessClients clients, ObjectMapper json) {
    this.validation = validation;
    this.clients = clients;
    this.json = json;
  }

  public record Prepared(ObjectNode payload, String zone, List<BusinessClients.MediaAsset> assets) {
    public Prepared {
      assets = List.copyOf(assets);
    }
  }

  public static UUID optionalId(JsonNode body, String field) {
    JsonNode value = body.get(field);
    if (value == null || value.isNull()) return null;
    try {
      if (!value.isTextual()) throw new IllegalArgumentException();
      return UUID.fromString(value.asText());
    } catch (IllegalArgumentException invalid) {
      throw SubmissionException.invalid(field, "TARGET_INVALID");
    }
  }

  public ObjectNode merge(JsonNode base, JsonNode patch) {
    JsonNode merged = PayloadJson.mergePatch(base, patch);
    if (!merged.isObject()) throw SubmissionException.invalid("payload", "INVALID_VALUE");
    // Explicit nulls must be validated before normalization removes optional members.
    restoreNullMembers((ObjectNode) merged, patch);
    return (ObjectNode) merged;
  }

  private void restoreNullMembers(ObjectNode merged, JsonNode patch) {
    for (var entry : patch.properties()) {
      if (entry.getValue().isNull()) merged.set(entry.getKey(), entry.getValue());
      else if (entry.getValue().isObject() && merged.path(entry.getKey()).isObject()) {
        restoreNullMembers((ObjectNode) merged.path(entry.getKey()), entry.getValue());
      }
    }
  }

  public void checkTargets(UUID place, UUID offer, JsonNode payload) {
    JsonNode suppliedPlace = payload.path("place").get("id");
    if (suppliedPlace != null
        && (!suppliedPlace.isTextual()
            || place == null
            || !suppliedPlace.asText().equals(place.toString()))) {
      throw SubmissionException.invalid("place.id", "TARGET_INVALID");
    }
    if (place != null
        && (payload.path("place").isMissingNode() || payload.path("place").isNull())) {
      ((ObjectNode) payload).set("place", json.createObjectNode());
    }
    if (place != null && payload.path("place").isObject())
      ((ObjectNode) payload.path("place")).put("id", place.toString());
    JsonNode offers = payload.get("offers");
    if (offers == null || !offers.isArray()) return;
    for (int index = 0; index < offers.size(); index++) {
      JsonNode suppliedOffer = offers.get(index).get("id");
      if (suppliedOffer != null
          && (!suppliedOffer.isTextual()
              || offer == null
              || index != 0
              || !suppliedOffer.asText().equals(offer.toString()))) {
        throw SubmissionException.invalid("offers[" + index + "].id", "TARGET_INVALID");
      }
      if (offer != null && index == 0 && offers.get(index).isObject())
        ((ObjectNode) offers.get(index)).put("id", offer.toString());
    }
  }

  public Prepared prepare(
      JsonNode input, SubmissionType type, boolean complete, UUID author, UUID targetOffer) {
    List<PayloadValidation.Violation> errors =
        new ArrayList<>(validation.validate(input, type, complete, ZoneOffset.UTC));
    ObjectNode normalized =
        input.isObject() ? (ObjectNode) PayloadJson.normalize(input) : json.createObjectNode();
    String zone = null;
    JsonNode location = normalized.path("place").get("location");
    if (complete
        && location != null
        && location.isObject()
        && location.path("lat").isNumber()
        && location.path("lon").isNumber()) {
      ZoneId resolved = clients.timezone(location);
      zone = resolved.getId();
      // Validate raw fields again: normalizing first would erase forbidden null metadata.
      errors = new ArrayList<>(validation.validate(input, type, true, resolved));
    }
    if (complete && normalized.path("place").path("categoryCode").isTextual()) {
      validateCategory(normalized.path("place").path("categoryCode").asText(), errors);
    }
    List<BusinessClients.MediaAsset> assets = observeMedia(normalized, author, targetOffer, errors);
    if (!errors.isEmpty()) throw new SubmissionException(422, "VALIDATION_FAILED", errors);
    return new Prepared(normalized, zone, assets);
  }

  private void validateCategory(String code, List<PayloadValidation.Violation> errors) {
    try {
      if (clients.category(code).isActive()) return;
    } catch (BusinessClients.ReferenceNotFound missing) {
      // Missing and inactive categories share the public field contract.
    }
    errors.add(
        new PayloadValidation.Violation(
            "place.categoryCode", "UNKNOWN_CATEGORY", "Неизвестная категория"));
  }

  private List<BusinessClients.MediaAsset> observeMedia(
      JsonNode payload, UUID author, UUID targetOffer, List<PayloadValidation.Violation> errors) {
    List<BusinessClients.MediaAsset> assets = new ArrayList<>();
    Set<String> publicAssets = null;
    JsonNode attachments = payload.path("attachments");
    if (!attachments.isArray()) return assets;
    for (int index = 0; index < attachments.size(); index++) {
      JsonNode media = attachments.get(index).get("mediaAssetId");
      if (media == null || !media.isTextual()) continue;
      UUID id;
      try {
        id = UUID.fromString(media.asText());
      } catch (IllegalArgumentException invalid) {
        continue;
      }
      try {
        var asset = clients.media(id);
        if (!asset.ownerId().equals(author)) {
          if (targetOffer != null && publicAssets == null) {
            publicAssets = new HashSet<>();
            for (JsonNode reference : clients.offer(targetOffer).payload().path("attachments")) {
              publicAssets.add(reference.path("mediaAssetId").asText());
            }
          }
          if (publicAssets == null || !publicAssets.contains(id.toString()))
            throw new SubmissionException(403, "MEDIA_OWNERSHIP_DENIED");
        }
        assets.add(asset);
      } catch (BusinessClients.ReferenceNotFound missing) {
        errors.add(
            new PayloadValidation.Violation(
                "attachments[" + index + "].mediaAssetId",
                "MEDIA_ASSET_INVALID",
                "Файл не найден"));
      }
    }
    return assets;
  }
}
