package org.studentdiscounts.submission.submission;

import static org.studentdiscounts.submission.submission.SubmissionPermissions.requireExpectedVersion;

import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import org.springframework.stereotype.Service;
import org.studentdiscounts.submission.domain.PayloadJson;
import org.studentdiscounts.submission.domain.SubmissionActor;
import org.studentdiscounts.submission.domain.SubmissionStatus;
import org.studentdiscounts.submission.domain.SubmissionType;
import org.studentdiscounts.submission.integration.BusinessClients;
import tools.jackson.databind.JsonNode;

@Service
public class ReviewCorrections {
  private final SubmissionStore store;
  private final SubmissionPermissions permissions;
  private final SubmissionPreparation preparation;
  private final IdempotentCommands idempotency;
  private final SubmissionViews views;
  private final BusinessClients clients;

  public ReviewCorrections(
      SubmissionStore store,
      SubmissionPermissions permissions,
      SubmissionPreparation preparation,
      IdempotentCommands idempotency,
      SubmissionViews views,
      BusinessClients clients) {
    this.store = store;
    this.permissions = permissions;
    this.preparation = preparation;
    this.idempotency = idempotency;
    this.views = views;
    this.clients = clients;
  }

  public SubmissionResponse correct(
      SubmissionActor actor, UUID id, int expected, String key, JsonNode body) {
    permissions.requireReviewer(actor);
    String operation = "POST /api/v1/submissions/" + id + "/versions";
    String fingerprint = idempotency.fingerprint(body, expected);
    SubmissionResponse replay = idempotency.replay(actor.id(), operation, key, fingerprint);
    if (replay != null) return replay;
    var initial = store.find(id);
    permissions.requireOtherReviewer(actor, initial);
    validateFields(body);
    UUID reviewed = SubmissionPreparation.optionalId(body, "reviewedVersionId");
    if (reviewed == null) throw SubmissionException.invalid("reviewedVersionId", "REQUIRED");
    Integer base = checkedCatalogBase(initial, body.get("expectedCatalogRowVersion"));
    JsonNode payload = body.get("payload");
    if (payload == null || !payload.isObject())
      throw SubmissionException.invalid("payload", "REQUIRED");
    payload = payload.deepCopy();
    preparation.checkTargets(initial.targetPlaceId(), initial.targetOfferId(), payload);
    var prepared =
        preparation.prepare(
            payload, initial.type(), true, initial.authorId(), initial.targetOfferId());
    return idempotency.execute(
        actor.id(),
        operation,
        key,
        fingerprint,
        () -> {
          var row = store.lock(id);
          requireExpectedVersion(row, expected);
          if (row.status() != SubmissionStatus.IN_REVIEW)
            throw new SubmissionException(409, "INVALID_TRANSITION");
          var previous = store.latest(row);
          if (!previous.id().equals(reviewed))
            throw new SubmissionException(409, "STALE_REVIEW_VERSION");
          String hash = PayloadJson.hash(prepared.payload());
          if (previous.contentSha256().equals(hash)
              && Objects.equals(previous.baseOfferRowVersion(), base)) {
            return views.version(200, row, previous);
          }
          var version =
              store.append(row, prepared.payload(), hash, base, actor.id(), prepared.zone());
          var corrected = store.update(row, row.status(), null, version.versionNo(), base);
          for (var asset : prepared.assets())
            store.recordMedia(version, asset.id(), asset.status(), asset.revision());
          store.versionEvent(corrected, version, "SubmissionVersionCreated");
          return views.version(201, corrected, version);
        });
  }

  private void validateFields(JsonNode body) {
    if (!body.isObject()) throw SubmissionException.invalid("", "INVALID_VALUE");
    for (String field : body.propertyNames()) {
      if (!Set.of("reviewedVersionId", "payload", "expectedCatalogRowVersion").contains(field)) {
        throw SubmissionException.invalid(field, "INVALID_VALUE");
      }
    }
  }

  private Integer checkedCatalogBase(SubmissionStore.SubmissionRow row, JsonNode expectedBase) {
    if (row.type() != SubmissionType.CHANGE) {
      if (expectedBase != null)
        throw SubmissionException.invalid("expectedCatalogRowVersion", "INVALID_VALUE");
      return null;
    }
    if (expectedBase == null
        || !expectedBase.isIntegralNumber()
        || !expectedBase.canConvertToInt()
        || expectedBase.asInt() < 0) {
      throw SubmissionException.invalid("expectedCatalogRowVersion", "REQUIRED");
    }
    int base = expectedBase.asInt();
    try {
      var offer = clients.offer(row.targetOfferId());
      if (!"ACTIVE".equals(offer.status())
          || !row.targetPlaceId().equals(offer.placeId())
          || offer.rowVersion() != base) {
        throw new SubmissionException(409, "CATALOG_VERSION_CONFLICT");
      }
    } catch (BusinessClients.ReferenceNotFound missing) {
      throw new SubmissionException(409, "CATALOG_VERSION_CONFLICT");
    }
    return base;
  }
}
