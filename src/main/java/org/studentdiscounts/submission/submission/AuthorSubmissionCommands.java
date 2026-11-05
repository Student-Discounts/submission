package org.studentdiscounts.submission.submission;

import static org.studentdiscounts.submission.submission.SubmissionPermissions.requireEditable;
import static org.studentdiscounts.submission.submission.SubmissionPermissions.requireExpectedVersion;

import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;
import org.studentdiscounts.submission.domain.PayloadJson;
import org.studentdiscounts.submission.domain.SubmissionActor;
import org.studentdiscounts.submission.domain.SubmissionStatus;
import org.studentdiscounts.submission.domain.SubmissionType;
import org.studentdiscounts.submission.integration.BusinessClients;
import org.studentdiscounts.submission.submission.SubmissionPreparation.Prepared;
import org.studentdiscounts.submission.submission.SubmissionStore.VersionRow;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;

@Service
public class AuthorSubmissionCommands {
  private final SubmissionStore store;
  private final SubmissionPermissions permissions;
  private final SubmissionPreparation preparation;
  private final IdempotentCommands idempotency;
  private final SubmissionViews views;
  private final TransactionTemplate transactions;
  private final ObjectMapper json;
  private final BusinessClients clients;

  public AuthorSubmissionCommands(
      SubmissionStore store,
      SubmissionPermissions permissions,
      SubmissionPreparation preparation,
      IdempotentCommands idempotency,
      SubmissionViews views,
      TransactionTemplate transactions,
      ObjectMapper json,
      BusinessClients clients) {
    this.store = store;
    this.permissions = permissions;
    this.preparation = preparation;
    this.idempotency = idempotency;
    this.views = views;
    this.transactions = transactions;
    this.json = json;
    this.clients = clients;
  }

  public SubmissionResponse create(SubmissionActor actor, String key, JsonNode body) {
    String operation = "POST /api/v1/submissions";
    String fingerprint = idempotency.fingerprint(body, null);
    SubmissionResponse replay = idempotency.replay(actor.id(), operation, key, fingerprint);
    if (replay != null) return replay;
    CreateRequest request = createRequest(body);
    permissions.requireAuthorWrite(actor, request.type(), request.place());
    InitialSnapshot initial = initialSnapshot(request);
    ObjectNode merged = preparation.merge(initial.payload(), request.payload());
    preparation.checkTargets(request.place(), request.offer(), merged);
    Prepared prepared =
        preparation.prepare(merged, request.type(), false, actor.id(), request.offer());
    return idempotency.execute(
        actor.id(),
        operation,
        key,
        fingerprint,
        () -> {
          var row =
              store.create(
                  request.type(),
                  actor.id(),
                  request.place(),
                  request.offer(),
                  prepared.payload(),
                  initial.base());
          store.history(row, null, SubmissionStatus.DRAFT, null, actor.id(), null);
          return views.metadata(201, row);
        });
  }

  private record CreateRequest(SubmissionType type, UUID place, UUID offer, JsonNode payload) {}

  private record InitialSnapshot(ObjectNode payload, Integer base) {}

  private CreateRequest createRequest(JsonNode body) {
    if (!body.isObject()) throw SubmissionException.invalid("", "INVALID_VALUE");
    for (String field : body.propertyNames()) {
      if (!Set.of("type", "targetPlaceId", "targetOfferId", "payload").contains(field))
        throw SubmissionException.invalid(field, "INVALID_VALUE");
    }
    JsonNode requestedType = body.path("type");
    UUID place = SubmissionPreparation.optionalId(body, "targetPlaceId");
    UUID offer = SubmissionPreparation.optionalId(body, "targetOfferId");
    if (!requestedType.isString()) throw SubmissionException.invalid("type", "TARGET_INVALID");
    SubmissionType type;
    try {
      type = SubmissionType.valueOf(requestedType.asText());
    } catch (IllegalArgumentException invalidType) {
      throw SubmissionException.invalid("type", "TARGET_INVALID");
    }
    boolean validTarget =
        switch (type) {
          case NEW_PLACE -> place == null && offer == null;
          case NEW_OFFER -> place != null && offer == null;
          case CHANGE -> place != null && offer != null;
        };
    if (!validTarget) throw SubmissionException.invalid("type", "TARGET_INVALID");
    JsonNode payload = body.get("payload");
    if (payload == null || !payload.isObject())
      throw SubmissionException.invalid("payload", "INVALID_VALUE");
    return new CreateRequest(type, place, offer, payload);
  }

  private InitialSnapshot initialSnapshot(CreateRequest request) {
    ObjectNode initial = json.createObjectNode();
    try {
      if (request.type() == SubmissionType.NEW_OFFER) {
        var place = clients.place(request.place());
        ObjectNode value = json.createObjectNode();
        value.put("id", place.id().toString());
        value.put("name", place.name());
        value.put("city", place.city());
        value.put("addressLine", place.addressLine());
        value.put("categoryCode", place.categoryCode());
        if (place.location() != null) value.set("location", place.location());
        initial.set("place", value);
      } else if (request.type() == SubmissionType.CHANGE) {
        var offer = clients.offer(request.offer());
        if (!"ACTIVE".equals(offer.status()) || !request.place().equals(offer.placeId()))
          throw SubmissionException.invalid("targetOfferId", "TARGET_INVALID");
        return new InitialSnapshot(
            (ObjectNode) PayloadJson.normalize(offer.payload()), offer.rowVersion());
      }
      return new InitialSnapshot(initial, null);
    } catch (BusinessClients.ReferenceNotFound missing) {
      throw SubmissionException.invalid("targetPlaceId", "TARGET_INVALID");
    }
  }

  public SubmissionResponse patch(SubmissionActor actor, UUID id, int expected, JsonNode patch) {
    var initial = store.find(id);
    permissions.requireOwner(actor, initial);
    requireExpectedVersion(initial, expected);
    requireEditable(initial);
    permissions.requireAuthorWrite(actor, initial.type(), initial.targetPlaceId());
    if (!patch.isObject()) throw SubmissionException.invalid("", "INVALID_VALUE");
    ObjectNode merged = preparation.merge(initial.draftPayload(), patch);
    preparation.checkTargets(initial.targetPlaceId(), initial.targetOfferId(), merged);
    Prepared prepared =
        preparation.prepare(merged, initial.type(), false, actor.id(), initial.targetOfferId());
    return transactions.execute(ignored -> {
      var row = store.lock(id);
      permissions.requireOwner(actor, row);
      requireExpectedVersion(row, expected);
      requireEditable(row);
      return views.metadata(
          200,
          store.update(
              row,
              row.status(),
              prepared.payload(),
              row.currentVersionNo(),
              row.draftBaseOfferRowVersion()));
    });
  }

  public SubmissionResponse submit(SubmissionActor actor, UUID id, int expected, String key) {
    String operation = "POST /api/v1/submissions/" + id + "/submit";
    String fingerprint = idempotency.fingerprint(json.createObjectNode(), expected);
    SubmissionResponse replay = idempotency.replay(actor.id(), operation, key, fingerprint);
    if (replay != null) return replay;
    var initial = store.find(id);
    permissions.requireOwner(actor, initial);
    if (initial.rowVersion() != expected) {
      // A concurrent identical request may have committed between the first replay read and this
      // row read.
      SubmissionResponse completed = idempotency.replay(actor.id(), operation, key, fingerprint);
      if (completed != null) return completed;
    }
    requireExpectedVersion(initial, expected);
    permissions.requireAuthorWrite(actor, initial.type(), initial.targetPlaceId());
    Prepared prepared =
        initial.status().editable()
            ? preparation.prepare(
                initial.draftPayload(), initial.type(), true, actor.id(), initial.targetOfferId())
            : null;
    return idempotency.execute(
        actor.id(),
        operation,
        key,
        fingerprint,
        () -> {
          var row = store.lock(id);
          permissions.requireOwner(actor, row);
          requireExpectedVersion(row, expected);
          VersionRow version = store.latest(row);
          if (!row.status().editable()) return views.submitted(200, row, version);
          String hash = PayloadJson.hash(prepared.payload());
          boolean changed =
              version == null
                  || !version.contentSha256().equals(hash)
                  || !Objects.equals(version.baseOfferRowVersion(), row.draftBaseOfferRowVersion());
          if (changed)
            version =
                store.append(
                    row,
                    prepared.payload(),
                    hash,
                    row.draftBaseOfferRowVersion(),
                    actor.id(),
                    prepared.zone());
          var submitted =
              store.update(
                  row,
                  SubmissionStatus.SUBMITTED,
                  null,
                  version.versionNo(),
                  row.draftBaseOfferRowVersion());
          for (var asset : prepared.assets())
            store.recordMedia(version, asset.id(), asset.status(), asset.revision());
          store.history(
              submitted, row.status(), SubmissionStatus.SUBMITTED, version, actor.id(), null);
          store.versionEvent(submitted, version, "SubmissionSubmitted");
          return views.submitted(202, submitted, version);
        });
  }

  public SubmissionResponse cancel(SubmissionActor actor, UUID id, int expected) {
    return transactions.execute(ignored -> {
      var row = store.lock(id);
      permissions.requireOwner(actor, row);
      requireExpectedVersion(row, expected);
      if (row.status() != SubmissionStatus.SUBMITTED)
        throw new SubmissionException(409, "INVALID_TRANSITION");
      var version = store.latest(row);
      var cancelled =
          store.update(
              row,
              SubmissionStatus.CANCELLED,
              null,
              row.currentVersionNo(),
              row.draftBaseOfferRowVersion());
      store.history(cancelled, row.status(), cancelled.status(), version, actor.id(), null);
      store.statusEvent(cancelled, row.status(), version, actor.id(), null);
      return views.metadata(200, cancelled);
    });
  }
}
