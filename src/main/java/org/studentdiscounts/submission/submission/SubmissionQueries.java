package org.studentdiscounts.submission.submission;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.springframework.stereotype.Service;
import org.studentdiscounts.submission.domain.PayloadJson;
import org.studentdiscounts.submission.domain.SubmissionActor;
import org.studentdiscounts.submission.domain.SubmissionType;
import org.studentdiscounts.submission.integration.BusinessClients;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;

@Service
public class SubmissionQueries {
  private final SubmissionStore store;
  private final SubmissionPermissions permissions;
  private final SubmissionViews views;
  private final ObjectMapper json;
  private final BusinessClients clients;

  public SubmissionQueries(
      SubmissionStore store,
      SubmissionPermissions permissions,
      SubmissionViews views,
      ObjectMapper json,
      BusinessClients clients) {
    this.store = store;
    this.permissions = permissions;
    this.views = views;
    this.json = json;
    this.clients = clients;
  }

  public SubmissionResponse get(SubmissionActor actor, UUID id) {
    var row = store.find(id);
    permissions.requireReadable(actor, row);
    SubmissionResponse response = views.metadata(200, row);
    if (!row.authorId().equals(actor.id())) ((ObjectNode) response.body()).remove("draftPayload");
    return response;
  }

  private int checkedLimit(int limit) {
    if (limit < 1 || limit > 100) throw new SubmissionException(400, "INVALID_PAGINATION");
    return limit;
  }

  public JsonNode list(SubmissionActor actor, UUID before, int limit) {
    var rows = store.own(actor.id(), before, checkedLimit(limit) + 1);
    List<ObjectNode> items = new ArrayList<>();
    for (var row : rows.subList(0, Math.min(limit, rows.size()))) {
      ObjectNode item = json.valueToTree(row);
      item.remove("draftPayload");
      item.remove("currentVersionNo");
      items.add(item);
    }
    return page(items, rows.size() > limit ? rows.get(limit - 1).id().toString() : null);
  }

  private JsonNode page(Object items, Object next) {
    ObjectNode page = json.createObjectNode();
    page.set("items", json.valueToTree(items));
    page.set("nextCursor", json.valueToTree(next));
    return page;
  }

  public JsonNode versions(SubmissionActor actor, UUID id, Integer before, int limit) {
    permissions.requireReadable(actor, store.find(id));
    if (before != null && before < 1) throw new SubmissionException(400, "INVALID_PAGINATION");
    var versions = store.versions(id, before, checkedLimit(limit) + 1);
    return page(
        versions.subList(0, Math.min(limit, versions.size())),
        versions.size() > limit ? versions.get(limit - 1).versionNo() : null);
  }

  public JsonNode version(SubmissionActor actor, UUID id, int number) {
    permissions.requireReadable(actor, store.find(id));
    if (number < 1) throw new SubmissionException(400, "INVALID_PAGINATION");
    return json.valueToTree(store.version(id, number));
  }

  public JsonNode history(SubmissionActor actor, UUID id, UUID before, int limit) {
    permissions.requireReadable(actor, store.find(id));
    var items = store.history(id, before, checkedLimit(limit) + 1);
    return page(
        items.subList(0, Math.min(limit, items.size())),
        items.size() > limit ? items.get(limit - 1).id().toString() : null);
  }

  public JsonNode comparison(SubmissionActor actor, UUID id) {
    var row = store.find(id);
    permissions.requireReadable(actor, row);
    JsonNode current =
        row.draftPayload() != null ? row.draftPayload() : store.latest(row).payload();
    JsonNode publicSnapshot = null;
    Integer base = null;
    try {
      if (row.type() == SubmissionType.CHANGE) {
        var offer = clients.offer(row.targetOfferId());
        publicSnapshot = offer.payload();
        base = offer.rowVersion();
      } else if (row.type() == SubmissionType.NEW_OFFER) {
        var place = clients.place(row.targetPlaceId());
        ObjectNode snapshot = json.createObjectNode();
        ObjectNode placeNode = json.valueToTree(place);
        placeNode.remove("timezone");
        placeNode.remove("rowVersion");
        snapshot.set("place", placeNode);
        publicSnapshot = snapshot;
      }
    } catch (BusinessClients.ReferenceNotFound missing) {
      throw SubmissionException.invalid("targetPlaceId", "TARGET_INVALID");
    }
    ObjectNode result = json.createObjectNode();
    result.set("currentSnapshot", current);
    result.set("publicSnapshot", json.valueToTree(publicSnapshot));
    result.set("catalogRowVersion", json.valueToTree(base));
    result.set(
        "changedFields", json.valueToTree(PayloadJson.changedFields(publicSnapshot, current)));
    return result;
  }

  public JsonNode suggestions(String city, String addressLine) {
    if (city == null
        || city.isBlank()
        || addressLine == null
        || addressLine.isBlank()
        || city.length() > 100
        || addressLine.length() > 300) {
      throw SubmissionException.invalid("city", "INVALID_VALUE");
    }
    return json.valueToTree(clients.suggestions(city.trim(), addressLine.trim()));
  }
}
