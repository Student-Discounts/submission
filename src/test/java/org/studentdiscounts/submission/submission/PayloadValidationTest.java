package org.studentdiscounts.submission.submission;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.studentdiscounts.submission.domain.PayloadJson;
import org.studentdiscounts.submission.domain.SubmissionType;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ObjectNode;

class PayloadValidationTest {
  private final PayloadValidation validator =
      new PayloadValidation(Clock.fixed(Instant.parse("2026-11-05T21:30:00Z"), ZoneOffset.UTC));

  private ObjectNode payload() {
    return (ObjectNode)
        PayloadJson.parse(
            """
            {"place":{"name":" Кофейня «Зерно» ","city":"Санкт-Петербург","addressLine":"Кронверкский пр., 49","categoryCode":"cafe"},
             "offers":[{"conditions":"Обед","discountType":"SPECIAL_PRICE","discountValue":500,"source":{"type":"WEBSITE","url":"https://example.com"},"validUntil":"2026-12-05",
             "schedule":[{"dayOfWeek":5,"timeFrom":"22:00","timeUntil":"24:00"},{"dayOfWeek":6,"timeFrom":"00:00","timeUntil":"02:00"}]}]}
            """);
  }

  private List<PayloadValidation.Violation> errors(JsonNode value, boolean complete) {
    return validator.validate(value, SubmissionType.NEW_PLACE, complete, ZoneOffset.UTC);
  }

  private ObjectNode offer(ObjectNode p) {
    return (ObjectNode) p.path("offers").get(0);
  }

  private void has(JsonNode p, String field, String code) {
    assertThat(errors(p, true))
        .anySatisfy(v -> {
          assertThat(v.field()).isEqualTo(field);
          assertThat(v.code()).isEqualTo(code);
        });
  }

  @Test
  void incompleteDraftIsAllowedButSubmitAggregatesRequiredFields() {
    assertThat(errors(PayloadJson.parse("{}"), false)).isEmpty();
    assertThat(errors(PayloadJson.parse("{}"), true))
        .extracting(PayloadValidation.Violation::field)
        .contains("place.name", "place.city", "place.addressLine", "place.categoryCode", "offers");
  }

  @Test
  void fullTwoOfferSnapshotAndMidnightSplitAreValid() {
    ObjectNode p = payload();
    ((tools.jackson.databind.node.ArrayNode) p.path("offers")).add(offer(p).deepCopy());
    assertThat(errors(p, true)).isEmpty();
  }

  @Test
  void wrongTypesUnknownFieldsAndMetadataAggregateWithoutCrashing() {
    JsonNode p =
        PayloadJson.parse(
            """
            {"authorId":"forged","place":{"name":42,"city":[],"timezone":"UTC","location":{"lat":"x","lon":181}},
             "offers":[null,3,{"conditions":{},"source":[],"schedule":[null,{"dayOfWeek":{},"timeFrom":4}]}],"attachments":[false]}
            """);
    assertThat(errors(p, true)).hasSizeGreaterThan(10);
    assertThat(errors(PayloadJson.parse("[]"), true))
        .extracting(PayloadValidation.Violation::code)
        .contains("INVALID_VALUE");
  }

  @Test
  void providedDraftFieldsStillHaveBounds() {
    ObjectNode p = payload();
    ((ObjectNode) p.path("place")).put("name", "x".repeat(201));
    assertThat(errors(p, false)).extracting(PayloadValidation.Violation::code).contains("TOO_LONG");
    offer(p).put("conditions", " ");
    has(p, "offers[0].conditions", "REQUIRED");
  }

  @Test
  void exactOfferCardinalityDependsOnType() {
    ObjectNode p = payload();
    ((tools.jackson.databind.node.ArrayNode) p.path("offers")).add(offer(p).deepCopy());
    assertThat(validator.validate(p, SubmissionType.CHANGE, true, ZoneOffset.UTC))
        .extracting(PayloadValidation.Violation::code)
        .contains("OFFERS_COUNT_INVALID");
    ((ObjectNode) p).set("offers", PayloadJson.parse("[]"));
    has(p, "offers", "OFFERS_COUNT_INVALID");
  }

  @Test
  void sourcesArePerOfferAndProofIndexMayCoverAll() {
    ObjectNode p = payload();
    offer(p).remove("source");
    has(p, "offers[0].source", "SOURCE_OR_PROOF_REQUIRED");
    p.set(
        "attachments",
        PayloadJson.parse(
            "[{\"mediaAssetId\":\"a50f6565-e35c-4bba-9686-5c9eef790e5c\",\"purpose\":\"PROOF\"}]"));
    assertThat(errors(p, true)).isEmpty();
    ((ObjectNode) p.path("attachments").get(0)).put("offerIndex", 2);
    has(p, "attachments[0].offerIndex", "OUT_OF_RANGE");
    has(p, "offers[0].source", "SOURCE_OR_PROOF_REQUIRED");
  }

  @Test
  void attachmentsRejectDuplicatesBadPurposeAndInvalidUuid() {
    ObjectNode p = payload();
    p.set(
        "attachments",
        PayloadJson.parse(
            """
            [{"mediaAssetId":"a50f6565-e35c-4bba-9686-5c9eef790e5c","purpose":"PHOTO"},
             {"mediaAssetId":"a50f6565-e35c-4bba-9686-5c9eef790e5c","purpose":"PROOF"},
             {"mediaAssetId":"bad","purpose":"OTHER"}]
            """));
    assertThat(errors(p, true))
        .extracting(PayloadValidation.Violation::code)
        .contains("INVALID_VALUE", "INVALID_FORMAT");
  }

  @Test
  void numericPrecisionIsNeverRoundedAndPercentBoundsAreInclusive() {
    ObjectNode p = payload();
    ObjectNode o = offer(p);
    o.put("discountType", "PERCENT");
    for (String invalid : List.of("0.99", "100.01", "999999999.00", "1.001")) {
      o.set("discountValue", PayloadJson.parse(invalid));
      has(p, "offers[0].discountValue", "OUT_OF_RANGE");
    }
    for (String valid : List.of("1", "100", "10.00")) {
      o.set("discountValue", PayloadJson.parse(valid));
      assertThat(errors(p, true)).isEmpty();
    }
    o.put("discountType", "FREE_ITEM");
    has(p, "offers[0].discountValue", "INVALID_VALUE");
  }

  @Test
  void fixedAmountMustBePositiveAndNumericValueIsOptional() {
    ObjectNode p = payload();
    offer(p).put("discountValue", 0);
    has(p, "offers[0].discountValue", "OUT_OF_RANGE");
    offer(p).remove("discountValue");
    assertThat(errors(p, true)).isEmpty();
  }

  @Test
  void sourceUrlMustBeAbsoluteHttpWithHostAndNeverFetched() {
    for (String url : List.of("javascript:evil", "https:missing", "/relative", "https://")) {
      ObjectNode p = payload();
      ((ObjectNode) offer(p).path("source")).put("url", url);
      has(p, "offers[0].source.url", "INVALID_FORMAT");
    }
    ObjectNode p = payload();
    ((ObjectNode) offer(p).path("source")).put("note", "x".repeat(1001));
    has(p, "offers[0].source.note", "TOO_LONG");
  }

  @Test
  void coordinatesAreOptionalButMustBePairedAndBounded() {
    ObjectNode p = payload();
    ObjectNode place = (ObjectNode) p.path("place");
    place.set("location", PayloadJson.parse("{\"lat\":90,\"lon\":-180}"));
    assertThat(errors(p, true)).isEmpty();
    place.set("location", PayloadJson.parse("{\"lat\":91}"));
    has(p, "place.location.lat", "OUT_OF_RANGE");
    has(p, "place.location.lon", "REQUIRED");
  }

  @Test
  void expiryIsInclusiveAndUsesResolvedLocalZone() {
    ObjectNode p = payload();
    offer(p).put("validUntil", "2026-11-05");
    assertThat(errors(p, true)).isEmpty();
    assertThat(validator.validate(p, SubmissionType.NEW_PLACE, true, ZoneId.of("Europe/Moscow")))
        .extracting(PayloadValidation.Violation::code)
        .contains("DATE_RANGE_INVALID");
    offer(p).put("validUntil", "2026-11-04");
    has(p, "offers[0].validUntil", "DATE_RANGE_INVALID");
    offer(p).put("validUntil", "2026-12-05");
    offer(p).put("validFrom", "2027-01-01");
    has(p, "offers[0].validUntil", "DATE_RANGE_INVALID");
    offer(p).put("validFrom", "2020-01-01");
    assertThat(errors(p, true)).isEmpty();
    offer(p).put("validFrom", "2026-02-30");
    has(p, "offers[0].validFrom", "INVALID_FORMAT");
  }

  @Test
  void schedulesPermitAdjacencyButRejectOverlapCrossingAndInvalidTimes() {
    ObjectNode p = payload();
    offer(p)
        .set(
            "schedule",
            PayloadJson.parse(
                """
                [{"dayOfWeek":1,"timeFrom":"10:00","timeUntil":"14:00"},{"dayOfWeek":1,"timeFrom":"14:00","timeUntil":"18:00"}]
                """));
    assertThat(errors(p, true)).isEmpty();
    ((ObjectNode) offer(p).path("schedule").get(1)).put("timeFrom", "13:59");
    has(p, "offers[0].schedule[1]", "SCHEDULE_OVERLAP");
    for (String time : List.of("24:01", "25:00", "1:00", "00:60", "18:00")) {
      offer(p)
          .set(
              "schedule",
              PayloadJson.parse(
                  "[{\"dayOfWeek\":1,\"timeFrom\":\"22:00\",\"timeUntil\":\"" + time + "\"}]"));
      assertThat(errors(p, true))
          .extracting(PayloadValidation.Violation::code)
          .contains("TIME_RANGE_INVALID");
    }
  }

  @Test
  void missingIntervalMembersAreInvalidEvenForDrafts() {
    ObjectNode p = payload();
    offer(p).set("schedule", PayloadJson.parse("[{}]"));
    assertThat(errors(p, false))
        .extracting(PayloadValidation.Violation::field)
        .contains(
            "offers[0].schedule[0].dayOfWeek",
            "offers[0].schedule[0].timeFrom",
            "offers[0].schedule[0].timeUntil");
  }

  @Test
  void normalizedDraftRetainsIntegralScheduleAndAttachmentIndices() {
    ObjectNode p = payload();
    p.set(
        "attachments",
        PayloadJson.parse(
            "[{\"mediaAssetId\":\"a50f6565-e35c-4bba-9686-5c9eef790e5c\",\"purpose\":\"PROOF\",\"offerIndex\":0}]"));
    JsonNode normalized = PayloadJson.normalize(p);
    assertThat(errors(normalized, true)).isEmpty();
    assertThat(
            normalized
                .path("offers")
                .get(0)
                .path("schedule")
                .get(0)
                .path("dayOfWeek")
                .isIntegralNumber())
        .isTrue();
    assertThat(normalized.path("attachments").get(0).path("offerIndex").isIntegralNumber())
        .isTrue();
  }

  @Test
  void normalizationAndHashAreDeterministicWithoutReorderingArrays() {
    JsonNode a =
        PayloadJson.parse(
            "{\"offers\":[{\"conditions\":\" a \",\"discountValue\":10.00}],\"place\":{\"name\":\""
                + " x \",\"city\":\" \"}}");
    JsonNode b =
        PayloadJson.parse(
            "{\"place\":{\"name\":\"x\"},\"offers\":[{\"discountValue\":10.0,\"conditions\":\"a\",\"discountType\":\"OTHER\"}]}");
    assertThat(PayloadJson.hash(a)).isEqualTo(PayloadJson.hash(b)).matches("[0-9a-f]{64}");
    assertThat(
            PayloadJson.hash(
                PayloadJson.parse("{\"offers\":[{\"conditions\":\"a\"},{\"conditions\":\"b\"}]}")))
        .isNotEqualTo(
            PayloadJson.hash(
                PayloadJson.parse("{\"offers\":[{\"conditions\":\"b\"},{\"conditions\":\"a\"}]}")));
  }

  @Test
  void mergePatchRecursesReplacesArraysAndRemovesNulls() {
    JsonNode a = PayloadJson.parse("{\"place\":{\"name\":\"x\",\"city\":\"y\"},\"offers\":[1,2]}");
    JsonNode b =
        PayloadJson.parse("{\"place\":{\"city\":null,\"addressLine\":\"z\"},\"offers\":[3]}");
    assertThat(PayloadJson.mergePatch(a, b))
        .isEqualTo(
            PayloadJson.parse("{\"place\":{\"name\":\"x\",\"addressLine\":\"z\"},\"offers\":[3]}"));
    assertThat(a.path("place").path("city").asText()).isEqualTo("y");
  }

  @Test
  void changedFieldsAreSortedEscapedJsonPointers() {
    assertThat(
            PayloadJson.changedFields(
                PayloadJson.parse("{\"x/y\":1,\"a\":2}"), PayloadJson.parse("{\"x/y\":3,\"b\":4}")))
        .containsExactly("/a", "/b", "/x~1y");
  }

  @Test
  void comparisonTreatsEquivalentDecimalRepresentationsAsUnchanged() {
    assertThat(
            PayloadJson.changedFields(
                PayloadJson.parse("{\"value\":10}"), PayloadJson.parse("{\"value\":10.00}")))
        .isEmpty();
  }

  @Test
  void allTextAndCollectionLimitsAreEnforcedInDraftMode() {
    ObjectNode p = payload();
    ObjectNode place = (ObjectNode) p.path("place");
    place.put("city", "x".repeat(101));
    place.put("addressLine", "x".repeat(301));
    place.put("categoryCode", "x".repeat(51));
    offer(p).put("conditions", "x".repeat(2001));
    ((ObjectNode) offer(p).path("source")).put("url", "https://example.com/" + "x".repeat(2048));
    tools.jackson.databind.node.ArrayNode schedule =
        (tools.jackson.databind.node.ArrayNode) PayloadJson.parse("[]");
    for (int i = 0; i < 29; i++)
      schedule.add(
          PayloadJson.parse("{\"dayOfWeek\":1,\"timeFrom\":\"10:00\",\"timeUntil\":\"11:00\"}"));
    offer(p).set("schedule", schedule);
    tools.jackson.databind.node.ArrayNode attachments =
        (tools.jackson.databind.node.ArrayNode) PayloadJson.parse("[]");
    for (int i = 0; i < 11; i++)
      attachments.add(
          PayloadJson.parse(
              "{\"mediaAssetId\":\"" + java.util.UUID.randomUUID() + "\",\"purpose\":\"PHOTO\"}"));
    p.set("attachments", attachments);
    assertThat(errors(p, false))
        .extracting(PayloadValidation.Violation::field)
        .contains(
            "place.city",
            "place.addressLine",
            "place.categoryCode",
            "offers[0].conditions",
            "offers[0].source.url",
            "offers[0].schedule",
            "attachments");
  }
}
