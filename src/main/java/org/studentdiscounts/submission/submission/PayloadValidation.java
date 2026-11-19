package org.studentdiscounts.submission.submission;

import java.math.BigDecimal;
import java.net.URI;
import java.time.Clock;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;
import org.springframework.stereotype.Component;
import org.studentdiscounts.submission.domain.SubmissionType;
import tools.jackson.databind.JsonNode;

/**
 * Pure raw-snapshot validation. Network reference/ownership checks belong to the application layer.
 */
@Component
public final class PayloadValidation {
  public record Violation(String field, String code, String message) {}

  private static final Set<String> DISCOUNTS =
      Set.of("PERCENT", "FIXED_AMOUNT", "SPECIAL_PRICE", "FREE_ITEM", "OTHER");
  private static final Set<String> SOURCES =
      Set.of("WEBSITE", "SOCIAL_MEDIA", "ORGANIZATION", "PERSONAL_VISIT", "OTHER");
  private static final Pattern UUID_FORMAT =
      Pattern.compile(
          "[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}");
  private static final Pattern DATE_FORMAT = Pattern.compile("[0-9]{4}-[0-9]{2}-[0-9]{2}");
  private static final Pattern TIME_FORMAT =
      Pattern.compile("(?:[01][0-9]|2[0-3]):[0-5][0-9]|24:00");
  private static final BigDecimal MAX_DISCOUNT_VALUE = new BigDecimal("99999999.99");
  private final Clock clock;

  public PayloadValidation(Clock clock) {
    this.clock = clock;
  }

  public List<Violation> validate(
      JsonNode payload, SubmissionType type, boolean complete, ZoneId zone) {
    Check check = new Check(complete, zone == null ? ZoneOffset.UTC : zone);
    if (type == null) {
      check.error("type", "INVALID_VALUE");
    }
    if (check.object(payload, "payload", true)) {
      check.members(payload, "", Set.of("place", "offers", "attachments"));
      check.place(payload.path("place"));
      check.offers(payload.path("offers"), type, payload.path("attachments"));
      JsonNode offers = payload.path("offers");
      check.attachments(payload.path("attachments"), offers.isArray() ? offers.size() : 0);
    }
    return List.copyOf(check.errors);
  }

  private final class Check {
    private final boolean complete;
    private final ZoneId zone;
    private final List<Violation> errors = new ArrayList<>();

    private Check(boolean complete, ZoneId zone) {
      this.complete = complete;
      this.zone = zone;
    }

    private void error(String field, String code) {
      errors.add(new Violation(field, code, message(code)));
    }

    private boolean absent(JsonNode value) {
      return value == null
          || value.isMissingNode()
          || value.isNull()
          || (value.isString() && value.asText().isBlank());
    }

    private boolean object(JsonNode value, String path, boolean required) {
      if (absent(value)) {
        if (required) error(path, "REQUIRED");
        return false;
      }
      if (!value.isObject()) {
        error(path, "INVALID_VALUE");
        return false;
      }
      return true;
    }

    private void members(JsonNode value, String path, Set<String> allowed) {
      for (Map.Entry<String, JsonNode> entry : value.properties()) {
        if (!allowed.contains(entry.getKey())) {
          error(path.isEmpty() ? entry.getKey() : path + "." + entry.getKey(), "INVALID_VALUE");
        }
      }
    }

    private String text(JsonNode parent, String name, String path, int max, boolean required) {
      JsonNode value = parent.path(name);
      if (absent(value)) {
        if (required) error(path, "REQUIRED");
        return null;
      }
      if (!value.isString()) {
        error(path, "INVALID_VALUE");
        return null;
      }
      String stripped = value.asText().strip();
      if (stripped.codePointCount(0, stripped.length()) > max) {
        error(path, "TOO_LONG");
      }
      return stripped;
    }

    private String enumeration(
        JsonNode parent, String name, String path, Set<String> allowed, boolean required) {
      String value = text(parent, name, path, Integer.MAX_VALUE, required);
      if (value != null && !allowed.contains(value)) {
        error(path, "INVALID_VALUE");
      }
      return value;
    }

    private void uuid(JsonNode value, String path, boolean required) {
      if (absent(value)) {
        if (required) error(path, "REQUIRED");
        return;
      }
      if (!value.isString() || !UUID_FORMAT.matcher(value.asText()).matches()) {
        error(path, "INVALID_FORMAT");
      }
    }

    private void place(JsonNode place) {
      if (!object(place, "place", false)) {
        if (absent(place) && complete) {
          for (String field : List.of("name", "city", "addressLine", "categoryCode")) {
            error("place." + field, "REQUIRED");
          }
        }
        return;
      }
      members(
          place, "place", Set.of("id", "name", "city", "addressLine", "location", "categoryCode"));
      uuid(place.path("id"), "place.id", false);
      text(place, "name", "place.name", 200, complete);
      text(place, "city", "place.city", 100, complete);
      text(place, "addressLine", "place.addressLine", 300, complete);
      text(place, "categoryCode", "place.categoryCode", 50, complete);
      JsonNode point = place.path("location");
      if (object(point, "place.location", false)) {
        members(point, "place.location", Set.of("lat", "lon"));
        coordinate(point.path("lat"), "place.location.lat", 90);
        coordinate(point.path("lon"), "place.location.lon", 180);
      }
    }

    private void coordinate(JsonNode value, String path, int max) {
      if (absent(value)) {
        error(path, "REQUIRED");
        return;
      }
      if (!value.isNumber()) {
        error(path, "INVALID_VALUE");
        return;
      }
      if (value.decimalValue().abs().compareTo(BigDecimal.valueOf(max)) > 0) {
        error(path, "OUT_OF_RANGE");
      }
    }

    private void offers(JsonNode offers, SubmissionType type, JsonNode attachments) {
      if (absent(offers)) {
        if (complete) error("offers", "OFFERS_COUNT_INVALID");
        return;
      }
      if (!offers.isArray()) {
        error("offers", "INVALID_VALUE");
        return;
      }
      int maximum = type == SubmissionType.CHANGE ? 1 : 5;
      if (offers.size() > maximum || (complete && offers.isEmpty())) {
        error("offers", "OFFERS_COUNT_INVALID");
      }
      for (int i = 0; i < offers.size(); i++) {
        offer(offers.get(i), "offers[" + i + "]", i, attachments);
      }
    }

    private void offer(JsonNode offer, String path, int index, JsonNode attachments) {
      if (!object(offer, path, true)) return;
      members(
          offer,
          path,
          Set.of(
              "id",
              "conditions",
              "discountType",
              "discountValue",
              "source",
              "validFrom",
              "validUntil",
              "schedule"));
      uuid(offer.path("id"), path + ".id", false);
      text(offer, "conditions", path + ".conditions", 2000, complete);
      String discount =
          enumeration(offer, "discountType", path + ".discountType", DISCOUNTS, false);
      discountValue(
          offer.path("discountValue"),
          path + ".discountValue",
          discount == null ? "OTHER" : discount);
      boolean hasSource = source(offer.path("source"), path + ".source");
      if (complete && !hasSource && !hasProof(attachments, index)) {
        error(path + ".source", "SOURCE_OR_PROOF_REQUIRED");
      }
      validityDates(offer, path);
      schedule(offer.path("schedule"), path + ".schedule");
    }

    private void discountValue(JsonNode value, String path, String discount) {
      if (absent(value)) return;
      if (!value.isNumber()) {
        error(path, "INVALID_VALUE");
        return;
      }
      BigDecimal decimal = value.decimalValue().stripTrailingZeros();
      boolean outOfRange = decimal.scale() > 2 || decimal.abs().compareTo(MAX_DISCOUNT_VALUE) > 0;
      if ("PERCENT".equals(discount)) {
        outOfRange |=
            decimal.compareTo(BigDecimal.ONE) < 0 || decimal.compareTo(BigDecimal.valueOf(100)) > 0;
      } else if ("FIXED_AMOUNT".equals(discount) || "SPECIAL_PRICE".equals(discount)) {
        outOfRange |= decimal.signum() <= 0;
      }
      if (outOfRange) error(path, "OUT_OF_RANGE");
      if ("FREE_ITEM".equals(discount) || "OTHER".equals(discount)) {
        error(path, "INVALID_VALUE");
      }
    }

    private boolean source(JsonNode source, String path) {
      if (!object(source, path, false)) return false;
      members(source, path, Set.of("type", "url", "note"));
      String kind = enumeration(source, "type", path + ".type", SOURCES, false);
      String url = text(source, "url", path + ".url", 2048, false);
      String note = text(source, "note", path + ".note", 1000, false);
      if (url != null) sourceUrl(url, path + ".url");
      return kind != null && SOURCES.contains(kind) && (url != null || note != null);
    }

    private void sourceUrl(String url, String path) {
      try {
        URI uri = URI.create(url);
        String scheme = uri.getScheme() == null ? "" : uri.getScheme().toLowerCase(Locale.ROOT);
        if ((!scheme.equals("http") && !scheme.equals("https")) || uri.getHost() == null) {
          error(path, "INVALID_FORMAT");
        }
      } catch (IllegalArgumentException invalid) {
        error(path, "INVALID_FORMAT");
      }
    }

    private boolean hasProof(JsonNode attachments, int index) {
      if (!attachments.isArray()) return false;
      for (JsonNode attachment : attachments) {
        JsonNode offerIndex = attachment.path("offerIndex");
        boolean matchesOffer =
            absent(offerIndex)
                || (offerIndex.isIntegralNumber()
                    && offerIndex.canConvertToInt()
                    && offerIndex.intValue() == index);
        if (attachment.isObject()
            && "PROOF".equals(attachment.path("purpose").asText())
            && !absent(attachment.path("mediaAssetId"))
            && matchesOffer) {
          return true;
        }
      }
      return false;
    }

    private void validityDates(JsonNode offer, String path) {
      LocalDate from = date(offer, "validFrom", path + ".validFrom");
      LocalDate until = date(offer, "validUntil", path + ".validUntil");
      if (until != null
          && ((from != null && until.isBefore(from))
              || until.isBefore(LocalDate.now(clock.withZone(zone))))) {
        error(path + ".validUntil", "DATE_RANGE_INVALID");
      }
    }

    private LocalDate date(JsonNode parent, String name, String path) {
      String raw = text(parent, name, path, Integer.MAX_VALUE, false);
      if (raw == null) return null;
      if (!DATE_FORMAT.matcher(raw).matches()) {
        error(path, "INVALID_FORMAT");
        return null;
      }
      try {
        return LocalDate.parse(raw);
      } catch (DateTimeParseException invalid) {
        error(path, "INVALID_FORMAT");
        return null;
      }
    }

    private void attachments(JsonNode attachments, int offerCount) {
      if (absent(attachments)) return;
      if (!attachments.isArray()) {
        error("attachments", "INVALID_VALUE");
        return;
      }
      if (attachments.size() > 10) error("attachments", "TOO_MANY");
      Set<UUID> seen = new HashSet<>();
      for (int i = 0; i < attachments.size(); i++) {
        attachment(attachments.get(i), "attachments[" + i + "]", offerCount, seen);
      }
    }

    private void attachment(JsonNode attachment, String path, int offerCount, Set<UUID> seen) {
      if (!object(attachment, path, true)) return;
      members(attachment, path, Set.of("mediaAssetId", "purpose", "offerIndex"));
      JsonNode mediaId = attachment.path("mediaAssetId");
      uuid(mediaId, path + ".mediaAssetId", true);
      if (mediaId.isString()) {
        try {
          if (!seen.add(UUID.fromString(mediaId.asText()))) {
            error(path + ".mediaAssetId", "INVALID_VALUE");
          }
        } catch (IllegalArgumentException invalid) {
          // UUID format violations are already collected above.
        }
      }
      enumeration(attachment, "purpose", path + ".purpose", Set.of("PHOTO", "PROOF"), true);
      JsonNode index = attachment.path("offerIndex");
      if (!absent(index)
          && (!index.isIntegralNumber()
              || !index.canConvertToInt()
              || index.intValue() < 0
              || index.intValue() >= offerCount)) {
        error(path + ".offerIndex", "OUT_OF_RANGE");
      }
    }

    private record Interval(int day, int start, int end, int index) {}

    private void schedule(JsonNode schedule, String path) {
      if (absent(schedule)) return;
      if (!schedule.isArray()) {
        error(path, "INVALID_VALUE");
        return;
      }
      if (schedule.size() > 28) error(path, "TOO_MANY");
      List<Interval> intervals = new ArrayList<>();
      for (int i = 0; i < schedule.size(); i++) {
        scheduleInterval(schedule.get(i), path + "[" + i + "]", i, intervals);
      }
      scheduleOverlaps(intervals, path);
    }

    private void scheduleInterval(
        JsonNode interval, String path, int index, List<Interval> intervals) {
      if (!object(interval, path, true)) return;
      members(interval, path, Set.of("dayOfWeek", "timeFrom", "timeUntil"));
      JsonNode day = interval.path("dayOfWeek");
      boolean validDay =
          day.isIntegralNumber()
              && day.canConvertToInt()
              && day.intValue() >= 1
              && day.intValue() <= 7;
      if (!validDay) error(path + ".dayOfWeek", absent(day) ? "REQUIRED" : "OUT_OF_RANGE");
      int start = minutes(interval.path("timeFrom"), path + ".timeFrom", false);
      int end = minutes(interval.path("timeUntil"), path + ".timeUntil", true);
      if (start >= 0 && end >= 0) {
        if (start >= end) {
          error(path + ".timeFrom", "TIME_RANGE_INVALID");
        } else if (validDay) {
          intervals.add(new Interval(day.intValue(), start, end, index));
        }
      }
    }

    private void scheduleOverlaps(List<Interval> intervals, String path) {
      intervals.sort(
          Comparator.comparingInt(Interval::day)
              .thenComparingInt(Interval::start)
              .thenComparingInt(Interval::index));
      int day = -1;
      int maxEnd = -1;
      for (Interval interval : intervals) {
        if (interval.day() != day) {
          day = interval.day();
          maxEnd = -1;
        }
        if (interval.start() < maxEnd) {
          error(path + "[" + interval.index() + "]", "SCHEDULE_OVERLAP");
        }
        maxEnd = Math.max(maxEnd, interval.end());
      }
    }

    private int minutes(JsonNode value, String path, boolean end) {
      if (absent(value)) {
        error(path, "REQUIRED");
        return -1;
      }
      if (!value.isString() || !TIME_FORMAT.matcher(value.asText()).matches()) {
        error(path, "TIME_RANGE_INVALID");
        return -1;
      }
      String time = value.asText();
      int minutes =
          Integer.parseInt(time.substring(0, 2)) * 60 + Integer.parseInt(time.substring(3, 5));
      if ((!end && minutes == 1440) || (end && minutes == 0)) {
        error(path, "TIME_RANGE_INVALID");
        return -1;
      }
      return minutes;
    }
  }

  private static String message(String code) {
    return switch (code) {
      case "REQUIRED" -> "Заполните обязательное поле";
      case "TOO_LONG" -> "Превышена максимальная длина";
      case "SOURCE_OR_PROOF_REQUIRED" -> "Укажите источник или приложите подтверждение";
      case "OFFERS_COUNT_INVALID" -> "Недопустимое количество предложений";
      case "DATE_RANGE_INVALID" -> "Недопустимый срок действия";
      case "TIME_RANGE_INVALID" -> "Недопустимое время интервала";
      case "SCHEDULE_OVERLAP" -> "Интервалы расписания пересекаются";
      case "OUT_OF_RANGE" -> "Значение вне допустимого диапазона";
      case "TOO_MANY" -> "Превышено максимальное количество";
      case "INVALID_FORMAT" -> "Неверный формат значения";
      default -> "Недопустимое значение";
    };
  }
}
