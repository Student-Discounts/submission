package org.studentdiscounts.submission.domain;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.JsonNodeFactory;
import tools.jackson.databind.node.NullNode;
import tools.jackson.databind.node.ObjectNode;

/** Immutable JSON transformations; array order is always significant. */
public final class PayloadJson {
  private static final JsonMapper MAPPER = JsonMapper.builder().build();
  private static final JsonNodeFactory NODES = JsonNodeFactory.instance;

  private PayloadJson() {}

  public static JsonNode parse(String json) {
    return MAPPER.readTree(json);
  }

  public static JsonNode normalize(JsonNode payload) {
    return normalizeValue(payload, false);
  }

  private static JsonNode normalizeValue(JsonNode value, boolean offer) {
    if (value == null || value.isNull() || value.isMissingNode()) {
      return NullNode.getInstance();
    }
    if (value.isObject()) return normalizeObject(value, offer);
    if (value.isArray()) return normalizeArray(value, false);
    if (value.isString()) return NODES.stringNode(value.asText().strip());
    if (value.isIntegralNumber()) return NODES.numberNode(value.bigIntegerValue());
    if (value.isNumber()) return NODES.numberNode(value.decimalValue().stripTrailingZeros());
    return value.deepCopy();
  }

  private static ObjectNode normalizeObject(JsonNode value, boolean offer) {
    ObjectNode result = NODES.objectNode();
    TreeSet<String> names = new TreeSet<>();
    value.properties().forEach(entry -> names.add(entry.getKey()));
    if (offer) names.add("discountType");
    for (String name : names) {
      JsonNode raw = value.get(name);
      JsonNode child;
      if (offer && name.equals("discountType") && blank(raw)) {
        child = NODES.stringNode("OTHER");
      } else if (name.equals("offers") && raw != null && raw.isArray()) {
        child = normalizeArray(raw, true);
      } else {
        child = normalizeValue(raw, false);
      }
      if (!child.isNull() && !(child.isString() && child.asText().isEmpty())) {
        result.set(name, child);
      }
    }
    return result;
  }

  private static boolean blank(JsonNode value) {
    return value == null || value.isNull() || (value.isString() && value.asText().isBlank());
  }

  private static ArrayNode normalizeArray(JsonNode value, boolean offers) {
    ArrayNode result = NODES.arrayNode();
    for (JsonNode child : value) {
      result.add(normalizeValue(child, offers));
    }
    return result;
  }

  /** Plain decimal notation avoids exponent/scale-dependent content hashes. */
  public static String canonical(JsonNode value) {
    StringBuilder output = new StringBuilder();
    appendCanonical(normalize(value), output);
    return output.toString();
  }

  private static void appendCanonical(JsonNode value, StringBuilder output) {
    if (value.isObject()) {
      appendCanonicalObject(value, output);
    } else if (value.isArray()) {
      appendCanonicalArray(value, output);
    } else if (value.isNumber()) {
      output.append(value.decimalValue().stripTrailingZeros().toPlainString());
    } else {
      output.append(value.toString());
    }
  }

  private static void appendCanonicalObject(JsonNode value, StringBuilder output) {
    output.append('{');
    boolean first = true;
    for (Map.Entry<String, JsonNode> entry : value.properties()) {
      if (!first) output.append(',');
      first = false;
      output.append(MAPPER.writeValueAsString(entry.getKey())).append(':');
      appendCanonical(entry.getValue(), output);
    }
    output.append('}');
  }

  private static void appendCanonicalArray(JsonNode value, StringBuilder output) {
    output.append('[');
    boolean first = true;
    for (JsonNode child : value) {
      if (!first) output.append(',');
      first = false;
      appendCanonical(child, output);
    }
    output.append(']');
  }

  public static String hash(JsonNode payload) {
    try {
      return HexFormat.of()
          .formatHex(
              MessageDigest.getInstance("SHA-256")
                  .digest(canonical(payload).getBytes(StandardCharsets.UTF_8)));
    } catch (NoSuchAlgorithmException impossible) {
      throw new IllegalStateException(impossible);
    }
  }

  public static JsonNode mergePatch(JsonNode target, JsonNode patch) {
    if (patch == null || !patch.isObject()) {
      return patch == null ? NullNode.getInstance() : patch.deepCopy();
    }
    ObjectNode result =
        target != null && target.isObject() ? ((ObjectNode) target).deepCopy() : NODES.objectNode();
    for (Map.Entry<String, JsonNode> entry : patch.properties()) {
      if (entry.getValue().isNull()) {
        result.remove(entry.getKey());
      } else {
        result.set(entry.getKey(), mergePatch(result.get(entry.getKey()), entry.getValue()));
      }
    }
    return result;
  }

  public static ObjectNode merge(ObjectNode target, ObjectNode patch) {
    return (ObjectNode) mergePatch(target, patch);
  }

  public static List<String> changedFields(JsonNode before, JsonNode after) {
    List<String> result = new ArrayList<>();
    diff(normalize(before), normalize(after), "", result);
    result.sort(String::compareTo);
    return List.copyOf(result);
  }

  private static void diff(JsonNode before, JsonNode after, String path, List<String> result) {
    if (before.equals(after)) return;
    if (before.isNumber()
        && after.isNumber()
        && before.decimalValue().compareTo(after.decimalValue()) == 0) {
      return;
    }
    if (before.isObject() && after.isObject()) {
      TreeSet<String> names = new TreeSet<>();
      before.properties().forEach(entry -> names.add(entry.getKey()));
      after.properties().forEach(entry -> names.add(entry.getKey()));
      for (String name : names) {
        diff(before.path(name), after.path(name), path + "/" + escape(name), result);
      }
    } else if (before.isArray() && after.isArray()) {
      for (int i = 0; i < Math.max(before.size(), after.size()); i++) {
        diff(before.path(i), after.path(i), path + "/" + i, result);
      }
    } else {
      result.add(path);
    }
  }

  private static String escape(String name) {
    return name.replace("~", "~0").replace("/", "~1");
  }
}
