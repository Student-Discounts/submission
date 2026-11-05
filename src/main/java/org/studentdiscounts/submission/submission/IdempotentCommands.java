package org.studentdiscounts.submission.submission;

import java.util.UUID;
import java.util.function.Supplier;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;
import org.studentdiscounts.submission.domain.PayloadJson;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;

/** Owns the reservation/business-write/response-finalization atomicity invariant. */
@Component
public class IdempotentCommands {
  private final SubmissionStore store;
  private final TransactionTemplate transactions;
  private final ObjectMapper json;

  public IdempotentCommands(
      SubmissionStore store, TransactionTemplate transactions, ObjectMapper json) {
    this.store = store;
    this.transactions = transactions;
    this.json = json;
  }

  public static String requestKey(String key) {
    if (key == null
        || key.isEmpty()
        || key.length() > 128
        || key.chars().anyMatch(c -> c < 32 || c > 126)) {
      throw new SubmissionException(400, "INVALID_IDEMPOTENCY_KEY");
    }
    return key;
  }

  public String fingerprint(JsonNode body, Integer expected) {
    ObjectNode request = json.createObjectNode();
    request.set("body", body);
    if (expected != null) request.put("ifMatch", expected);
    return PayloadJson.hash(request);
  }

  public SubmissionResponse replay(UUID actor, String operation, String key, String fingerprint) {
    requestKey(key);
    return store.completedRequest(actor, operation, key, fingerprint);
  }

  public SubmissionResponse execute(
      UUID actor,
      String operation,
      String key,
      String fingerprint,
      Supplier<SubmissionResponse> command) {
    return transactions.execute(ignored -> {
      // INSERT waits for a competing reservation transaction before consulting its committed
      // response.
      if (!store.reserveRequest(actor, operation, key, fingerprint)) {
        return store.completedRequest(actor, operation, key, fingerprint);
      }
      SubmissionResponse response = command.get();
      return store.completeRequest(actor, operation, key, response);
    });
  }
}
