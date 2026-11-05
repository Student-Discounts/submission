package org.studentdiscounts.submission.integration;

import java.time.Clock;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;
import org.studentdiscounts.submission.config.SubmissionProperties;
import org.studentdiscounts.submission.domain.SubmissionStatus;

/** Work ownership is the persisted status/current version, never an in-memory queue. */
@Component
public class ValidationWorker {
  private static final Logger LOG = LoggerFactory.getLogger(ValidationWorker.class);
  private static final String CORRELATION = "validation-worker";
  private final WorkflowRepository repository;
  private final TransactionTemplate transactions;
  private final BusinessClients clients;
  private final Clock clock;
  private final boolean enabled;
  private final int batchSize;
  private final long retryMillis;
  private final Map<UUID, Long> retryAfter = new ConcurrentHashMap<>();
  private final LongAdder failures = new LongAdder();
  // Fixed-delay scheduling owns this scan cursor; persisted statuses, not the cursor, own the work.
  private UUID validationCursor;

  public ValidationWorker(
      WorkflowRepository repository,
      TransactionTemplate transactions,
      BusinessClients clients,
      Clock clock,
      SubmissionProperties properties) {
    this.repository = repository;
    this.transactions = transactions;
    this.clients = clients;
    this.clock = clock;
    this.enabled = properties.workers().validationEnabled();
    this.batchSize = properties.workers().validationBatchSize();
    this.retryMillis = properties.workers().mediaRetryMs().toMillis();
  }

  @Scheduled(fixedDelayString = "${submission.workers.validation-delay-ms:500}")
  public void scheduled() {
    if (enabled) {
      runBatch();
    }
  }

  public void runBatch() {
    List<UUID> ids = repository.pendingValidationIds(validationCursor, batchSize);
    if (!ids.isEmpty()) validationCursor = ids.getLast();
    for (UUID id : ids) {
      if (retryAfter.getOrDefault(id, 0L) > clock.millis()) {
        continue;
      }
      try {
        validate(id);
        retryAfter.remove(id);
      } catch (RuntimeException failure) {
        retryAfter.put(id, clock.millis() + retryMillis);
        failures.increment();
        // No payload, file URL, exception message or user token enters the log.
        LOG.warn(
            "Submission validation pending after dependency/database failure; submissionId={}"
                + " failureType={}",
            id,
            failure.getClass().getSimpleName());
      }
    }
    retryAfter.keySet().removeIf(id -> !ids.contains(id) && retryAfter.get(id) <= clock.millis());
  }

  public long failureCount() {
    return failures.sum();
  }

  private void validate(UUID id) {
    WorkflowSubmission pending = transactions.execute(ignored -> beginValidation(id));
    if (pending == null) {
      return;
    }
    // HTTP is deliberately outside the parent-row lock/command transaction.
    Map<UUID, BusinessClients.MediaAsset> observations = new LinkedHashMap<>();
    for (UUID asset : WorkflowRules.assets(pending.payload())) {
      observations.put(asset, clients.media(asset));
    }
    transactions.executeWithoutResult(ignored -> completeValidation(pending, observations));
  }

  private WorkflowSubmission beginValidation(UUID id) {
    WorkflowSubmission row = repository.lock(id);
    if (row.status() == SubmissionStatus.SUBMITTED) {
      repository.transition(row, SubmissionStatus.VALIDATING, null, null, CORRELATION);
      return repository.read(id);
    }
    return row.status() == SubmissionStatus.VALIDATING ? row : null;
  }

  private void completeValidation(
      WorkflowSubmission pending, Map<UUID, BusinessClients.MediaAsset> observations) {
    WorkflowSubmission row = repository.lock(pending.id());
    if (row.status() != SubmissionStatus.VALIDATING
        || !row.versionId().equals(pending.versionId())) {
      return;
    }
    for (BusinessClients.MediaAsset asset : observations.values()) {
      repository.recordMedia(
          row.versionId(),
          new WorkflowEvent.MediaCheck(asset.id(), asset.status(), asset.revision()));
    }
    List<String> statuses = repository.mediaStatuses(row.versionId());
    if (statuses.contains("REJECTED")) {
      repository.transition(row, SubmissionStatus.REJECTED, null, "MEDIA_REJECTED", CORRELATION);
    } else if (statuses.size() == observations.size()
        && statuses.stream().allMatch("CLEAN"::equals)) {
      repository.transition(row, SubmissionStatus.PENDING_REVIEW, null, null, CORRELATION);
    }
  }
}
