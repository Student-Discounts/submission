package org.studentdiscounts.submission.integration;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;
import org.studentdiscounts.submission.config.SubmissionProperties;

/** A bounded broker acknowledgement occurs while the oldest aggregate event is locked. */
@Component
public class OutboxRelay {
  private static final Logger LOG = LoggerFactory.getLogger(OutboxRelay.class);
  private final OutboxRepository repository;
  private final TransactionTemplate transactions;
  private final KafkaTemplate<String, String> kafka;
  private final boolean enabled;
  private final int batch;
  private final long sendTimeout;
  private final String topic;
  private final LongAdder failures = new LongAdder();

  public OutboxRelay(
      OutboxRepository repository,
      TransactionTemplate transactions,
      KafkaTemplate<String, String> kafka,
      SubmissionProperties properties) {
    this.repository = repository;
    this.transactions = transactions;
    this.kafka = kafka;
    var workers = properties.workers();
    this.enabled = workers.outboxEnabled();
    this.batch = workers.outboxBatchSize();
    this.sendTimeout = workers.outboxSendTimeoutMs().toMillis();
    this.topic = properties.kafka().eventsTopic();
  }

  @Scheduled(fixedDelayString = "${submission.workers.outbox-delay-ms:500}")
  public void scheduled() {
    if (enabled) {
      relayBatch();
    }
  }

  public void relayBatch() {
    for (int i = 0; i < batch; i++) {
      try {
        Boolean sent = transactions.execute(ignored -> relayNext());
        if (!Boolean.TRUE.equals(sent)) {
          return;
        }
      } catch (RuntimeException failure) {
        failures.increment();
        LOG.warn(
            "Outbox delivery remains pending; failureType={}", failure.getClass().getSimpleName());
        return;
      }
    }
  }

  public long failureCount() {
    return failures.sum();
  }

  private boolean relayNext() {
    Optional<OutboxRepository.Event> next = repository.lockNext();
    if (next.isEmpty()) {
      return false;
    }
    OutboxRepository.Event event = next.get();
    send(event);
    repository.markPublished(event.id());
    return true;
  }

  private void send(OutboxRepository.Event event) {
    try {
      kafka
          .send(topic, event.submissionId().toString(), event.payload())
          .get(sendTimeout, TimeUnit.MILLISECONDS);
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      throw new RelayFailure(interrupted);
    } catch (Exception failure) {
      throw new RelayFailure(failure);
    }
  }

  private static final class RelayFailure extends RuntimeException {
    private RelayFailure(Throwable cause) {
      super(cause);
    }
  }
}
