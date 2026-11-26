package org.studentdiscounts.submission.integration;

import java.sql.Timestamp;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;
import org.studentdiscounts.submission.config.SubmissionProperties;

@Component
public class PublicationMetadataRelay {
  private static final Logger LOG = LoggerFactory.getLogger(PublicationMetadataRelay.class);

  private record Metadata(UUID id, UUID submissionId, String payload) {}

  private final NamedParameterJdbcTemplate jdbc;
  private final TransactionTemplate transactions;
  private final KafkaTemplate<String, String> kafka;
  private final Clock clock;
  private final boolean enabled;
  private final int batch;
  private final long sendTimeout;
  private final String topic;
  private final LongAdder failures = new LongAdder();

  public PublicationMetadataRelay(
      NamedParameterJdbcTemplate jdbc,
      TransactionTemplate transactions,
      KafkaTemplate<String, String> kafka,
      Clock clock,
      SubmissionProperties properties) {
    this.jdbc = jdbc;
    this.transactions = transactions;
    this.kafka = kafka;
    this.clock = clock;
    var workers = properties.workers();
    this.enabled = workers.outboxEnabled();
    this.batch = workers.outboxBatchSize();
    this.sendTimeout = workers.outboxSendTimeoutMs().toMillis();
    this.topic = properties.kafka().publicationMetadataTopic();
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
            "Publication metadata delivery remains pending; failureType={}",
            failure.getClass().getSimpleName());
        return;
      }
    }
  }

  public long failureCount() {
    return failures.sum();
  }

  private boolean relayNext() {
    List<Metadata> next =
        jdbc.query(
            """
            select id,submission_id,payload::text from publication_metadata_outbox
            where published_at is null
            order by occurred_at,id limit 1 for update skip locked
            """,
            Map.of(),
            (rs, index) ->
                new Metadata(
                    rs.getObject("id", UUID.class),
                    rs.getObject("submission_id", UUID.class),
                    rs.getString("payload")));
    if (next.isEmpty()) {
      return false;
    }
    Metadata metadata = next.getFirst();
    send(metadata);
    jdbc.update(
        "update publication_metadata_outbox set published_at=:now where id=:id",
        Map.of("id", metadata.id(), "now", Timestamp.from(clock.instant())));
    return true;
  }

  private void send(Metadata metadata) {
    try {
      kafka
          .send(topic, metadata.submissionId().toString(), metadata.payload())
          .get(sendTimeout, TimeUnit.MILLISECONDS);
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Publication metadata delivery interrupted", interrupted);
    } catch (Exception failure) {
      throw new IllegalStateException("Publication metadata delivery failed", failure);
    }
  }
}
