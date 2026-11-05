package org.studentdiscounts.submission.integration;

import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

@Component
public class WorkflowMetrics {
  public WorkflowMetrics(
      WorkflowRepository workflow,
      OutboxRepository outbox,
      MeterRegistry registry,
      ValidationWorker validation,
      OutboxRelay relay) {
    Gauge.builder("submission.outbox.pending", outbox, OutboxRepository::pendingCount)
        .register(registry);
    Gauge.builder("submission.outbox.oldest.age", outbox, OutboxRepository::oldestPendingAge)
        .baseUnit("seconds")
        .register(registry);
    Gauge.builder(
            "submission.validation.pending", workflow, WorkflowRepository::pendingValidationCount)
        .register(registry);
    Gauge.builder(
            "submission.validation.oldest.age", workflow, WorkflowRepository::oldestValidationAge)
        .baseUnit("seconds")
        .register(registry);
    FunctionCounter.builder(
            "submission.validation.failures", validation, ValidationWorker::failureCount)
        .register(registry);
    FunctionCounter.builder("submission.outbox.failures", relay, OutboxRelay::failureCount)
        .register(registry);
  }
}
