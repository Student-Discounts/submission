package org.studentdiscounts.submission.integration;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

@Component
public class WorkflowListener {
  private final WorkflowService workflow;
  private final ObjectMapper json;
  private final Counter failures;

  public WorkflowListener(WorkflowService workflow, ObjectMapper json, MeterRegistry registry) {
    this.workflow = workflow;
    this.json = json;
    this.failures = registry.counter("submission.workflow.consumer.failures");
  }

  @KafkaListener(
      topics = "${submission.kafka.workflow-topic:submission.workflow.v1}",
      groupId = "${submission.kafka.group-id:submission-workflow-v1}",
      containerFactory = "workflowKafkaListenerContainerFactory")
  public void consume(String message) {
    JsonNode event = parse(message);
    String correlation = event == null ? "" : event.path("correlationId").asText();
    String previous = MDC.get("correlationId");
    try {
      if (correlation.matches("[A-Za-z0-9._:-]{1,64}")) {
        MDC.put("correlationId", correlation);
      }
      workflow.process(event);
    } catch (RuntimeException failure) {
      failures.increment();
      throw failure;
    } finally {
      if (previous == null) {
        MDC.remove("correlationId");
      } else {
        MDC.put("correlationId", previous);
      }
    }
  }

  private JsonNode parse(String message) {
    try {
      return json.readTree(message);
    } catch (RuntimeException malformed) {
      failures.increment();
      throw new InvalidWorkflowEvent("MALFORMED_JSON");
    }
  }
}
