package org.studentdiscounts.submission.integration;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.util.backoff.FixedBackOff;
import org.studentdiscounts.submission.config.SubmissionProperties;

@Configuration(proxyBeanMethods = false)
@EnableKafka
@EnableScheduling
public class WorkflowKafkaConfiguration {
  @Bean
  ConcurrentKafkaListenerContainerFactory<String, String> workflowKafkaListenerContainerFactory(
      ConsumerFactory<String, String> consumers,
      KafkaTemplate<String, String> template,
      MeterRegistry registry,
      SubmissionProperties properties) {
    String topic = properties.kafka().workflowTopic();
    DeadLetterPublishingRecoverer publisher =
        new DeadLetterPublishingRecoverer(
            template, (record, error) -> new TopicPartition(topic + ".DLT", record.partition()));
    publisher.setFailIfSendResultIsError(true);
    publisher.setWaitForSendResultTimeout(Duration.ofSeconds(10));
    Counter failures = registry.counter("submission.workflow.dlt.failures");
    Counter published = registry.counter("submission.workflow.dlt.published");
    DefaultErrorHandler handler =
        new DefaultErrorHandler(
            (record, error) -> {
              try {
                publisher.accept(record, error);
                published.increment();
              } catch (RuntimeException failure) {
                failures.increment();
                throw failure;
              }
            },
            new FixedBackOff(1000L, 2L));
    handler.addNotRetryableExceptions(InvalidWorkflowEvent.class);
    handler.setAckAfterHandle(true);
    ConcurrentKafkaListenerContainerFactory<String, String> factory =
        new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(consumers);
    factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
    factory.setCommonErrorHandler(handler);
    return factory;
  }

  @Bean(destroyMethod = "close")
  AdminClient submissionKafkaAdminClient(KafkaAdmin kafkaAdmin) {
    // Reuse Boot's complete security properties, not just the bootstrap address.
    HashMap<String, Object> properties = new HashMap<>(kafkaAdmin.getConfigurationProperties());
    properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 3000);
    properties.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 3000);
    return AdminClient.create(properties);
  }

  @Bean
  HealthIndicator kafkaHealthIndicator(AdminClient submissionKafkaAdminClient) {
    return () -> {
      try {
        var nodes = submissionKafkaAdminClient.describeCluster().nodes().get(3, TimeUnit.SECONDS);
        return nodes.isEmpty() ? Health.down().build() : Health.up().build();
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
        return Health.down().build();
      } catch (Exception unavailable) {
        return Health.down().build();
      }
    };
  }

  @Bean
  @ConditionalOnProperty(name = "submission.kafka.local-topics-enabled", havingValue = "true")
  KafkaAdmin.NewTopics localWorkflowTopics(SubmissionProperties properties) {
    String workflow = properties.kafka().workflowTopic();
    String events = properties.kafka().eventsTopic();
    return new KafkaAdmin.NewTopics(topic(workflow), topic(events), topic(workflow + ".DLT"));
  }

  private static NewTopic topic(String name) {
    return TopicBuilder.name(name).partitions(1).replicas(1).build();
  }
}
