package org.studentdiscounts.submission.config;

import java.time.Clock;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(SubmissionProperties.class)
public class SubmissionConfiguration {
  @Bean
  Clock submissionClock() {
    return Clock.systemUTC();
  }
}
