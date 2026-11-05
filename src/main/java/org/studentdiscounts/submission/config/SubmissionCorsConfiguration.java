package org.studentdiscounts.submission.config;

import java.net.URI;
import java.util.List;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.cors.CorsConfiguration;
import org.springframework.web.cors.CorsConfigurationSource;
import org.springframework.web.cors.UrlBasedCorsConfigurationSource;

@Configuration(proxyBeanMethods = false)
public class SubmissionCorsConfiguration {
  @Bean
  CorsConfigurationSource corsConfigurationSource(SubmissionProperties properties) {
    var cors = new CorsConfiguration();
    cors.setAllowedOrigins(properties.cors().allowedOrigins().stream().map(URI::toString).toList());
    cors.setAllowedMethods(List.of("GET", "POST", "PATCH", "OPTIONS"));
    cors.setAllowedHeaders(
        List.of(
            "Authorization", "Content-Type", "If-Match", "Idempotency-Key", "X-Correlation-ID"));
    cors.setExposedHeaders(List.of("ETag", "Location", "X-Correlation-ID", "Retry-After"));
    cors.setAllowCredentials(false);
    cors.setMaxAge(3600L);
    var source = new UrlBasedCorsConfigurationSource();
    source.registerCorsConfiguration("/**", cors);
    return source;
  }
}
