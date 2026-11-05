package org.studentdiscounts.submission.integration;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.security.test.web.servlet.setup.SecurityMockMvcConfigurers.springSecurity;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.options;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.mock.web.MockServletContext;
import org.springframework.test.context.support.TestPropertySourceUtils;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.support.AnnotationConfigWebApplicationContext;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.studentdiscounts.submission.config.PostgresRateLimiter;
import org.studentdiscounts.submission.config.SecurityConfig;
import tools.jackson.databind.json.JsonMapper;

class SecurityHttpContractIT {
  static AnnotationConfigWebApplicationContext context;
  static MockMvc mvc;
  static PostgresRateLimiter limiter;

  @BeforeAll
  static void start() throws Exception {
    AuthContractIT.startJwks();
    context = new AnnotationConfigWebApplicationContext();
    context.setServletContext(new MockServletContext());
    TestPropertySourceUtils.addInlinedPropertiesToEnvironment(
        context,
        "spring.security.oauth2.resourceserver.jwt.issuer-uri=" + AuthContractIT.issuer,
        "spring.security.oauth2.resourceserver.jwt.jwk-set-uri="
            + AuthContractIT.issuer
            + "/.well-known/jwks.json",
        "submission.cors.allowed-origins=http://localhost:5173",
        "submission.dependencies.catalog-base-url=http://localhost:8090",
        "submission.dependencies.media-base-url=http://localhost:8090",
        "submission.dependencies.geo-base-url=http://localhost:8090");
    context.register(Fixture.class);
    context.refresh();
    limiter = context.getBean(PostgresRateLimiter.class);
    mvc = MockMvcBuilders.webAppContextSetup(context).apply(springSecurity()).build();
  }

  @AfterAll
  static void stop() {
    if (context != null) context.close();
    AuthContractIT.stopJwks();
  }

  static String token(List<String> roles) throws Exception {
    return AuthContractIT.sign(
        AuthContractIT.claims().claim("roles", roles).build(), AuthContractIT.key);
  }

  @Test
  void returnsProblemDetailsForMissingInvalidAndInsufficientAuthority() throws Exception {
    mvc.perform(get("/api/v1/probe"))
        .andExpect(status().isUnauthorized())
        .andExpect(content().contentTypeCompatibleWith("application/problem+json"))
        .andExpect(jsonPath("$.code").value("UNAUTHORIZED"));
    mvc.perform(get("/api/v1/probe").header("Authorization", "Bearer nonsense"))
        .andExpect(status().isUnauthorized());
    mvc.perform(
            get("/actuator/prometheus")
                .header("Authorization", "Bearer " + token(List.of("STUDENT"))))
        .andExpect(status().isForbidden())
        .andExpect(jsonPath("$.code").value("FORBIDDEN"));
    mvc.perform(
            get("/actuator/prometheus")
                .header("Authorization", "Bearer " + token(List.of("SENIOR_MODERATOR"))))
        .andExpect(status().isOk());
    mvc.perform(get("/api/v1/probe").header("Authorization", "Bearer " + token(List.of("STUDENT"))))
        .andExpect(status().isOk())
        .andExpect(header().exists("X-Correlation-ID"));
    mvc.perform(get("/unmapped")).andExpect(status().isUnauthorized());
  }

  @Test
  void malformedRequiredClaimsFailAs401AndScopeCannotGrantRolePrivileges() throws Exception {
    for (var bad :
        List.of(
            AuthContractIT.claims().subject("invalid").build(),
            AuthContractIT.claims().claim("sid", null).build(),
            AuthContractIT.claims().claim("roles", "STUDENT").build(),
            AuthContractIT.claims().audience("foreign").build())) {
      mvc.perform(
              get("/api/v1/probe")
                  .header(
                      "Authorization", "Bearer " + AuthContractIT.sign(bad, AuthContractIT.key)))
          .andExpect(status().isUnauthorized())
          .andExpect(content().contentTypeCompatibleWith("application/problem+json"));
    }
    String scopeOnly =
        AuthContractIT.sign(
            AuthContractIT.claims().claim("scope", "SENIOR_MODERATOR").build(), AuthContractIT.key);
    mvc.perform(get("/actuator/prometheus").header("Authorization", "Bearer " + scopeOnly))
        .andExpect(status().isForbidden());
  }

  @Test
  void keepsHealthAndOpenapiPublicAndLimitsPerAuthenticatedUser() throws Exception {
    mvc.perform(get("/actuator/health/readiness")).andExpect(status().isOk());
    mvc.perform(get("/openapi.yaml")).andExpect(status().isOk());
    when(limiter.consume(any())).thenReturn(new PostgresRateLimiter.Decision(false, 37));
    try {
      mvc.perform(
              get("/api/v1/probe").header("Authorization", "Bearer " + token(List.of("STUDENT"))))
          .andExpect(status().isTooManyRequests())
          .andExpect(header().string("Retry-After", "37"))
          .andExpect(jsonPath("$.code").value("RATE_LIMIT_EXCEEDED"));
      mvc.perform(
              get("/actuator/health/readiness")
                  .header("Authorization", "Bearer " + token(List.of("STUDENT"))))
          .andExpect(status().isOk());
    } finally {
      when(limiter.consume(any())).thenReturn(new PostgresRateLimiter.Decision(true, 1));
    }
  }

  @Test
  void permitsOnlyConfiguredCorsOriginsAndExposesConcurrencyHeaders() throws Exception {
    mvc.perform(
            options("/api/v1/probe")
                .header("Origin", "http://localhost:5173")
                .header("Access-Control-Request-Method", "PATCH")
                .header("Access-Control-Request-Headers", "If-Match,Idempotency-Key,Authorization"))
        .andExpect(status().isOk())
        .andExpect(header().string("Access-Control-Allow-Origin", "http://localhost:5173"));
    mvc.perform(
            options("/api/v1/probe")
                .header("Origin", "https://foreign.example")
                .header("Access-Control-Request-Method", "GET"))
        .andExpect(status().isForbidden());
    mvc.perform(
            get("/api/v1/probe")
                .header("Origin", "http://localhost:5173")
                .header("Authorization", "Bearer " + token(List.of("STUDENT"))))
        .andExpect(
            header()
                .string(
                    "Access-Control-Expose-Headers",
                    "ETag, Location, X-Correlation-ID, Retry-After"));
  }

  @Configuration
  @EnableWebMvc
  @Import(SecurityConfig.class)
  static class Fixture {
    @Bean
    JsonMapper mapper() {
      return JsonMapper.builder().build();
    }

    @Bean
    PostgresRateLimiter limiter() {
      var limiter = mock(PostgresRateLimiter.class);
      when(limiter.consume(any())).thenReturn(new PostgresRateLimiter.Decision(true, 1));
      return limiter;
    }

    @Bean
    Probe probe() {
      return new Probe();
    }
  }

  @RestController
  static class Probe {
    @GetMapping({
      "/api/v1/probe",
      "/actuator/prometheus",
      "/actuator/health/readiness",
      "/openapi.yaml"
    })
    String probe() {
      return "ok";
    }
  }
}
