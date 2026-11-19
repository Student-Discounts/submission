package org.studentdiscounts.submission.config;

import java.util.Collection;
import java.util.Objects;
import java.util.UUID;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.oauth2.core.OAuth2TokenValidator;
import org.springframework.security.oauth2.core.OAuth2TokenValidatorResult;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtClaimNames;
import org.springframework.security.oauth2.jwt.JwtClaimValidator;

@Configuration(proxyBeanMethods = false)
public class JwtSecurityConfiguration {
  @Bean
  OAuth2TokenValidator<Jwt> accessTokenExpirationValidator() {
    return new JwtClaimValidator<>(JwtClaimNames.EXP, Objects::nonNull);
  }

  /** The submission domain and per-user rate limiter require a canonical UUID identity. */
  @Bean
  OAuth2TokenValidator<Jwt> submissionSubjectValidator() {
    return new JwtClaimValidator<Object>(
        JwtClaimNames.SUB,
        value -> {
          if (!(value instanceof String subject)) return false;
          try {
            return UUID.fromString(subject).toString().equalsIgnoreCase(subject);
          } catch (IllegalArgumentException invalid) {
            return false;
          }
        });
  }

  /**
   * Check only the representation consumed by Spring's roles converter, not auth's role catalog.
   */
  @Bean
  OAuth2TokenValidator<Jwt> submissionRolesValidator() {
    var validator =
        new JwtClaimValidator<Object>(
            "roles",
            value ->
                value instanceof Collection<?> roles
                    && roles.stream().allMatch(String.class::isInstance));
    return jwt ->
        jwt.hasClaim("roles") ? validator.validate(jwt) : OAuth2TokenValidatorResult.success();
  }
}
