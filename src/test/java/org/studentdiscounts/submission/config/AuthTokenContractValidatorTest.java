package org.studentdiscounts.submission.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.security.oauth2.jwt.Jwt;
import org.studentdiscounts.submission.domain.ActorRole;

class AuthTokenContractValidatorTest {
  private static final Instant NOW = Instant.parse("2030-01-01T00:00:00Z");
  private final AuthTokenContractValidator validator =
      new AuthTokenContractValidator(
          new SubmissionProperties(
              new SubmissionProperties.Security("student-discounts-api"),
              null,
              null,
              null,
              null,
              null),
          Clock.fixed(NOW, ZoneOffset.UTC));

  private Jwt token(Instant issuedAt, Object roles) {
    return Jwt.withTokenValue("test-only-claims")
        .header("alg", "RS256")
        .subject(UUID.randomUUID().toString())
        .claim("sid", UUID.randomUUID().toString())
        .claim("roles", roles)
        .audience(List.of("student-discounts-api"))
        .issuedAt(issuedAt)
        .expiresAt(NOW.plusSeconds(300))
        .build();
  }

  @Test
  void issuedAtClockSkewIsInclusiveAndUsesInjectedClock() {
    assertThat(validator.validate(token(NOW.plusSeconds(60), List.of("STUDENT"))).hasErrors())
        .isFalse();
    assertThat(validator.validate(token(NOW.plusSeconds(61), List.of("STUDENT"))).hasErrors())
        .isTrue();
  }

  @Test
  void roleContractUsesTheAuthoritativeDomainRolesButNeverCoercesArbitraryClaims() {
    for (ActorRole role : ActorRole.values()) {
      assertThat(validator.validate(token(NOW, List.of(role.name()))).hasErrors()).isFalse();
    }
    for (Object roles : List.of("STUDENT", List.of("SERVICE"), List.of("STUDENT", 1))) {
      assertThat(validator.validate(token(NOW, roles)).hasErrors()).isTrue();
    }
  }
}
