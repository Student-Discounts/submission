package org.studentdiscounts.submission.config;

import java.time.Clock;
import java.util.Collection;
import java.util.UUID;
import org.springframework.security.oauth2.core.OAuth2Error;
import org.springframework.security.oauth2.core.OAuth2TokenValidator;
import org.springframework.security.oauth2.core.OAuth2TokenValidatorResult;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.stereotype.Component;
import org.studentdiscounts.submission.domain.ActorRole;

/** Claims actually issued by the external auth service, in addition to standard JWT validators. */
@Component
public final class AuthTokenContractValidator implements OAuth2TokenValidator<Jwt> {
  private final String audience;
  private final Clock clock;

  public AuthTokenContractValidator(SubmissionProperties properties, Clock clock) {
    this.audience = properties.security().audience();
    this.clock = clock;
  }

  @Override
  public OAuth2TokenValidatorResult validate(Jwt jwt) {
    Object roles = jwt.getClaims().get("roles");
    boolean validRoles =
        roles instanceof Collection<?> values && values.stream().allMatch(this::knownRole);
    boolean validDates =
        jwt.getIssuedAt() != null
            && jwt.getExpiresAt() != null
            && !jwt.getIssuedAt().isAfter(clock.instant().plusSeconds(60))
            && jwt.getExpiresAt().isAfter(jwt.getIssuedAt());
    if (isUuid(jwt.getClaims().get("sub"))
        && isUuid(jwt.getClaims().get("sid"))
        && validRoles
        && validDates
        && jwt.getAudience() != null
        && jwt.getAudience().contains(audience)) {
      return OAuth2TokenValidatorResult.success();
    }
    return OAuth2TokenValidatorResult.failure(
        new OAuth2Error(
            "invalid_token", "Token does not satisfy the authentication contract", null));
  }

  private boolean knownRole(Object value) {
    if (!(value instanceof String role)) return false;
    try {
      ActorRole.valueOf(role);
      return true;
    } catch (IllegalArgumentException invalid) {
      return false;
    }
  }

  private static boolean isUuid(Object value) {
    if (!(value instanceof String text)) return false;
    try {
      return UUID.fromString(text).toString().equalsIgnoreCase(text);
    } catch (IllegalArgumentException invalid) {
      return false;
    }
  }
}
