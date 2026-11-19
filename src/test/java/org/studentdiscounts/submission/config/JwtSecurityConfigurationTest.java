package org.studentdiscounts.submission.config;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.security.oauth2.jwt.JwtDecoder;

class JwtSecurityConfigurationTest {
  private final ApplicationContextRunner context =
      new ApplicationContextRunner()
          .withUserConfiguration(JwtSecurityConfiguration.class)
          .withPropertyValues(
              "submission.dependencies.catalog-base-url=http://catalog.invalid",
              "submission.dependencies.media-base-url=http://media.invalid",
              "submission.dependencies.geo-base-url=http://geo.invalid",
              "spring.security.oauth2.resourceserver.jwt.issuer-uri=https://auth.invalid/public-issuer",
              "spring.security.oauth2.resourceserver.jwt.jwk-set-uri=https://keys.invalid/private/jwks.json");

  @ParameterizedTest
  @ValueSource(
      strings = {
        "spring.security.oauth2.resourceserver.jwt.issuer-uri=",
        "spring.security.oauth2.resourceserver.jwt.jwk-set-uri=",
        "spring.security.oauth2.resourceserver.jwt.jwk-set-uri=file:///tmp/key.json",
        "spring.security.oauth2.resourceserver.jwt.issuer-uri=http://user:password@auth.invalid"
      })
  void malformedOrMissingJwtEndpointsFailWithoutDiscoveryOrNetworkFallback(String setting) {
    context.withPropertyValues(setting).run(started -> assertThat(started).hasFailed());
  }

  @Test
  void explicitSeparateIssuerAndKeyLocationStartWithoutDiscoveryOrFetchingKeys() {
    context.run(started -> {
      assertThat(started).hasNotFailed();
      assertThat(started).hasSingleBean(JwtDecoder.class);
    });
  }
}
