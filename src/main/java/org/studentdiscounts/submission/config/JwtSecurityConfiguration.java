package org.studentdiscounts.submission.config;

import java.net.URI;
import java.time.Clock;
import java.time.Duration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.security.oauth2.server.resource.autoconfigure.OAuth2ResourceServerProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.security.oauth2.core.DelegatingOAuth2TokenValidator;
import org.springframework.security.oauth2.jose.jws.SignatureAlgorithm;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.JwtIssuerValidator;
import org.springframework.security.oauth2.jwt.JwtTimestampValidator;
import org.springframework.security.oauth2.jwt.NimbusJwtDecoder;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationConverter;
import org.springframework.security.oauth2.server.resource.authentication.JwtGrantedAuthoritiesConverter;
import org.springframework.web.client.RestTemplate;

@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(OAuth2ResourceServerProperties.class)
@Import({SubmissionConfiguration.class, AuthTokenContractValidator.class})
public class JwtSecurityConfiguration {
  @Bean
  public JwtDecoder jwtDecoder(
      OAuth2ResourceServerProperties properties, AuthTokenContractValidator contract, Clock clock) {
    var jwt = properties.getJwt();
    String issuer = requiredEndpoint(jwt.getIssuerUri(), "JWT issuer");
    String jwks = requiredEndpoint(jwt.getJwkSetUri(), "JWT JWK URI");
    var http = new SimpleClientHttpRequestFactory();
    http.setConnectTimeout(Duration.ofSeconds(2));
    http.setReadTimeout(Duration.ofSeconds(3));
    var decoder =
        NimbusJwtDecoder.withJwkSetUri(jwks)
            .jwsAlgorithm(SignatureAlgorithm.RS256)
            .restOperations(new RestTemplate(http))
            .build();
    var timestamp = new JwtTimestampValidator(Duration.ofSeconds(60));
    timestamp.setClock(clock);
    decoder.setJwtValidator(
        new DelegatingOAuth2TokenValidator<>(timestamp, new JwtIssuerValidator(issuer), contract));
    return decoder;
  }

  private static String requiredEndpoint(String value, String name) {
    if (value == null || value.isBlank()) throw new IllegalArgumentException(name + " is required");
    SubmissionProperties.requireHttp(URI.create(value), name);
    return value;
  }

  @Bean
  public JwtAuthenticationConverter jwtAuthenticationConverter() {
    var authorities = new JwtGrantedAuthoritiesConverter();
    authorities.setAuthoritiesClaimName("roles");
    authorities.setAuthorityPrefix("ROLE_");
    var converter = new JwtAuthenticationConverter();
    converter.setJwtGrantedAuthoritiesConverter(authorities);
    return converter;
  }
}
