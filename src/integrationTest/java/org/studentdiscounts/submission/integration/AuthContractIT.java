package org.studentdiscounts.submission.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.jwk.gen.RSAKeyGenerator;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.security.oauth2.server.resource.autoconfigure.OAuth2ResourceServerProperties;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.JwtException;
import org.studentdiscounts.submission.config.AuthTokenContractValidator;
import org.studentdiscounts.submission.config.JwtSecurityConfiguration;
import org.studentdiscounts.submission.config.SubmissionProperties;

class AuthContractIT {
  static HttpServer jwks;
  static RSAKey key;
  static JwtDecoder decoder;
  static String issuer;
  static final UUID USER = UUID.randomUUID();
  static final UUID SESSION = UUID.randomUUID();

  @BeforeAll
  static void startJwks() throws Exception {
    key = new RSAKeyGenerator(2048).keyID("auth-contract").generate();
    jwks = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    jwks.createContext(
        "/.well-known/jwks.json",
        exchange -> {
          byte[] body = new JWKSet(key.toPublicJWK()).toString().getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().set("Content-Type", "application/json");
          exchange.sendResponseHeaders(200, body.length);
          try (var stream = exchange.getResponseBody()) {
            stream.write(body);
          }
        });
    jwks.start();
    issuer = "http://127.0.0.1:" + jwks.getAddress().getPort();
    var oauth = new OAuth2ResourceServerProperties();
    oauth.getJwt().setIssuerUri(issuer);
    oauth.getJwt().setJwkSetUri(issuer + "/.well-known/jwks.json");
    var settings =
        new SubmissionProperties(
            new SubmissionProperties.Security("student-discounts-api"),
            null,
            null,
            null,
            null,
            null);
    var clock = Clock.systemUTC();
    decoder =
        new JwtSecurityConfiguration()
            .jwtDecoder(oauth, new AuthTokenContractValidator(settings, clock), clock);
  }

  @AfterAll
  static void stopJwks() {
    if (jwks != null) jwks.stop(0);
  }

  static JWTClaimsSet.Builder claims() {
    return new JWTClaimsSet.Builder()
        .issuer(issuer)
        .subject(USER.toString())
        .audience("student-discounts-api")
        .claim("sid", SESSION.toString())
        .claim("roles", List.of("STUDENT"))
        .issueTime(Date.from(Instant.now()))
        .expirationTime(Date.from(Instant.now().plusSeconds(300)));
  }

  static String sign(JWTClaimsSet claims, RSAKey signingKey) throws JOSEException {
    var token =
        new SignedJWT(
            new JWSHeader.Builder(JWSAlgorithm.RS256).keyID(key.getKeyID()).build(), claims);
    token.sign(new RSASSASigner(signingKey));
    return token.serialize();
  }

  @Test
  void acceptsExactAuthContractWithVersionFourSubjectAndSession() throws Exception {
    var jwt = decoder.decode(sign(claims().build(), key));
    assertThat(jwt.getSubject()).isEqualTo(USER.toString());
    assertThat(jwt.getClaimAsString("sid")).isEqualTo(SESSION.toString());
    assertThat(
            new JwtSecurityConfiguration()
                .jwtAuthenticationConverter()
                .convert(jwt)
                .getAuthorities())
        .filteredOn(authority -> authority.getAuthority().startsWith("ROLE_"))
        .extracting("authority")
        .containsExactly("ROLE_STUDENT");
  }

  @Test
  void rejectsInvalidIdentityAudienceIssuerExpirationAndSignature() throws Exception {
    var invalid =
        List.of(
            claims().subject("not-a-uuid").build(),
            claims().subject(null).build(),
            claims().subject("1-1-1-1-1").build(),
            claims().claim("sid", "not-a-uuid").build(),
            claims().claim("sid", null).build(),
            claims().claim("sid", 42).build(),
            claims().claim("roles", "STUDENT").build(),
            claims().claim("roles", List.of("STUDENT", 1)).build(),
            claims().claim("roles", List.of("SERVICE")).build(),
            claims().claim("roles", null).build(),
            claims().audience("another-service").build(),
            claims().audience((String) null).build(),
            claims().issuer("http://wrong-issuer").build(),
            claims().expirationTime(Date.from(Instant.now().minusSeconds(120))).build(),
            claims().issueTime(null).build(),
            claims().issueTime(Date.from(Instant.now().plusSeconds(120))).build(),
            claims().expirationTime(null).build());
    for (var bad : invalid) {
      String token = sign(bad, key);
      assertThatThrownBy(() -> decoder.decode(token))
          .as(bad.toString())
          .isInstanceOf(JwtException.class);
    }
    String forged = sign(claims().build(), new RSAKeyGenerator(2048).generate());
    assertThatThrownBy(() -> decoder.decode(forged)).isInstanceOf(JwtException.class);
    var wrongAlgorithm =
        new SignedJWT(
            new JWSHeader.Builder(JWSAlgorithm.RS512).keyID(key.getKeyID()).build(),
            claims().build());
    wrongAlgorithm.sign(new RSASSASigner(key));
    assertThatThrownBy(() -> decoder.decode(wrongAlgorithm.serialize()))
        .isInstanceOf(JwtException.class);
  }

  @Test
  void representativeAndSeniorModeratorUseExactAuthorityNames() throws Exception {
    var jwt =
        decoder.decode(
            sign(
                claims().claim("roles", List.of("ORG_REPRESENTATIVE", "SENIOR_MODERATOR")).build(),
                key));
    assertThat(
            new JwtSecurityConfiguration()
                .jwtAuthenticationConverter()
                .convert(jwt)
                .getAuthorities())
        .filteredOn(authority -> authority.getAuthority().startsWith("ROLE_"))
        .extracting("authority")
        .containsExactlyInAnyOrder("ROLE_ORG_REPRESENTATIVE", "ROLE_SENIOR_MODERATOR");
  }

  @Test
  void publicIssuerAndReachableJwksLocationAreIndependent() throws Exception {
    String publicIssuer = "https://auth.example/tenant";
    var oauth = new OAuth2ResourceServerProperties();
    oauth.getJwt().setIssuerUri(publicIssuer);
    oauth.getJwt().setJwkSetUri(issuer + "/.well-known/jwks.json");
    var settings =
        new SubmissionProperties(
            new SubmissionProperties.Security("student-discounts-api"),
            null,
            null,
            null,
            null,
            null);
    var clock = Clock.systemUTC();
    var separateDecoder =
        new JwtSecurityConfiguration()
            .jwtDecoder(oauth, new AuthTokenContractValidator(settings, clock), clock);
    assertThat(
            separateDecoder.decode(sign(claims().issuer(publicIssuer).build(), key)).getSubject())
        .isEqualTo(USER.toString());
    assertThatThrownBy(() -> separateDecoder.decode(sign(claims().build(), key)))
        .isInstanceOf(JwtException.class);
  }
}
