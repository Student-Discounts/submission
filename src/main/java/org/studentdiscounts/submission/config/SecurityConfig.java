package org.studentdiscounts.submission.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.method.configuration.EnableMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationConverter;
import org.springframework.security.oauth2.server.resource.web.authentication.BearerTokenAuthenticationFilter;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.context.SecurityContextHolderFilter;
import tools.jackson.databind.ObjectMapper;

/**
 * Stateless HTTP authorization and request boundaries; JWT and CORS have dedicated configurations.
 */
@Configuration(proxyBeanMethods = false)
@EnableMethodSecurity
@EnableWebSecurity
@Import({JwtSecurityConfiguration.class, SubmissionCorsConfiguration.class})
public class SecurityConfig {
  @Bean
  SecurityFilterChain securityFilterChain(
      HttpSecurity http,
      JwtDecoder decoder,
      JwtAuthenticationConverter converter,
      ObjectMapper mapper,
      PostgresRateLimiter rateLimiter,
      SubmissionProperties properties)
      throws Exception {
    return http.csrf(AbstractHttpConfigurer::disable)
        .cors(Customizer.withDefaults())
        .sessionManagement(
            session -> session.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
        .requestCache(AbstractHttpConfigurer::disable)
        .formLogin(AbstractHttpConfigurer::disable)
        .httpBasic(AbstractHttpConfigurer::disable)
        .logout(AbstractHttpConfigurer::disable)
        .authorizeHttpRequests(access ->
            access
                .requestMatchers("/actuator/health", "/actuator/health/**", "/openapi.yaml")
                .permitAll()
                .requestMatchers("/actuator/**")
                .hasRole("SENIOR_MODERATOR")
                .anyRequest()
                .authenticated())
        .exceptionHandling(errors ->
            errors
                .authenticationEntryPoint((request, response, failure) -> {
                  response.setHeader("WWW-Authenticate", "Bearer");
                  SecurityProblems.write(
                      mapper,
                      request,
                      response,
                      401,
                      "UNAUTHORIZED",
                      "A valid bearer token is required");
                })
                .accessDeniedHandler((request, response, failure) ->
                    SecurityProblems.write(
                        mapper, request, response, 403, "FORBIDDEN", "Insufficient permissions")))
        .oauth2ResourceServer(resource ->
            resource
                .jwt(jwt -> jwt.decoder(decoder).jwtAuthenticationConverter(converter))
                .authenticationEntryPoint((request, response, failure) -> {
                  response.setHeader("WWW-Authenticate", "Bearer error=\"invalid_token\"");
                  SecurityProblems.write(
                      mapper,
                      request,
                      response,
                      401,
                      "UNAUTHORIZED",
                      "A valid bearer token is required");
                })
                .accessDeniedHandler((request, response, failure) ->
                    SecurityProblems.write(
                        mapper, request, response, 403, "FORBIDDEN", "Insufficient permissions")))
        .addFilterBefore(new CorrelationFilter(), SecurityContextHolderFilter.class)
        .addFilterBefore(
            new RequestBodyLimitFilter(properties.limits().maxBodyBytes(), mapper),
            BearerTokenAuthenticationFilter.class)
        .addFilterAfter(
            new UserRateLimitFilter(rateLimiter, mapper), BearerTokenAuthenticationFilter.class)
        .build();
  }
}
