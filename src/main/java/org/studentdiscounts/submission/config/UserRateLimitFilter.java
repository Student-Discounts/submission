package org.studentdiscounts.submission.config;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.UUID;
import org.springframework.dao.DataAccessException;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;
import org.springframework.web.filter.OncePerRequestFilter;
import tools.jackson.databind.ObjectMapper;

public final class UserRateLimitFilter extends OncePerRequestFilter {
  private final PostgresRateLimiter limiter;
  private final ObjectMapper mapper;

  public UserRateLimitFilter(PostgresRateLimiter limiter, ObjectMapper mapper) {
    this.limiter = limiter;
    this.mapper = mapper;
  }

  @Override
  protected boolean shouldNotFilter(HttpServletRequest request) {
    return request.getServletPath().equals("/actuator/health")
        || request.getServletPath().startsWith("/actuator/health/")
        || request.getRequestURI().equals("/actuator/health")
        || request.getRequestURI().startsWith("/actuator/health/");
  }

  @Override
  protected void doFilterInternal(
      HttpServletRequest request, HttpServletResponse response, FilterChain chain)
      throws ServletException, IOException {
    var authentication = SecurityContextHolder.getContext().getAuthentication();
    if (authentication instanceof JwtAuthenticationToken jwt && jwt.isAuthenticated()) {
      PostgresRateLimiter.Decision decision;
      try {
        decision = limiter.consume(UUID.fromString(jwt.getToken().getSubject()));
      } catch (DataAccessException unavailable) {
        SecurityProblems.write(
            mapper,
            request,
            response,
            503,
            "DEPENDENCY_UNAVAILABLE",
            "Request limiting is temporarily unavailable");
        return;
      }
      if (!decision.allowed()) {
        response.setHeader("Retry-After", Long.toString(decision.retryAfterSeconds()));
        SecurityProblems.write(
            mapper,
            request,
            response,
            429,
            "RATE_LIMIT_EXCEEDED",
            "Request limit exceeded; retry after the indicated delay");
        return;
      }
    }
    chain.doFilter(request, response);
  }
}
