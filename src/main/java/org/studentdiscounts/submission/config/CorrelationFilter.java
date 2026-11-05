package org.studentdiscounts.submission.config;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.UUID;
import java.util.regex.Pattern;
import org.slf4j.MDC;
import org.springframework.web.filter.OncePerRequestFilter;

public final class CorrelationFilter extends OncePerRequestFilter {
  private static final Pattern SAFE_ID = Pattern.compile("[A-Za-z0-9._-]{1,64}");

  @Override
  protected void doFilterInternal(
      HttpServletRequest request, HttpServletResponse response, FilterChain chain)
      throws ServletException, IOException {
    String supplied = request.getHeader("X-Correlation-ID");
    String id =
        supplied != null && SAFE_ID.matcher(supplied).matches()
            ? supplied
            : UUID.randomUUID().toString();
    String previous = MDC.get("correlationId");
    request.setAttribute("correlationId", id);
    response.setHeader("X-Correlation-ID", id);
    MDC.put("correlationId", id);
    try {
      chain.doFilter(request, response);
    } finally {
      if (previous == null) MDC.remove("correlationId");
      else MDC.put("correlationId", previous);
    }
  }
}
