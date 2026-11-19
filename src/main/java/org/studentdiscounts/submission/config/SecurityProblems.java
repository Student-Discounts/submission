package org.studentdiscounts.submission.config;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import tools.jackson.databind.ObjectMapper;

final class SecurityProblems {
  private SecurityProblems() {}

  static void write(
      ObjectMapper mapper,
      HttpServletRequest request,
      HttpServletResponse response,
      int status,
      String code,
      String detail)
      throws IOException {
    var problem = ApiProblems.create(status, code);
    problem.setDetail(detail);
    Object correlationId = request.getAttribute("correlationId");
    if (correlationId != null) problem.setProperty("correlationId", correlationId.toString());
    // Filters can run without MVC's ProblemDetail mix-in; flatten extensions explicitly.
    var body = mapper.createObjectNode();
    body.put("type", problem.getType().toString());
    body.put("title", problem.getTitle());
    body.put("status", problem.getStatus());
    body.put("detail", problem.getDetail());
    problem.getProperties().forEach((name, value) -> body.set(name, mapper.valueToTree(value)));
    response.setStatus(status);
    response.setContentType("application/problem+json");
    mapper.writeValue(response.getOutputStream(), body);
  }
}
