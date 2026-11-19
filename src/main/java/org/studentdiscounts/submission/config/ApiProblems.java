package org.studentdiscounts.submission.config;

import java.net.URI;
import org.slf4j.MDC;
import org.springframework.http.ProblemDetail;

/** Safe common fields for MVC and pre-controller security errors. */
public final class ApiProblems {
  private static final URI ABOUT_BLANK = URI.create("about:blank");

  private ApiProblems() {}

  public static ProblemDetail create(int status, String code) {
    var problem = ProblemDetail.forStatus(status);
    problem.setType(ABOUT_BLANK);
    problem.setProperty("code", code);
    String correlationId = MDC.get("correlationId");
    if (correlationId != null) problem.setProperty("correlationId", correlationId);
    return problem;
  }
}
