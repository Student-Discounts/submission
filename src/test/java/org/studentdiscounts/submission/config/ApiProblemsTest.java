package org.studentdiscounts.submission.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

class ApiProblemsTest {
  @Test
  void sharesStandardProblemShapeWithCorrelationButWithoutInternalDetails() {
    MDC.put("correlationId", "case-123");
    try {
      var problem = ApiProblems.create(403, "FORBIDDEN");
      assertThat(problem.getType()).isEqualTo(URI.create("about:blank"));
      assertThat(problem.getStatus()).isEqualTo(403);
      assertThat(problem.getTitle()).isEqualTo("Forbidden");
      assertThat(problem.getDetail()).isNull();
      assertThat(problem.getProperties())
          .containsEntry("code", "FORBIDDEN")
          .containsEntry("correlationId", "case-123");
    } finally {
      MDC.remove("correlationId");
    }
  }

  @Test
  void omitsMissingCorrelationAndLeavesSemanticTitleToCaller() {
    var problem = ApiProblems.create(422, "VALIDATION_FAILED");
    assertThat(problem.getProperties()).containsOnlyKeys("code");
    problem.setTitle("Заявка не прошла проверку");
    assertThat(problem.getTitle()).isEqualTo("Заявка не прошла проверку");
  }
}
