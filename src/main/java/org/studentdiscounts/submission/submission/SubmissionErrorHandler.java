package org.studentdiscounts.submission.submission;

import org.springframework.dao.DataAccessException;
import org.springframework.http.MediaType;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;
import org.studentdiscounts.submission.config.ApiProblems;
import org.studentdiscounts.submission.integration.BusinessClients;

@RestControllerAdvice
public class SubmissionErrorHandler {
  @ExceptionHandler(SubmissionException.class)
  public ResponseEntity<ProblemDetail> domain(SubmissionException failure) {
    ProblemDetail problem = ApiProblems.create(failure.status, failure.code);
    if (failure.status == 422) {
      problem.setTitle("Заявка не прошла проверку");
      problem.setProperty("errors", failure.errors);
    }
    return response(failure.status, problem);
  }

  @ExceptionHandler({
    HttpMessageNotReadableException.class,
    MethodArgumentTypeMismatchException.class,
    MissingServletRequestParameterException.class
  })
  public ResponseEntity<ProblemDetail> malformed(Exception ignored) {
    return response(400, ApiProblems.create(400, "MALFORMED_REQUEST"));
  }

  @ExceptionHandler({BusinessClients.DependencyUnavailable.class, DataAccessException.class})
  public ResponseEntity<ProblemDetail> dependency(Exception ignored) {
    return response(503, ApiProblems.create(503, "DEPENDENCY_UNAVAILABLE"));
  }

  private ResponseEntity<ProblemDetail> response(int status, ProblemDetail problem) {
    return ResponseEntity.status(status)
        .contentType(MediaType.APPLICATION_PROBLEM_JSON)
        .body(problem);
  }
}
