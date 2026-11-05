package org.studentdiscounts.submission.submission;

import java.util.List;

public final class SubmissionException extends RuntimeException {
  public final int status;
  public final String code;
  public final List<PayloadValidation.Violation> errors;

  public SubmissionException(int status, String code) {
    this(status, code, List.of());
  }

  public SubmissionException(int status, String code, List<PayloadValidation.Violation> errors) {
    super(code);
    this.status = status;
    this.code = code;
    this.errors = List.copyOf(errors);
  }

  public static SubmissionException invalid(String field, String code) {
    return new SubmissionException(
        422, code, List.of(new PayloadValidation.Violation(field, code, "Недопустимое значение")));
  }
}
