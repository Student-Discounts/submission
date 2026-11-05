package org.studentdiscounts.submission.integration;

/** A permanent workflow contract or transition violation, sent directly to the DLT. */
public final class InvalidWorkflowEvent extends RuntimeException {
  public InvalidWorkflowEvent(String code) {
    super(code);
  }
}
