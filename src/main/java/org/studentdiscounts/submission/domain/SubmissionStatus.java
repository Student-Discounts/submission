package org.studentdiscounts.submission.domain;

/** Submission lifecycle, distinct from public Catalog offer visibility. */
public enum SubmissionStatus {
  DRAFT,
  SUBMITTED,
  VALIDATING,
  PENDING_REVIEW,
  IN_REVIEW,
  CHANGES_REQUESTED,
  APPROVED,
  REJECTED,
  CANCELLED,
  PUBLISHED,
  SUSPENDED,
  ARCHIVED;

  public boolean editable() {
    return this == DRAFT || this == CHANGES_REQUESTED;
  }

  /** Whether Catalog has confirmed the publication identity, including later visibility changes. */
  public boolean published() {
    return this == PUBLISHED || this == SUSPENDED || this == ARCHIVED;
  }
}
