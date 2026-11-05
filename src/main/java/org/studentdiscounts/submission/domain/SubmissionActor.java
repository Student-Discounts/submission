package org.studentdiscounts.submission.domain;

import java.util.Objects;
import java.util.Set;
import java.util.UUID;

public record SubmissionActor(UUID id, Set<ActorRole> roles) {
  public SubmissionActor {
    Objects.requireNonNull(id, "id");
    roles = Set.copyOf(roles);
  }
}
