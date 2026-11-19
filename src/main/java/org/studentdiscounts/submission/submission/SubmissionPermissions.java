package org.studentdiscounts.submission.submission;

import java.util.UUID;
import org.springframework.stereotype.Component;
import org.studentdiscounts.submission.domain.ActorRole;
import org.studentdiscounts.submission.domain.SubmissionActor;
import org.studentdiscounts.submission.domain.SubmissionStatus;
import org.studentdiscounts.submission.domain.SubmissionType;
import org.studentdiscounts.submission.integration.BusinessClients;
import org.studentdiscounts.submission.submission.SubmissionStore.SubmissionRow;

@Component
public class SubmissionPermissions {
  private final BusinessClients clients;

  public SubmissionPermissions(BusinessClients clients) {
    this.clients = clients;
  }

  public boolean reviewer(SubmissionActor actor) {
    return actor.roles().contains(ActorRole.MODERATOR)
        || actor.roles().contains(ActorRole.SENIOR_MODERATOR);
  }

  public void requireReviewer(SubmissionActor actor) {
    if (!reviewer(actor)) throw new SubmissionException(403, "FORBIDDEN");
  }

  public void requireOwner(SubmissionActor actor, SubmissionRow row) {
    if (!row.authorId().equals(actor.id())) throw new SubmissionException(404, "NOT_FOUND");
  }

  public void requireReadable(SubmissionActor actor, SubmissionRow row) {
    if (!row.authorId().equals(actor.id())
        && (!reviewer(actor) || row.status() == SubmissionStatus.DRAFT)) {
      throw new SubmissionException(404, "NOT_FOUND");
    }
  }

  public void requireOtherReviewer(SubmissionActor actor, SubmissionRow row) {
    requireReviewer(actor);
    if (row.authorId().equals(actor.id()))
      throw new SubmissionException(403, "SELF_REVIEW_FORBIDDEN");
  }

  /** Organization membership takes precedence even when the token also has STUDENT. */
  public void requireAuthorWrite(SubmissionActor actor, SubmissionType type, UUID targetPlace) {
    if (type == SubmissionType.NEW_PLACE) {
      if (!actor.roles().contains(ActorRole.STUDENT))
        throw new SubmissionException(403, "FORBIDDEN");
      return;
    }
    if (!actor.roles().contains(ActorRole.STUDENT)
        && !actor.roles().contains(ActorRole.ORG_REPRESENTATIVE)) {
      throw new SubmissionException(403, "FORBIDDEN");
    }
    if (actor.roles().contains(ActorRole.ORG_REPRESENTATIVE)) {
      var authorization = clients.authorize(actor.id(), targetPlace);
      if (!authorization.allowed() || !authorization.verified()) {
        throw new SubmissionException(403, "ORGANIZATION_FORBIDDEN");
      }
    }
  }

  public static void requireExpectedVersion(SubmissionRow row, int expected) {
    if (row.rowVersion() != expected) throw new SubmissionException(412, "STALE_ROW_VERSION");
  }

  public static void requireEditable(SubmissionRow row) {
    if (!row.status().editable()) throw new SubmissionException(409, "INVALID_TRANSITION");
  }
}
