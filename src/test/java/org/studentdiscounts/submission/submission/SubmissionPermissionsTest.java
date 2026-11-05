package org.studentdiscounts.submission.submission;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.EnumSet;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.studentdiscounts.submission.domain.ActorRole;
import org.studentdiscounts.submission.domain.SubmissionActor;
import org.studentdiscounts.submission.domain.SubmissionStatus;
import org.studentdiscounts.submission.domain.SubmissionType;
import org.studentdiscounts.submission.integration.BusinessClients;

class SubmissionPermissionsTest {
  private final BusinessClients clients = mock(BusinessClients.class);
  private final SubmissionPermissions permissions = new SubmissionPermissions(clients);

  @Test
  void actorRolesCannotChangeAfterConstruction() {
    var roles = EnumSet.of(ActorRole.STUDENT);
    var actor = new SubmissionActor(UUID.randomUUID(), roles);
    roles.add(ActorRole.MODERATOR);

    assertThat(actor.roles()).containsExactly(ActorRole.STUDENT);
    assertThatThrownBy(() -> actor.roles().add(ActorRole.MODERATOR))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void ownershipAndUnreadableResourcesRemainHiddenWith404() {
    var owner = new SubmissionActor(UUID.randomUUID(), Set.of(ActorRole.STUDENT));
    var stranger = new SubmissionActor(UUID.randomUUID(), Set.of(ActorRole.STUDENT));
    var row = row(owner.id(), SubmissionStatus.SUBMITTED);

    assertThatCode(() -> permissions.requireOwner(owner, row)).doesNotThrowAnyException();
    assertThatThrownBy(() -> permissions.requireOwner(stranger, row))
        .isInstanceOfSatisfying(
            SubmissionException.class,
            error -> {
              assertThat(error.status).isEqualTo(404);
              assertThat(error.code).isEqualTo("NOT_FOUND");
            });
    assertThatThrownBy(() -> permissions.requireReadable(stranger, row))
        .isInstanceOfSatisfying(
            SubmissionException.class,
            error -> {
              assertThat(error.status).isEqualTo(404);
              assertThat(error.code).isEqualTo("NOT_FOUND");
            });
    verifyNoInteractions(clients);
  }

  @Test
  void bothReviewerRolesCanReadSubmittedButNotForeignDrafts() {
    UUID owner = UUID.randomUUID();
    for (ActorRole role : Set.of(ActorRole.MODERATOR, ActorRole.SENIOR_MODERATOR)) {
      var reviewer = new SubmissionActor(UUID.randomUUID(), Set.of(role));
      assertThatCode(() -> permissions.requireReviewer(reviewer)).doesNotThrowAnyException();
      assertThatCode(
              () -> permissions.requireReadable(reviewer, row(owner, SubmissionStatus.SUBMITTED)))
          .doesNotThrowAnyException();
      assertThatThrownBy(
              () -> permissions.requireReadable(reviewer, row(owner, SubmissionStatus.DRAFT)))
          .isInstanceOfSatisfying(
              SubmissionException.class,
              error -> {
                assertThat(error.status).isEqualTo(404);
                assertThat(error.code).isEqualTo("NOT_FOUND");
              });
    }
    verifyNoInteractions(clients);
  }

  @Test
  void missingReviewerRoleAndSelfReviewRemain403() {
    var student = new SubmissionActor(UUID.randomUUID(), Set.of(ActorRole.STUDENT));
    assertThatThrownBy(() -> permissions.requireReviewer(student))
        .isInstanceOfSatisfying(
            SubmissionException.class,
            error -> {
              assertThat(error.status).isEqualTo(403);
              assertThat(error.code).isEqualTo("FORBIDDEN");
            });
    var reviewer =
        new SubmissionActor(student.id(), Set.of(ActorRole.STUDENT, ActorRole.MODERATOR));
    assertThatThrownBy(() ->
            permissions.requireOtherReviewer(
                reviewer, row(student.id(), SubmissionStatus.IN_REVIEW)))
        .isInstanceOfSatisfying(
            SubmissionException.class,
            error -> {
              assertThat(error.status).isEqualTo(403);
              assertThat(error.code).isEqualTo("SELF_REVIEW_FORBIDDEN");
            });
    verifyNoInteractions(clients);
  }

  @Test
  void studentRoleNeverBypassesRepresentativeMembershipOrVerification() {
    UUID place = UUID.randomUUID();
    var actor =
        new SubmissionActor(
            UUID.randomUUID(), Set.of(ActorRole.STUDENT, ActorRole.ORG_REPRESENTATIVE));
    for (var authorization :
        Set.of(
            new BusinessClients.Authorization(false, true),
            new BusinessClients.Authorization(true, false))) {
      when(clients.authorize(actor.id(), place)).thenReturn(authorization);
      for (SubmissionType type : Set.of(SubmissionType.NEW_OFFER, SubmissionType.CHANGE)) {
        assertThatThrownBy(() -> permissions.requireAuthorWrite(actor, type, place))
            .isInstanceOfSatisfying(
                SubmissionException.class,
                error -> {
                  assertThat(error.status).isEqualTo(403);
                  assertThat(error.code).isEqualTo("ORGANIZATION_FORBIDDEN");
                });
      }
    }
    verify(clients, org.mockito.Mockito.times(4)).authorize(actor.id(), place);
  }

  @Test
  void newPlaceStillRequiresStudentAndDoesNotAuthorizeAnOrganization() {
    var representative =
        new SubmissionActor(UUID.randomUUID(), Set.of(ActorRole.ORG_REPRESENTATIVE));
    assertThatThrownBy(
            () -> permissions.requireAuthorWrite(representative, SubmissionType.NEW_PLACE, null))
        .isInstanceOfSatisfying(
            SubmissionException.class,
            error -> {
              assertThat(error.status).isEqualTo(403);
              assertThat(error.code).isEqualTo("FORBIDDEN");
            });
    var mixed =
        new SubmissionActor(
            representative.id(), Set.of(ActorRole.STUDENT, ActorRole.ORG_REPRESENTATIVE));
    assertThatCode(() -> permissions.requireAuthorWrite(mixed, SubmissionType.NEW_PLACE, null))
        .doesNotThrowAnyException();
    verifyNoInteractions(clients);
  }

  private SubmissionStore.SubmissionRow row(UUID owner, SubmissionStatus status) {
    Instant now = Instant.parse("2026-11-05T10:00:00Z");
    return new SubmissionStore.SubmissionRow(
        UUID.randomUUID(),
        SubmissionType.NEW_PLACE,
        status,
        owner,
        null,
        null,
        null,
        null,
        0,
        null,
        null,
        now,
        now);
  }
}
