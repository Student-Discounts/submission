package org.studentdiscounts.submission.integration;

import static org.studentdiscounts.submission.integration.WorkflowEvent.require;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;
import org.studentdiscounts.submission.domain.SubmissionType;
import tools.jackson.databind.JsonNode;

/** Owns workflow transactions and prepares external observations before acquiring parent locks. */
@Service
public class WorkflowService {
  private final WorkflowRepository repository;
  private final TransactionTemplate transactions;
  private final BusinessClients clients;

  public WorkflowService(
      WorkflowRepository repository, TransactionTemplate transactions, BusinessClients clients) {
    this.repository = repository;
    this.transactions = transactions;
    this.clients = clients;
  }

  public void process(JsonNode message) {
    WorkflowEvent event = WorkflowEvent.parse(message);
    if (repository.inboxContains(event.id())) {
      return;
    }
    if (event.type() == WorkflowEvent.Type.MediaAssetChecked) {
      transactions.executeWithoutResult(ignored -> {
        if (repository.reserveInbox(event.id())) {
          recordMediaEvent(event.mediaCheck());
        }
      });
      return;
    }
    UUID id = event.submissionId();
    WorkflowSubmission prepared = repository.read(id);
    ApprovalObservations observations = prepareApproval(event, prepared);
    transactions.executeWithoutResult(ignored -> {
      if (!repository.reserveInbox(event.id())) {
        return;
      }
      WorkflowSubmission row = repository.lock(id);
      WorkflowRules.Transition transition = WorkflowRules.evaluate(row, event);
      if (transition.alreadyApplied()) {
        return;
      }
      if (event.type() == WorkflowEvent.Type.SubmissionApproved) {
        requireApproval(row, observations);
      }
      if (event.type() == WorkflowEvent.Type.OfferPublished) {
        repository.recordPublishedOffers(id, transition.publishedOfferIds());
      }
      if (event.type() == WorkflowEvent.Type.SubmissionChangesRequested) {
        repository.seedEditableDraft(row);
      }
      repository.transition(
          row,
          transition.target(),
          transition.actorId(),
          transition.reason(),
          event.correlationId());
    });
  }

  private ApprovalObservations prepareApproval(WorkflowEvent event, WorkflowSubmission prepared) {
    BusinessClients.Offer catalog = null;
    Map<UUID, BusinessClients.MediaAsset> media = new HashMap<>();
    if (event.type() == WorkflowEvent.Type.SubmissionApproved) {
      if (prepared.type() == SubmissionType.CHANGE) {
        catalog = clients.offer(prepared.targetOfferId());
      }
      // Fresh reads close the gap after validation: old CLEAN observations cannot approve an unsafe
      // asset.
      for (UUID asset : WorkflowRules.assets(prepared.payload())) {
        media.put(asset, clients.media(asset));
      }
    }
    return new ApprovalObservations(prepared.versionId(), catalog, media);
  }

  private void requireApproval(WorkflowSubmission row, ApprovalObservations observations) {
    require(row.versionId().equals(observations.versionId()), "STALE_REVIEWED_VERSION");
    require(
        !repository.hasPublicationConflict(row.id(), row.versionId()),
        "NEW_REVIEWED_VERSION_REQUIRED");
    if (row.type() == SubmissionType.CHANGE) {
      require(
          observations.catalog() != null
              && Objects.equals(row.baseOfferRowVersion(), observations.catalog().rowVersion()),
          "CATALOG_VERSION_CONFLICT");
    }
    for (BusinessClients.MediaAsset asset : observations.media().values()) {
      repository.recordMedia(
          row.versionId(),
          new WorkflowEvent.MediaCheck(asset.id(), asset.status(), asset.revision()));
    }
    for (UUID asset : WorkflowRules.assets(row.payload())) {
      require(repository.isMediaClean(row.versionId(), asset), "MEDIA_NOT_CLEAN");
    }
  }

  private void recordMediaEvent(WorkflowEvent.MediaCheck check) {
    for (UUID version : repository.lockValidatingVersionsForAsset(check.assetId())) {
      repository.recordMedia(version, check);
    }
  }

  private record ApprovalObservations(
      UUID versionId, BusinessClients.Offer catalog, Map<UUID, BusinessClients.MediaAsset> media) {}
}
