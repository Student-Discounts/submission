package org.studentdiscounts.submission.submission;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;
import org.springframework.security.web.method.annotation.AuthenticationPrincipalArgumentResolver;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.transaction.support.TransactionTemplate;
import org.studentdiscounts.submission.domain.SubmissionStatus;
import org.studentdiscounts.submission.domain.SubmissionType;
import org.studentdiscounts.submission.integration.BusinessClients;
import tools.jackson.databind.json.JsonMapper;

class SubmissionApiTest {
  SubmissionStore store;
  BusinessClients clients;
  TransactionTemplate transactions;
  MockMvc mvc;
  Jwt author;

  @BeforeEach
  void setup() {
    store = mock(SubmissionStore.class);
    clients = mock(BusinessClients.class);
    transactions = mock(TransactionTemplate.class);
    var json = JsonMapper.builder().build();
    var clock = Clock.fixed(Instant.parse("2026-11-05T10:00:00Z"), ZoneOffset.UTC);
    var permissions = new SubmissionPermissions(clients);
    var preparation = new SubmissionPreparation(new PayloadValidation(clock), clients, json);
    var idempotency = new IdempotentCommands(store, transactions, json);
    var views = new SubmissionViews(store, json);
    var commands =
        new AuthorSubmissionCommands(
            store, permissions, preparation, idempotency, views, transactions, json, clients);
    var queries = new SubmissionQueries(store, permissions, views, json, clients);
    var corrections =
        new ReviewCorrections(store, permissions, preparation, idempotency, views, clients);
    mvc =
        MockMvcBuilders.standaloneSetup(new SubmissionController(commands, queries, corrections))
            .setControllerAdvice(new SubmissionErrorHandler())
            .setCustomArgumentResolvers(new AuthenticationPrincipalArgumentResolver())
            .build();
    author =
        Jwt.withTokenValue("fixture")
            .header("alg", "RS256")
            .subject(UUID.randomUUID().toString())
            .claim("roles", List.of("STUDENT"))
            .build();
    SecurityContextHolder.getContext().setAuthentication(new JwtAuthenticationToken(author));
  }

  @AfterEach
  void clearAuthentication() {
    SecurityContextHolder.clearContext();
  }

  @Test
  void missingAndMalformedPreconditionsAreProblems() throws Exception {
    String path = "/api/v1/submissions/" + UUID.randomUUID();
    mvc.perform(patch(path).contentType("application/merge-patch+json").content("{}"))
        .andExpect(status().is(428))
        .andExpect(content().contentTypeCompatibleWith("application/problem+json"))
        .andExpect(jsonPath("$.code").value("IF_MATCH_REQUIRED"));
    mvc.perform(
            patch(path)
                .header("If-Match", "broken")
                .contentType("application/merge-patch+json")
                .content("{}"))
        .andExpect(status().isBadRequest());
    verifyNoInteractions(store, clients, transactions);
  }

  @Test
  void malformedJsonIs400AndAggregatedSemanticErrorsAre422() throws Exception {
    mvc.perform(
            post("/api/v1/submissions")
                .header("Idempotency-Key", "key")
                .contentType("application/json")
                .content("{"))
        .andExpect(status().isBadRequest());
    verifyNoInteractions(store, clients, transactions);

    mvc.perform(
            post("/api/v1/submissions")
                .header("Idempotency-Key", "key")
                .contentType("application/json")
                .content(
                    """
                    {"type":"NEW_PLACE","payload":{"place":{"name":123},"offers":{}}}
                    """))
        .andExpect(status().is(422))
        .andExpect(content().contentTypeCompatibleWith("application/problem+json"))
        .andExpect(jsonPath("$.code").value("VALIDATION_FAILED"))
        .andExpect(jsonPath("$.title").value("Заявка не прошла проверку"))
        .andExpect(jsonPath("$.errors.length()").value(2))
        .andExpect(jsonPath("$.errors[0].field").value("place.name"))
        .andExpect(jsonPath("$.errors[0].code").value("INVALID_VALUE"))
        .andExpect(jsonPath("$.errors[1].field").value("offers"))
        .andExpect(jsonPath("$.errors[1].code").value("INVALID_VALUE"));
    verify(store)
        .completedRequest(
            eq(UUID.fromString(author.getSubject())),
            eq("POST /api/v1/submissions"),
            eq("key"),
            anyString());
    verifyNoMoreInteractions(store);
    verifyNoInteractions(clients, transactions);
  }

  @Test
  void unknownAndNonStringTypesRemainSemanticTargetErrors() throws Exception {
    for (String type : new String[] {"\"UNKNOWN\"", "\"new_place\"", "null", "42", "{}", "[]"}) {
      mvc.perform(
              post("/api/v1/submissions")
                  .header("Idempotency-Key", "invalid-type")
                  .contentType("application/json")
                  .content("{\"type\":" + type + ",\"payload\":{}}"))
          .andExpect(status().is(422))
          .andExpect(jsonPath("$.code").value("TARGET_INVALID"))
          .andExpect(jsonPath("$.errors[0].field").value("type"))
          .andExpect(jsonPath("$.errors[0].code").value("TARGET_INVALID"));
    }
    mvc.perform(
            post("/api/v1/submissions")
                .header("Idempotency-Key", "missing-type")
                .contentType("application/json")
                .content("{\"payload\":{}}"))
        .andExpect(status().is(422))
        .andExpect(jsonPath("$.code").value("TARGET_INVALID"))
        .andExpect(jsonPath("$.errors[0].field").value("type"));
    verifyNoInteractions(clients, transactions);
  }

  @Test
  void submitAndCancelRejectBodiesRatherThanIgnoringThem() throws Exception {
    String path = "/api/v1/submissions/" + UUID.randomUUID();
    mvc.perform(
            post(path + "/submit")
                .header("If-Match", "\"0\"")
                .header("Idempotency-Key", "key")
                .contentType("application/json")
                .content("{\"status\":\"PUBLISHED\"}"))
        .andExpect(status().isBadRequest());
    mvc.perform(
            post(path + "/cancel")
                .header("If-Match", "\"0\"")
                .contentType("application/json")
                .content("{}"))
        .andExpect(status().isBadRequest());
    verifyNoInteractions(store, clients, transactions);
  }

  @Test
  void staleSubmitFailsBeforeExternalReadsOrCommandTransaction() throws Exception {
    UUID id = UUID.randomUUID();
    Instant now = Instant.parse("2026-11-05T10:00:00Z");
    when(store.find(id))
        .thenReturn(
            new SubmissionStore.SubmissionRow(
                id,
                SubmissionType.NEW_PLACE,
                SubmissionStatus.DRAFT,
                UUID.fromString(author.getSubject()),
                null,
                null,
                JsonMapper.builder().build().createObjectNode(),
                null,
                2,
                null,
                null,
                now,
                now));
    mvc.perform(
            post("/api/v1/submissions/" + id + "/submit")
                .header("If-Match", "\"3\"")
                .header("Idempotency-Key", "future"))
        .andExpect(status().is(412))
        .andExpect(jsonPath("$.code").value("STALE_ROW_VERSION"));
    var reads = inOrder(store);
    reads
        .verify(store)
        .completedRequest(
            eq(UUID.fromString(author.getSubject())),
            eq("POST /api/v1/submissions/" + id + "/submit"),
            eq("future"),
            anyString());
    reads.verify(store).find(id);
    reads
        .verify(store)
        .completedRequest(
            eq(UUID.fromString(author.getSubject())),
            eq("POST /api/v1/submissions/" + id + "/submit"),
            eq("future"),
            anyString());
    reads.verifyNoMoreInteractions();
    verifyNoInteractions(clients, transactions);
  }

  @Test
  void staleRowReadReplaysAnIdenticalCommandThatJustCommitted() throws Exception {
    UUID id = UUID.randomUUID();
    UUID authorId = UUID.fromString(author.getSubject());
    String operation = "POST /api/v1/submissions/" + id + "/submit";
    var json = JsonMapper.builder().build();
    var cachedBody = json.createObjectNode().put("id", id.toString()).put("status", "SUBMITTED");
    var cached = new SubmissionResponse(202, cachedBody, "\"1\"");
    when(store.completedRequest(eq(authorId), eq(operation), eq("send"), anyString()))
        .thenReturn(null, cached);
    Instant now = Instant.parse("2026-11-05T10:00:00Z");
    when(store.find(id))
        .thenReturn(
            new SubmissionStore.SubmissionRow(
                id,
                SubmissionType.NEW_PLACE,
                SubmissionStatus.SUBMITTED,
                authorId,
                null,
                null,
                null,
                1,
                1,
                null,
                null,
                now,
                now));

    mvc.perform(
            post("/api/v1/submissions/" + id + "/submit")
                .header("If-Match", "\"0\"")
                .header("Idempotency-Key", "send"))
        .andExpect(status().isAccepted())
        .andExpect(header().string("ETag", "\"1\""))
        .andExpect(content().json(cachedBody.toString()));

    var reads = inOrder(store);
    reads.verify(store).completedRequest(eq(authorId), eq(operation), eq("send"), anyString());
    reads.verify(store).find(id);
    reads.verify(store).completedRequest(eq(authorId), eq(operation), eq("send"), anyString());
    reads.verifyNoMoreInteractions();
    verifyNoInteractions(clients, transactions);
  }

  @Test
  void etagRequiresQuotedNonnegativeVersion() {
    assertThat(SubmissionController.expectedVersion("\"12\"")).isEqualTo(12);
    assertThatThrownBy(() -> SubmissionController.expectedVersion(null))
        .isInstanceOf(SubmissionException.class)
        .extracting("status")
        .isEqualTo(428);
    for (String invalid : new String[] {"12", "W/\"12\"", "\"-1\"", "\"x\"", "\"2147483648\""}) {
      assertThatThrownBy(() -> SubmissionController.expectedVersion(invalid))
          .isInstanceOf(SubmissionException.class)
          .extracting("status")
          .isEqualTo(400);
    }
  }

  @Test
  void idempotencyKeyRejectsControlCharactersAndOversize() {
    assertThat(IdempotentCommands.requestKey("safe-key 12")).isEqualTo("safe-key 12");
    for (String invalid : new String[] {"", "a\nb", "é", "x".repeat(129)}) {
      assertThatThrownBy(() -> IdempotentCommands.requestKey(invalid))
          .isInstanceOf(SubmissionException.class);
    }
  }
}
