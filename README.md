# Submission Service

Independent Java 25 / Spring Boot 4.1.2 service for author drafts, immutable reviewed snapshots, validation, review workflow, and confirmed publication state. PostgreSQL is authoritative; HTTP commands atomically write versions, state/history, idempotency responses and an outbox. Kafka delivery is **at least once**, not exactly once. `APPROVED` is not `PUBLISHED` and this service never writes public Catalog tables.

## Architecture

The HTTP controller converts the validated JWT once into `domain.SubmissionActor` (UUID plus immutable `ActorRole` set), then delegates to concrete use cases: `AuthorSubmissionCommands` handles draft/submit/cancel commands, `SubmissionQueries` handles authorized reads/comparisons, and `ReviewCorrections` handles moderator-created immutable versions. Application permissions no longer depend on Spring JWT objects or string role checks. `SubmissionType` and `SubmissionStatus` encode the domain alternatives; persistence/events explicitly retain their existing names. `SubmissionStore` owns typed persistence operations and SQL; callers cannot access repository internals.

The Spring-free `domain` package owns those types, `PayloadJson`, and the shared UUIDv7 `SubmissionIds` generator. Submission application code depends on the domain and external integration adapters; integration workflow code depends on the domain, not on application use cases. `WorkflowEvent` parses messages, pure `WorkflowRules` checks lifecycle/tuple invariants against `WorkflowSubmission`, and `WorkflowRepository` owns SQL/history/outbox writes. `WorkflowService` and `ValidationWorker` orchestrate external observations outside command locks and atomic transitions; `OutboxRelay` retains the ordered row lock through bounded Kafka acknowledgement. Full snapshots deliberately remain `JsonNode`: semantic errors must aggregate as 422, merge-patch must preserve missing/null distinctions, and old schema-versioned snapshots must remain readable. No generic CRUD framework or sibling-auth source/build dependency is introduced.

Validation selection uses bounded keyset round-robin scanning rather than repeatedly taking the same oldest pending batch. A private UUID cursor advances across still-processing or dependency-delayed submissions and wraps through disjoint ID ranges without duplicate selection. It is scheduling state only: persisted submission/version/media observations remain authoritative, and restart merely resets scan order. No synthetic heartbeat, schema change, extra version or ETag mutation is needed to prevent a full batch of pending assets from starving later ready submissions.

## Provenance and tools

The base was generated with Spring Initializr (`https://start.spring.io/starter.zip`): `type=gradle-project`, `language=java`, `bootVersion=4.1.2`, `javaVersion=25`, `groupId=org.studentdiscounts`, `artifactId=submission-service`, `name=submission-service`, `packageName=org.studentdiscounts.submission`, `version=0.1.0-SNAPSHOT`, dependencies `web,validation,security,oauth2-resource-server,jdbc,flyway,postgresql,kafka,actuator,testcontainers,configuration-processor`. The generated modular Boot 4 starters and application entry point are retained. No Maven or Gradle wrapper is used. Build with installed **Gradle 9.7.1**; remaining managed library versions come from Boot's BOM, except `java-uuid-generator:5.2.0` for Submission-generated UUIDv7. User/session UUIDs from auth may be any UUID version.

On the implementation macOS host `java` defaults to a different version; explicitly use:

```bash
export JAVA_HOME=/usr/local/opt/openjdk@25/libexec/openjdk.jdk/Contents/Home
gradle tasks --all
```

On other hosts install Java 25 and Gradle 9.7.1 and set `JAVA_HOME` accordingly. Docker must be running for integration tests and local dependencies. No H2, container-test skipping, bare context-load test, sibling repository build, or external Git remote is required.

## Startup

The separately operated **real auth service must already run on port 8080** for a normal authenticated local session. This repository does not start/build/modify auth, read its database, issue runtime tokens or install auth/JWKS mappings in Compose.

```bash
# From this repository; starts real PostgreSQL/Kafka and explicit business HTTP mocks only.
docker compose config --quiet
docker compose up -d --wait postgres kafka stubs
# Runs Submission on 8081; local profile alone supplies local-only credentials and stub URLs.
gradle bootRun --args='--spring.profiles.active=local'
```

Available local endpoints: Submission `http://localhost:8081`, OpenAPI `http://localhost:8081/openapi.yaml`, health `/actuator/health`, `/actuator/health/liveness`, `/actuator/health/readiness`; PostgreSQL `127.0.0.1:5433/submission`; Kafka `localhost:9092`; business WireMock `http://localhost:8090` (admin `/__admin` is a local fixture interface, not a production API). Host bindings are loopback only. Auth remains external `http://localhost:8080`.

For the runtime-only, non-root Java 25 image:

```bash
gradle bootJar
# If .env is copied from .env.example its host JWKS URL must be overridden for containers:
SD_SUBMISSION_AUTH_JWK_SET_URI=http://host.docker.internal:8080/.well-known/jwks.json \
  docker compose --profile app up -d --build
curl --fail http://localhost:8081/actuator/health/readiness
```

`Dockerfile` copies the host-built `build/libs/submission-service.jar`; it does not build sources. Compose overrides database/broker/business URLs for container networking. `host.docker.internal:host-gateway` supports Linux. Issuer stays the token's public issuer (`http://localhost:8080`) even when JWKS are fetched internally. Operators may override issuer and JWKS independently. The app profile waits for healthy DB/Kafka/stubs. A healthy container alone does not prove real-auth API access.

## Configuration

`application.yml` is the nonlocal configuration: required DB credentials, broker, issuer/JWKS and Catalog/Media/Geo URLs have **no production password/mock defaults**. Missing or invalid required settings fail startup. Organization provider may be absent and then representative target-bound writes fail closed. `application-local.yml` explicitly supplies development-only defaults. `.env.example` documents local values; Compose reads `.env`, but host Gradle does not automatically load it. To load it into the host shell: `set -a; source .env; set +a`. Never commit populated secrets.

Submission-owned values bind once into immutable, validated `SubmissionProperties` groups (`security`, `limits`, `cors`, `dependencies`, `workers`, `kafka`), registered by `SubmissionConfiguration` under normal Boot component scanning. HTTP business bases are typed `URI`; invalid protocols/credentials/query fragments fail before serving traffic, without DNS resolution or contacting providers. Required Catalog/Media/Geo bases cannot be omitted; an empty organization base remains absent. Body/rate/batch limits must be positive and scheduler/send durations at least one millisecond. Existing `*-ms` property names and bare numeric millisecond values are preserved with `@DurationUnit(MILLIS)`; explicit duration values such as `500ms` also work. Empty CORS still disables cross-origin access; comma-separated explicit origins retain their existing meaning. No environment key or worker pause flag is renamed.

Standard datasource/Kafka client/OAuth settings remain Boot-owned. `JwtSecurityConfiguration` consumes Boot's existing `OAuth2ResourceServerProperties`, retaining separate explicit issuer and JWK URI settings and fixed 2-second connect/3-second read timeouts. `AuthTokenContractValidator` uses injected `Clock` and the authoritative `ActorRole` enum; Spring's standard timestamp/issuer validators and RS256 decoder remain in place. `SecurityConfig` owns only stateless HTTP authorization/filter assembly; `SubmissionCorsConfiguration` owns the CORS policy. Startup validation neither discovers OIDC metadata nor fetches remote keys. Only actual token verification fetches JWKS; no token issuing or fallback authentication is added.

| Environment variable | Local default / purpose |
|---|---|
| `SERVER_PORT` | `8081` |
| `SD_SUBMISSION_DB_URL` | `jdbc:postgresql://localhost:5433/submission` |
| `SD_SUBMISSION_DB_USER`, `SD_SUBMISSION_DB_PASSWORD` | `submission` / `submission`, local only |
| `SD_SUBMISSION_KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` (container `kafka:29092`) |
| `SD_SUBMISSION_KAFKA_GROUP_ID` | `submission-workflow-v1`; isolate every independent consumer instance/test |
| `SD_SUBMISSION_AUTH_ISSUER` | `http://localhost:8080` |
| `SD_SUBMISSION_AUTH_JWK_SET_URI` | `http://localhost:8080/.well-known/jwks.json`; container default uses host.docker.internal |
| `SD_SUBMISSION_CATALOG_BASE_URL` | `http://localhost:8090`, explicit local mock |
| `SD_SUBMISSION_MEDIA_BASE_URL` | `http://localhost:8090`, explicit local mock |
| `SD_SUBMISSION_GEO_BASE_URL` | `http://localhost:8090`, explicit local mock |
| `SD_SUBMISSION_ORGANIZATIONS_BASE_URL` | `http://localhost:8090`, explicit local mock; absent provider denies safely |
| `SD_SUBMISSION_CORS_ALLOWED_ORIGINS` | local `http://localhost:5173`; otherwise empty/disabled |
| `SD_SUBMISSION_MAX_BODY_BYTES` | `262144`, including chunked bodies; overflow 413 |
| `SD_SUBMISSION_REQUESTS_PER_MINUTE` | `120` per authenticated actor in PostgreSQL UTC-minute windows; 429 + Retry-After |
| `SD_SUBMISSION_VALIDATION_ENABLED` | `true`; pause durable validation without losing queued work |
| `SD_SUBMISSION_OUTBOX_ENABLED` | `true`; pause durable relay without losing committed events |

`submission.workers` defaults: validation every 500 ms/batch20, media retry5000 ms; outbox every500 ms/batch50, bounded send10000 ms. `submission.kafka.local-topics-enabled` is false normally and true only in local profile: a local AdminClient creates `submission.workflow.v1`, `submission.events.v1`, `submission.workflow.v1.DLT`, each one partition/replica. Production must provision topics, TLS/SASL and ACLs externally; configure standard `spring.kafka.*` security properties. Every principal allowed to write the shared workflow topic is trusted for **all workflow event types**; `producer` is a checked contract string, not cryptographic identity. End-user Kafka writes must be prohibited.

Tomcat max100 threads/200 connections/accept queue100; DB pool10. HTTP dependencies use fixed configured bases, 2s connect/3s read timeout and never return success fallback. Correlation IDs are safe 1–64-character `[A-Za-z0-9._:-]`, generated when missing/invalid, returned and carried through MDC, external HTTP/history/events. Payloads, tokens, passwords and private proof URLs must not be logged. CORS permits Authorization, Content-Type, If-Match, Idempotency-Key, X-Correlation-ID and exposes ETag/Location.

## Existing auth integration and permissions

RS256 resource-server validation fetches explicit JWKS (no OIDC discovery), checks issuer, exact audience `student-discounts-api`, expiry, UUID `sub` and `sid`, and a string-array `roles`. Authorities use `ROLE_` plus actual roles `STUDENT`, `ORG_REPRESENTATIVE`, `MODERATOR`, `SENIOR_MODERATOR`. No `user_id`, account-status or fabricated SERVICE claim exists. Bearer headers only; no trusted identity headers, cookies or per-request `/api/v1/me` introspection.

Auth currently issues five-minute access tokens. Offline validation cannot immediately observe user/session revocation or changed roles before expiry. Auth's organization verification/role-management/refresh/logout/revocation interfaces are unfinished; organization membership is a distinct unimplemented business contract, **not an auth endpoint**. Obtain real tokens using auth's documented register/login endpoints. Submission never grants roles or manufactures production tokens.

Students author their own submissions; NEW_PLACE requires STUDENT. For target-bound NEW_OFFER/CHANGE create/edit/submit, presence of ORG_REPRESENTATIVE always requires `allowed && verified` organization authorization, even with STUDENT also present. Foreign private submissions return404; no cross-author list exists. Reviewers cannot review/correct their own submission. Editable drafts remain owner-only.

## HTTP commands

See [`/openapi.yaml`](http://localhost:8081/openapi.yaml) and `src/main/resources/static/openapi.yaml` for every route and schema. Examples use an already obtained real `$TOKEN`; do not print it or commit it.

```bash
# Incomplete drafts are legitimate; no version exists yet.
curl -i http://localhost:8081/api/v1/submissions \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: create-my-draft-1' \
  --data '{"type":"NEW_PLACE","payload":{}}'
# Copy returned id and exact quoted ETag; patch is the payload itself, not {payload:...}.
curl -i -X PATCH "http://localhost:8081/api/v1/submissions/$ID" \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/merge-patch+json' \
  -H 'If-Match: "0"' \
  --data '{"place":{"name":"Кофейня «Зерно»","city":"Санкт-Петербург","addressLine":"Кронверкский пр., 49","categoryCode":"cafe","location":{"lat":59.9571,"lon":30.3083}},"offers":[{"conditions":"Студенческий обед за 500 рублей","discountType":"SPECIAL_PRICE","discountValue":500,"source":{"type":"WEBSITE","url":"https://example.com/students"},"schedule":[{"dayOfWeek":5,"timeFrom":"22:00","timeUntil":"24:00"}]}]}'
# Use the PATCH response ETag, not the stale example above.
curl -i -X POST "http://localhost:8081/api/v1/submissions/$ID/submit" \
  -H "Authorization: Bearer $TOKEN" -H "If-Match: $ETAG" \
  -H 'Idempotency-Key: submit-my-draft-1'
```

POST create requires Idempotency-Key, returns201/Location/ETag/DRAFT. PATCH requires If-Match, only DRAFT/CHANGES_REQUESTED; objects recursively merge, arrays replace, null removes. Targets cannot change. NEW_OFFER prefills public place; CHANGE loads full ACTIVE public snapshot and reviewed base, never automatically rebases on edits. Submit requires If-Match + key, no body;202 begins a cycle,200 can return an existing frozen snapshot. Cancel only allows SUBMITTED→CANCELLED and preserves version/history. Other routes: own list `?limit=20&before=<uuid>`; detail; `/versions`, `/versions/{versionNo}`, `/history`, `/comparison`; moderator correction POST `/versions`; `/api/v1/location-suggestions?city=...&addressLine=...`. UUID and version cursors are exclusive/descending, limits1–100. Moderator correction returns a Version record, not a submit result. CHANGE correction requires the exact `expectedCatalogRowVersion` explicitly reviewed through comparison; a stale value returns409, never silently substitutes a newer revision. There is no approve/reject HTTP endpoint.

Missing If-Match428, malformed400, stale412 `STALE_ROW_VERSION`, illegal transition409 `INVALID_TRANSITION`. Successful idempotency keys are retained without expiry, scoped by actor + exact method/path, fingerprint normalized body plus original If-Match. A same-key replay returns original status/body/ETag before state checks or external reads; changed fingerprint409 `IDEMPOTENCY_KEY_REUSED`; failed commands leave no successful record. Equal latest payload hash/base creates no new immutable row. An unchanged CHANGES_REQUESTED resubmit still begins a new cycle/event; already-sent noneditable repeat with current ETag returns200 without a new event.

Semantic errors aggregate as RFC9457 `application/problem+json`,422, `type=about:blank`, title `Заявка не прошла проверку`, indexed `errors:[{field,code,message}]`. Malformed JSON400; valid JSON with bad field/type/enum422. Dependency failures503 with no partial command. Source URLs are checked HTTP(S) with host and **never dereferenced**. Offers1–5 (CHANGE exactly1); per-offer source type+URL/note or matching PROOF required; at most10 distinct media references. Coordinates optional/paired. Inclusive expiry uses resolved place zone (provisional UTC without coordinates); frozen coordinates always require a resolved timezone even without dates. Schedules support end24:00, adjacent intervals but no overlaps/midnight crossing, at most28 intervals/offer. Decimal values fit numeric(10,2) without rounding. Unknown fields and client service metadata rejected. Normalization trims strings, removes absent optional text, defaults discount type OTHER, normalizes decimals, sorts object keys and preserves array order; SHA256 covers normalized payload only, not generated version/time/zone metadata. Old schemas/snapshots are never rewritten.

## Workflow and event contracts

Versioned JSON Schemas are in `contracts/events/`: `payload-v1.schema.json`, `workflow-v1.schema.json`, `submission-events-v1.schema.json`. Envelope: eventId, eventType, schemaVersion1, producer, occurredAt, correlationId, data; outgoing adds aggregateSequence. Kafka key is submission UUID (media events use media UUID). Review/publication require exact `{submissionId,versionId,versionNo,expectedRowVersion}`; moderator events also actorId; reasons required for requested changes/rejection/reopening/suspension/archive/conflict.

Durable validation progresses SUBMITTED→VALIDATING→PENDING_REVIEW only when all media are CLEAN (or absent); processing/quarantined stays pending, any rejected attached file hard-rejects even with a URL source. HTTP outages never mean CLEAN. Newer MediaAssetChecked revisions wake validation; stale/unknown references no-op. ReviewStarted→IN_REVIEW; ChangesRequested seeds editable snapshot; rejection is final. Approval requires exact current snapshot, nonauthor reviewer, coordinates/frozen zone, clean files and unchanged CHANGE base, reaching **APPROVED only**. No automatic approval exists.

**Catalog's sole publication trigger is committed outgoing `SubmissionStatusChanged` with `toStatus=APPROVED`, never raw incoming `SubmissionApproved`.** Catalog correlates the preceding immutable version event, transactionally deduplicates the approval trigger and reuses allocated offer IDs on replay; only then sends OfferPublished with the approved tuple/current rowVersion and one distinct ID per offer. Submission stores IDs and moves APPROVED→PUBLISHED. Duplicate confirmations do not duplicate history; conflicting IDs/stale tuples go to DLT. CHANGE publication conflict returns APPROVED→PENDING_REVIEW (`CATALOG_VERSION_CONFLICT`), requiring explicit refreshed-base version/review rather than stale overwrite. Catalog controls actual expiry/visibility; status observations support PUBLISHED→SUSPENDED→ARCHIVED and moderator reopening SUSPENDED→PENDING_REVIEW.

Inbox/business/history/outbox commit together. Transient broker/DB errors receive3 total attempts/1s backoff; malformed/unauthorized/illegal transitions go straight to DLT. Offsets are acknowledged only after DB commit or successful DLT publication. Outbox relays lock oldest unpublished per aggregate with SKIP LOCKED, hold lock during bounded acknowledged send, then mark published. A crash between send/mark can duplicate stable eventId; consumers must deduplicate. Multiple relays must not overtake aggregateSequence.

## What is real, mocked, or absent

| Boundary | Local implementation |
|---|---|
| Submission HTTP/domain/PostgreSQL/Flyway/idempotency/inbox/outbox/Kafka | Real implementation, separate database |
| Auth | External real service; no normal Compose auth fixture. Smoke contract JWKS is test-only, not auth replacement |
| Catalog reads | Strict WireMock place/two offers/category/revision7 snapshots; no public catalog database/search/publication application |
| Catalog publication | Explicit smoke event fixture following committed approval; no real public offers created |
| Media | Mock metadata + explicit fixture events; no uploads, scanner, quarantine storage, byte/type checks or S3 implementation |
| Geo | Explicit Moscow/UTC lookup and no-match fixtures; no real geocoder |
| Organization membership | Explicit verified/denied mock contract, unavailable from current auth; production fail closed |
| Moderation | Explicit human-decision event fixtures and moderator test tokens only; no auto-approver or decision store |
| Risk / duplicates | Not implemented here; no fake score or safety claim |

Static business mappings in `dev/stubs/mappings` match known URLs/query values only; all unknown requests404. Categories `cafe` active / `inactive` inactive; place `019a0000-0000-7000-8000-000000000001`; two offers ending0011/0012 at revision7; exact Moscow point59.9571/30.3083; suggestions city `No Match`, address `Unknown` returns empty. Default known-place organization responses deny.

For actor-specific Media/verified organization fixtures, obtain two actual authenticated UUID subjects, then:

```bash
bash dev/configure-business-fixtures.sh "$AUTHOR_UUID" "$OTHER_AUTHOR_UUID"
```

This installs only six named operator mappings via local WireMock admin; never resets shared mappings, changes auth roles or uses a magic owner UUID. Media IDs ending0021 CLEAN,0022 QUARANTINED,0023 PROCESSING,0024 REJECTED belong to supplied author;0025 CLEAN belongs to supplied other author. Verified organization authorization matches only supplied author + known place. Smoke creates/deletes its own unique mappings instead, avoiding operator/shared fixtures.

## Tests and smoke

JUnit Jupiter pure-rule/MVC tests live in `src/test`; real PostgreSQL17/Kafka4/JWKS/HTTP tests in `src/integrationTest`. `check` includes `integrationTest` and reproducible `javaFormatCheck`; tests do not replace PostgreSQL with H2. `gradle javaFormat` applies pinned google-java-format 2.0.0 to **all** Java source sets, including tests and smoke, using the Java 25 toolchain. Run Gradle commands serially in one checkout to avoid shared test-output races:

```bash
gradle test --tests 'org.studentdiscounts.submission.submission.PayloadValidationTest'
gradle test --tests 'org.studentdiscounts.submission.submission.SubmissionApiTest'
gradle integrationTest --tests 'org.studentdiscounts.submission.integration.PostgresWorkflowIT'
gradle integrationTest --tests 'org.studentdiscounts.submission.integration.AuthContractIT'
gradle integrationTest --tests 'org.studentdiscounts.submission.integration.KafkaLifecycleIT'
gradle build
gradle smoke --args=contract
SD_AUTH_BASE_URL=http://localhost:8080 gradle smoke --args=auth
```

Smoke depends on bootJar and launches its own actual executable jar on18081, unique PostgreSQL schema, separate Kafka consumer group, unique actor/correlation IDs/mappings, a30s assertion deadline, and isolated request limit10000. It refuses occupied18081/18090, cleans only its own schema/mappings/processes, never resets shared WireMock or drops volumes. Contract mode uses fresh RSA test-only JWKS on18090 and exact auth-format claims; no runtime token-issuing endpoint exists. Auth mode uses real register/login/JWKS and fails if unavailable, **never falls back to test keys**. Optional `SD_SMOKE_STUDENT_EMAIL`/`SD_SMOKE_STUDENT_PASSWORD` and `SD_SMOKE_OTHER_EMAIL`/`SD_SMOKE_OTHER_PASSWORD` select dedicated existing accounts; otherwise synthetic unique accounts and24-character passwords are used. No credentials/tokens printed; no role-admin endpoint invented. Auth mode proves student flows only; contract-mode moderator fixture is not proof of real role administration.

### Observed verification

Commands below use Java 25 with `JAVA_HOME=/usr/local/opt/openjdk@25/libexec/openjdk.jdk/Contents/Home`; no wrapper/Maven is involved. Only completed commands are reported.

- Focused `gradle test --tests 'org.studentdiscounts.submission.config.*' integrationTest --tests 'org.studentdiscounts.submission.integration.AuthContractIT' --tests 'org.studentdiscounts.submission.integration.SecurityHttpContractIT' --tests 'org.studentdiscounts.submission.integration.PostgresRateLimitIT'`: **BUILD SUCCESSFUL** in 19 seconds, **39 unit + 9 integration tests**, zero failures/errors/skips. This exercises invalid startup settings, required/optional providers, blank/CSV CORS authorization, legacy millisecond units, fixed-clock claim boundaries, and real JWKS including independent public issuer/internal key URLs.
- Current production/domain/settings/fairness tree: `gradle javaFormat build compileSmokeJava --console=plain`: **BUILD SUCCESSFUL** in 1 minute 14 seconds, **84 unit + 37 PostgreSQL/Kafka/HTTP integration tests**, zero failures/errors/skips, plus compiled smoke and formatting checks. This includes the full Spring HTTP regression, semantic enum-input boundaries, typed actor permissions, reviewed-version approval race, and two validation-selection fairness regressions.
- After adding the actual-scheduler fairness smoke case: the same formatter/build/compile command **succeeded in 11 seconds**. Smoke compilation/formatting executed; unchanged production compilation and the previously passed 121 tests were correctly `UP-TO-DATE`, not claimed as rerun.
- `docker compose config --quiet && docker compose --profile app up -d --build --wait`: exit 0 in 26.89 seconds, **all four containers Healthy**. The runtime image was rebuilt from the current production jar, not left on the earlier implementation. Fresh container checks confirmed UID/GID 999 (`submission`), readiness `{"status":"UP"}`, unauthenticated API HTTP 401, and OpenAPI HTTP 200. This does not establish real-auth token access.
- Current actual-jar proof: `JAVA_HOME=/usr/local/opt/openjdk@25/libexec/openjdk.jdk/Contents/Home /usr/local/bin/gradle smoke --args=contract --console=plain`: **PASS**, exit 0 in 70.34 seconds (Gradle 1 minute 10 seconds). All eight original acceptance scenarios remain, plus actual scheduled fairness: the child jar is explicitly launched with validation batch size 2, two proof-only quarantined heads stay VALIDATING, and a third complete submission reaches PENDING_REVIEW through the real scheduled worker. No database writes or direct worker calls fake those transitions. Owned schemas were confirmed removed after teardown.
- `JAVA_HOME=/usr/local/opt/openjdk@25/libexec/openjdk.jdk/Contents/Home SD_AUTH_BASE_URL=http://localhost:8080 /usr/local/bin/gradle smoke --args=auth --console=plain`: **unavailable prerequisite**, exit 1 in 3.40 seconds with `java.net.ConnectException` fetching the real JWKS on port 8080, before schema/app creation. No test-key fallback or auth build/start occurred. Real register/login and token-authenticated container access are **not verified**.
- OpenAPI YAML syntax/internal references, three Draft 2020-12 event JSON Schemas, CI YAML structure and fixture installer shell syntax were validated. CI itself has not run locally.

### Selected observed red → green regressions

These are consumer-visible failures actually observed before fixes, not inferred TDD claims. Re-run the permanent test classes below using the installed `gradle` binary; `JAVA_HOME` setup is above. Early missing-class compilation and concurrent Gradle output-file races were infrastructure failures and are not counted as behavioral reds.

| Permanent test / command | Observed red | Green after fix |
|---|---|---|
| `gradle test --tests 'org.studentdiscounts.submission.integration.BusinessClientsTest'` | Catalog revision 0 rejected; outgoing correlation header missing (two new real-HTTP regressions failed) | Nonnegative revisions accepted; current MDC correlation propagated; all 11 adapter tests passed |
| `gradle integrationTest --tests 'org.studentdiscounts.submission.integration.AuthContractIT'` | Missing audience caused NPE instead of a JWT rejection | Explicit missing-audience validation; all 4 real-JWKS contract tests passed |
| `gradle integrationTest --tests 'org.studentdiscounts.submission.integration.PostgresWorkflowIT'` | Coordinate-bearing input could normalize away forbidden `status:null` metadata | Raw-input validation in resolved zone before normalization; all 15 PostgreSQL workflow tests passed |
| `gradle integrationTest --tests 'org.studentdiscounts.submission.integration.KafkaLifecycleIT'` | Concurrent correction allowed approval to reuse observations from the previous version; expected rejection was absent | Prepared observations bound to locked immutable version; all 10 lifecycle tests passed |
| `gradle test --tests 'org.studentdiscounts.submission.submission.SubmissionApiTest'` | Object/array values for submission type threw Jackson exceptions and returned 500 during enum cutover | Explicit semantic type validation preserves 422 `TARGET_INVALID`; all 8 API tests passed |
| `gradle integrationTest --tests 'org.studentdiscounts.submission.integration.ValidationFairnessIT'` | A batch filled by quarantined or dependency-delayed work left a later ready submission permanently SUBMITTED | Bounded keyset round-robin selection advances later work and wraps without duplicate IDs; both real-PostgreSQL cases passed |

The first actual-jar smoke also caught direct field access through a Spring repository proxy (HTTP 500). Concrete use cases with explicit dependencies plus a full Spring HTTP create-draft regression replaced that coupling. The final actual-jar contract smoke passed; this does not claim every platform feature was implemented or every test had a captured preimplementation behavioral red.

## Operations, backup and recovery

Health is public/redacted. Liveness excludes DB/Kafka, readiness includes both. Detailed health, metrics and Prometheus require SENIOR_MODERATOR. Request latency histograms/p50/p95/p99 plus workflow backlog/outbox age and consumer/DLQ failures support diagnosis; these are not proof of SLO/load targets. Do not publicly expose broker/admin/management ports in production.

PostgreSQL contains authoritative drafts, append-only snapshots/history, permanent successful idempotency responses, inbox and outbox. Back up all together; cross-service IDs have no FKs. Example backup/restore of this service only (choose a new/empty target; restore must not merge into a live database):

```bash
mkdir -p backups
# Host shell redirect intentionally creates a local backup; protect it as private data.
docker compose exec -T postgres pg_dump -U submission -d submission -Fc > backups/submission.dump
# Stop this service's writer processes first, then restore into an EMPTY separately provisioned database:
# docker compose exec -T postgres pg_restore -U submission -d submission_restore < backups/submission.dump
```

Use actual configured DB user if changed. Protect/encrypt backups; practice restores and align Kafka retention/consumer offsets with restored inbox/outbox. Never disable immutable triggers or truncate production snapshots for cleanup. Keep named DB/Kafka volumes; `docker compose down` preserves them, `down -v` destroys service data and is not a recovery command. After outage/restart, committed validation/outbox work remains pending; re-enable worker flags and monitor backlog until drained. Preserve eventId during replay; reprocess DLT only after identifying/fixing the cause, retaining original identity and review tuple. Never skip poison messages as success or rewrite reviewed snapshots. Schema migration failure should be investigated, not bypassed by clearing Flyway history. Real Catalog/Media/Geo/organization/Moderation services and production security/backup policies are deployment prerequisites; this repository does not establish full-platform MVP, SBOM/signing infrastructure, or load/SLO compliance.
