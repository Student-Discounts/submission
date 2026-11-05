ALTER TABLE submissions ADD COLUMN draft_base_offer_row_version int;
ALTER TABLE submissions ADD COLUMN published_offer_ids uuid[];
ALTER TABLE submission_versions ADD COLUMN resolved_timezone varchar(64);

CREATE TABLE idempotency_requests (
    actor_id uuid NOT NULL, operation text NOT NULL, request_key varchar(128) NOT NULL,
    request_sha256 char(64) NOT NULL, response_status int, response_body jsonb,
    response_etag text, created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (actor_id, operation, request_key)
);
CREATE TABLE outbox_events (
    id uuid PRIMARY KEY, submission_id uuid NOT NULL REFERENCES submissions(id),
    aggregate_sequence bigint NOT NULL, event_type text NOT NULL, payload jsonb NOT NULL,
    occurred_at timestamptz NOT NULL, published_at timestamptz,
    UNIQUE(submission_id, aggregate_sequence)
);
CREATE INDEX outbox_events_unpublished_idx ON outbox_events(occurred_at,id) WHERE published_at IS NULL;
CREATE TABLE inbox_events (
    consumer_name text NOT NULL, event_id uuid NOT NULL, processed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY(consumer_name,event_id)
);
CREATE TABLE submission_status_history (
    id uuid PRIMARY KEY, submission_id uuid NOT NULL REFERENCES submissions(id),
    version_id uuid REFERENCES submission_versions(id), from_status submission_status,
    to_status submission_status NOT NULL, actor_id uuid, reason text, correlation_id text NOT NULL,
    occurred_at timestamptz NOT NULL
);
CREATE INDEX submission_status_history_submission_idx ON submission_status_history(submission_id,occurred_at,id);
CREATE TRIGGER submission_status_history_append_only BEFORE UPDATE OR DELETE ON submission_status_history
    FOR EACH ROW EXECUTE FUNCTION submission_versions_reject_change();
CREATE TRIGGER submission_status_history_no_truncate BEFORE TRUNCATE ON submission_status_history
    FOR EACH STATEMENT EXECUTE FUNCTION submission_versions_reject_change();
CREATE TABLE submission_media_checks (
    version_id uuid NOT NULL REFERENCES submission_versions(id), media_asset_id uuid NOT NULL,
    status text NOT NULL CHECK(status IN ('QUARANTINED','PROCESSING','CLEAN','REJECTED')),
    asset_revision bigint NOT NULL,
    PRIMARY KEY(version_id,media_asset_id)
);
CREATE TABLE api_rate_limits (
    actor_id uuid NOT NULL, window_start timestamptz NOT NULL, request_count int NOT NULL CHECK(request_count>0),
    PRIMARY KEY(actor_id,window_start)
);
