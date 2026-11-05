CREATE TYPE submission_type AS ENUM ('NEW_PLACE', 'NEW_OFFER', 'CHANGE');
CREATE TYPE submission_status AS ENUM (
    'DRAFT', 'SUBMITTED', 'VALIDATING', 'CANCELLED', 'REJECTED', 'PENDING_REVIEW',
    'IN_REVIEW', 'CHANGES_REQUESTED', 'APPROVED', 'PUBLISHED', 'SUSPENDED', 'ARCHIVED'
);

CREATE TABLE submissions (
    id                 uuid              PRIMARY KEY,
    type               submission_type   NOT NULL,
    status             submission_status NOT NULL DEFAULT 'DRAFT',
    author_id          uuid              NOT NULL,
    target_place_id    uuid,
    target_offer_id    uuid,
    draft_payload      jsonb,
    current_version_no int,
    row_version        int               NOT NULL DEFAULT 0,
    created_at         timestamptz       NOT NULL DEFAULT now(),
    updated_at         timestamptz       NOT NULL DEFAULT now(),
    CONSTRAINT submissions_target_matches_type CHECK (
        (type = 'NEW_PLACE' AND target_place_id IS NULL AND target_offer_id IS NULL)
        OR (type = 'NEW_OFFER' AND target_place_id IS NOT NULL AND target_offer_id IS NULL)
        OR (type = 'CHANGE' AND target_place_id IS NOT NULL AND target_offer_id IS NOT NULL)
    ),
    CONSTRAINT submissions_draft_has_no_version CHECK (
        (status = 'DRAFT') = (current_version_no IS NULL)
    )
);

CREATE INDEX submissions_author_idx ON submissions (author_id, created_at DESC);
CREATE INDEX submissions_status_idx ON submissions (status, created_at);
CREATE INDEX submissions_target_offer_idx ON submissions (target_offer_id) WHERE target_offer_id IS NOT NULL;

CREATE TABLE submission_versions (
    id                     uuid        PRIMARY KEY,
    submission_id          uuid        NOT NULL REFERENCES submissions (id),
    version_no             int         NOT NULL,
    previous_version_id    uuid,
    payload                jsonb       NOT NULL,
    payload_schema_version int         NOT NULL,
    content_sha256         char(64)    NOT NULL,
    base_offer_row_version int,
    created_by             uuid        NOT NULL,
    created_at             timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT submission_versions_no_uniq UNIQUE (submission_id, version_no),
    CONSTRAINT submission_versions_id_uniq UNIQUE (submission_id, id),
    CONSTRAINT submission_versions_previous_same_submission
        FOREIGN KEY (submission_id, previous_version_id)
        REFERENCES submission_versions (submission_id, id),
    CONSTRAINT submission_versions_no_positive CHECK (version_no >= 1),
    CONSTRAINT submission_versions_chain CHECK ((version_no = 1) = (previous_version_id IS NULL)),
    CONSTRAINT submission_versions_schema_positive CHECK (payload_schema_version >= 1),
    CONSTRAINT submission_versions_sha256_format CHECK (content_sha256 ~ '^[0-9a-f]{64}$')
);

ALTER TABLE submissions
    ADD CONSTRAINT submissions_current_version_exists
    FOREIGN KEY (id, current_version_no)
    REFERENCES submission_versions (submission_id, version_no);

CREATE FUNCTION submission_versions_reject_change() RETURNS trigger
    LANGUAGE plpgsql AS
$$
BEGIN
    RAISE EXCEPTION 'submission_versions is append-only: % is not allowed', TG_OP
        USING ERRCODE = 'restrict_violation';
END;
$$;

CREATE TRIGGER submission_versions_append_only
    BEFORE UPDATE OR DELETE ON submission_versions
    FOR EACH ROW EXECUTE FUNCTION submission_versions_reject_change();

CREATE TRIGGER submission_versions_no_truncate
    BEFORE TRUNCATE ON submission_versions
    FOR EACH STATEMENT EXECUTE FUNCTION submission_versions_reject_change();
