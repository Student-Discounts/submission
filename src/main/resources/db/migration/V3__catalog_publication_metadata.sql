ALTER TABLE submissions ADD COLUMN draft_base_place_row_version int CHECK (draft_base_place_row_version >= 0);
ALTER TABLE submission_versions ADD COLUMN base_place_row_version int CHECK (base_place_row_version >= 0);

CREATE TABLE publication_metadata_outbox (
    id uuid PRIMARY KEY,
    submission_id uuid NOT NULL REFERENCES submissions(id),
    version_id uuid NOT NULL UNIQUE REFERENCES submission_versions(id),
    payload jsonb NOT NULL,
    occurred_at timestamptz NOT NULL,
    published_at timestamptz
);
CREATE INDEX publication_metadata_outbox_unpublished_idx
    ON publication_metadata_outbox(occurred_at,id) WHERE published_at IS NULL;
