ALTER TABLE submissions
    ADD COLUMN draft_base_place_row_version integer CHECK (draft_base_place_row_version >= 0);

ALTER TABLE submission_versions
    ADD COLUMN base_place_row_version integer CHECK (base_place_row_version >= 0);
