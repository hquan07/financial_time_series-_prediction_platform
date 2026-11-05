ALTER TABLE dataset_versions DROP CONSTRAINT dataset_versions_layer_check;
ALTER TABLE dataset_versions ADD CONSTRAINT dataset_versions_layer_check
    CHECK (layer IN ('silver', 'gold'));

CREATE TABLE dataset_parents (
    child_dataset_id UUID NOT NULL REFERENCES dataset_versions(dataset_id),
    parent_dataset_id UUID NOT NULL REFERENCES dataset_versions(dataset_id),
    ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
    relationship TEXT NOT NULL CHECK (relationship IN
        ('market_input', 'macro_input', 'feature_input', 'label_input')),
    PRIMARY KEY (child_dataset_id, parent_dataset_id),
    UNIQUE (child_dataset_id, ordinal),
    CHECK (child_dataset_id <> parent_dataset_id)
);
