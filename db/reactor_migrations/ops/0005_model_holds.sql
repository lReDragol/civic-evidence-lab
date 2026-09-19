CREATE TABLE civic_model_holds (
    provider TEXT NOT NULL,
    account_id TEXT NOT NULL DEFAULT '',
    until_at REAL NOT NULL,
    blocked INTEGER NOT NULL DEFAULT 0 CHECK(blocked IN (0,1)),
    reason TEXT NOT NULL,
    updated_at REAL NOT NULL,
    PRIMARY KEY(provider,account_id)
);
