-- Every time column is milliseconds since the epoch in the server's clock.
-- A phone timestamp is stored twice: as the phone sent it (*_client_ms) and
-- turned into server time by the clock skew measured on the same request.

CREATE TABLE IF NOT EXISTS utterances (
    sequence                     INTEGER PRIMARY KEY,
    utterance_id                 TEXT NOT NULL UNIQUE,
    client_utterance_id          TEXT NOT NULL,
    principal_id                 TEXT NOT NULL,
    mode                         TEXT NOT NULL,
    strategy                     TEXT NOT NULL,
    trigger                      TEXT NOT NULL,
    speech_started_at_client_ms  INTEGER NOT NULL,
    speech_started_at_ms         INTEGER NOT NULL,
    client_clock_skew_ms         INTEGER NOT NULL,
    received_at_ms               INTEGER NOT NULL,
    ended_at_client_ms           INTEGER,
    ended_at_ms                  INTEGER,
    finished_at_ms               INTEGER,
    finalized_at_ms              INTEGER,
    cancelled_at_ms              INTEGER,
    audio_filename               TEXT NOT NULL DEFAULT '',
    audio_content_type           TEXT NOT NULL DEFAULT '',
    audio_size_bytes             INTEGER NOT NULL DEFAULT 0,
    audio_received_at_ms         INTEGER,
    errors                       TEXT NOT NULL DEFAULT '[]',
    UNIQUE (principal_id, client_utterance_id)
);

CREATE TABLE IF NOT EXISTS segments (
    utterance_id                 TEXT NOT NULL REFERENCES utterances(utterance_id),
    segment_index                INTEGER NOT NULL,
    text                         TEXT NOT NULL,
    speech_ended_at_client_ms    INTEGER NOT NULL,
    speech_ended_at_ms           INTEGER NOT NULL,
    client_clock_skew_ms         INTEGER NOT NULL,
    received_at_ms               INTEGER NOT NULL,
    PRIMARY KEY (utterance_id, segment_index)
);

-- One row per llm-bridge-server session an utterance sent text to.
CREATE TABLE IF NOT EXISTS utterance_sessions (
    session_id                   TEXT PRIMARY KEY,
    utterance_id                 TEXT NOT NULL REFERENCES utterances(utterance_id),
    segment_indexes              TEXT NOT NULL,
    text                         TEXT NOT NULL,
    role                         TEXT NOT NULL,
    pooled                       INTEGER NOT NULL,
    pooled_session_state         TEXT NOT NULL DEFAULT '',
    effort                       TEXT NOT NULL,
    model                        TEXT NOT NULL,
    instance_id                  TEXT NOT NULL,
    dispatch_started_at_ms       INTEGER NOT NULL,
    dispatched_at_ms             INTEGER,
    first_block_at_ms            INTEGER,
    result_at_ms                 INTEGER,
    stopped_at_ms                INTEGER,
    stop_reason                  TEXT NOT NULL DEFAULT '',
    error                        TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS utterance_sessions_by_utterance ON utterance_sessions(utterance_id);

CREATE TABLE IF NOT EXISTS decisions (
    sequence                     INTEGER PRIMARY KEY,
    utterance_id                 TEXT NOT NULL REFERENCES utterances(utterance_id),
    session_id                   TEXT NOT NULL,
    role                         TEXT NOT NULL,
    reason                       TEXT NOT NULL,
    decided_at_ms                INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS decisions_by_utterance ON decisions(utterance_id);

-- One row per question put to the continuation judge.
CREATE TABLE IF NOT EXISTS continuation_judgments (
    sequence                     INTEGER PRIMARY KEY,
    utterance_id                 TEXT NOT NULL REFERENCES utterances(utterance_id),
    segment_index                INTEGER NOT NULL,
    earlier_text                 TEXT NOT NULL,
    new_text                     TEXT NOT NULL,
    continues                    INTEGER,
    reason                       TEXT NOT NULL DEFAULT '',
    model                        TEXT NOT NULL DEFAULT '',
    model_role                   TEXT NOT NULL,
    started_at_ms                INTEGER NOT NULL,
    duration_ms                  INTEGER NOT NULL,
    error                        TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS continuation_judgments_by_utterance ON continuation_judgments(utterance_id);

-- Sessions started ahead of speech. A row is taken by at most one utterance.
CREATE TABLE IF NOT EXISTS pooled_sessions (
    session_id                   TEXT PRIMARY KEY,
    principal_id                 TEXT NOT NULL,
    instance_id                  TEXT NOT NULL,
    effort                       TEXT NOT NULL,
    model                        TEXT NOT NULL,
    working_directory            TEXT NOT NULL,
    created_at_ms                INTEGER NOT NULL,
    taken_at_ms                  INTEGER,
    taken_by_utterance_id        TEXT NOT NULL DEFAULT '',
    released_at_ms               INTEGER,
    release_reason               TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS stored_settings (
    key                          TEXT PRIMARY KEY,
    value                        TEXT NOT NULL
);
