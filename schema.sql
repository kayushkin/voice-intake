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
    -- Streaming (contract v2): the phone streams PCM and this service ends
    -- the segments. Audio byte 0 is speech_started_at_ms.
    audio_streaming              INTEGER NOT NULL DEFAULT 0,
    audio_format                 TEXT NOT NULL DEFAULT '',
    stream_received_bytes        INTEGER NOT NULL DEFAULT 0,
    stop_recording_reason        TEXT NOT NULL DEFAULT '',
    stop_recording_sent_at_ms    INTEGER,
    stopped_at_client_ms         INTEGER,
    stopped_at_ms                INTEGER,
    stop_reason                  TEXT NOT NULL DEFAULT '',
    -- Set when the utterance is a replay of a stored recording or text.
    replay_id                    TEXT NOT NULL DEFAULT '',
    -- Where the words go: 'pool' (a voice session, split and judged),
    -- 'new_session' (an ordinary chat session), 'session' (the next message
    -- of target_session_id) or 'signal' (the answer to target_signal_id).
    target_kind                  TEXT NOT NULL DEFAULT 'pool',
    target_session_id            TEXT NOT NULL DEFAULT '',
    target_signal_id             TEXT NOT NULL DEFAULT '',
    -- Targets session, signal and new_session hold the joined words until
    -- the person confirms them. confirmation_state: '' (nothing held yet),
    -- 'awaiting', then 'confirmed' (sent), 'cancelled' or 'expired' (never
    -- sent).
    confirm_before_send          INTEGER NOT NULL DEFAULT 0,
    confirmation_state           TEXT NOT NULL DEFAULT '',
    confirmation_text            TEXT NOT NULL DEFAULT '',
    confirmation_expires_at_ms   INTEGER,
    confirmation_resolved_at_ms  INTEGER,
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
    -- 'text' when the phone sent the words, 'audio' when it sent the sound
    -- and this service had it transcribed. The columns below are for audio.
    source                       TEXT NOT NULL DEFAULT 'text',
    audio_size_bytes             INTEGER,
    audio_duration_ms            INTEGER,
    transcription_started_at_ms  INTEGER,
    transcribed_at_ms            INTEGER,
    transcript_raw               TEXT NOT NULL DEFAULT '',
    -- 'stream' segments: where the detector heard the speech, as offsets
    -- into the streamed audio, and when it decided the speech had ended.
    detected_speech_start_offset_ms INTEGER,
    detected_speech_end_offset_ms   INTEGER,
    endpointed_at_ms             INTEGER,
    transcriber                  TEXT NOT NULL DEFAULT '',
    -- 'stream' segments: the detector's mean speech probability over the
    -- segment, the audio it heard as surely speech, and why a segment too
    -- weakly heard holds no words (it was neither transcribed nor sent).
    mean_speech_probability      REAL,
    voiced_ms                    INTEGER,
    dropped_reason               TEXT NOT NULL DEFAULT '',
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
    error                        TEXT NOT NULL DEFAULT '',
    working_directory            TEXT NOT NULL DEFAULT '',
    reply_text                   TEXT NOT NULL DEFAULT '',
    turn_cost_usd                REAL
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

-- Recordings kept for replay, with the known words and the true end of
-- speech, which endpoint delay is measured against.
CREATE TABLE IF NOT EXISTS replay_recordings (
    sequence                     INTEGER PRIMARY KEY,
    recording_id                 TEXT NOT NULL UNIQUE,
    filename                     TEXT NOT NULL,
    expected_text                TEXT NOT NULL,
    duration_ms                  INTEGER NOT NULL,
    true_speech_end_ms           INTEGER NOT NULL,
    true_speech_end_source       TEXT NOT NULL,
    words                        TEXT NOT NULL DEFAULT '[]',
    created_at_ms                INTEGER NOT NULL
);

-- A named way to run a replay: detector, transcriber, and where the text
-- goes.
CREATE TABLE IF NOT EXISTS replay_configurations (
    sequence                     INTEGER PRIMARY KEY,
    configuration_id             TEXT NOT NULL UNIQUE,
    name                         TEXT NOT NULL,
    detector                     TEXT NOT NULL,
    endpoint_silence_ms          INTEGER NOT NULL,
    transcriber                  TEXT NOT NULL,
    dispatch                     TEXT NOT NULL,
    instance_id                  TEXT NOT NULL DEFAULT '',
    model                        TEXT NOT NULL DEFAULT '',
    effort                       TEXT NOT NULL DEFAULT '',
    working_directory            TEXT NOT NULL DEFAULT '',
    prompt_variant               TEXT NOT NULL DEFAULT '',
    warm_session                 INTEGER NOT NULL DEFAULT 0,
    max_budget_usd               REAL NOT NULL DEFAULT 0,
    created_at_ms                INTEGER NOT NULL,
    -- 0: no high-pass filter, and every segment transcribed.
    high_pass_cutoff_hz          INTEGER NOT NULL DEFAULT 0,
    minimum_segment_mean_speech_probability REAL NOT NULL DEFAULT 0
);

-- One replay: a recording or a text run once under a configuration.
CREATE TABLE IF NOT EXISTS replays (
    sequence                     INTEGER PRIMARY KEY,
    replay_id                    TEXT NOT NULL UNIQUE,
    configuration_id             TEXT NOT NULL REFERENCES replay_configurations(configuration_id),
    recording_id                 TEXT NOT NULL DEFAULT '',
    text                         TEXT NOT NULL DEFAULT '',
    utterance_id                 TEXT NOT NULL DEFAULT '',
    started_at_ms                INTEGER NOT NULL,
    finished_at_ms               INTEGER,
    -- dispatch 'oneshot': one model call with no session.
    oneshot_started_at_ms        INTEGER,
    oneshot_answered_at_ms       INTEGER,
    oneshot_model                TEXT NOT NULL DEFAULT '',
    oneshot_text                 TEXT NOT NULL DEFAULT '',
    error                        TEXT NOT NULL DEFAULT ''
);

-- The phone app's published builds, one row per version code, kept forever.
-- The APK lies in the data directory's app-releases/ under filename.
CREATE TABLE IF NOT EXISTS app_releases (
    version_code     INTEGER PRIMARY KEY,
    version_name     TEXT NOT NULL,
    commit_sha       TEXT NOT NULL,
    sha256           TEXT NOT NULL,
    size_bytes       INTEGER NOT NULL,
    filename         TEXT NOT NULL,
    published_at_ms  INTEGER NOT NULL
);
