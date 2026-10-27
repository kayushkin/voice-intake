package voiceintake

import (
	"database/sql"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

//go:embed schema.sql
var schemaSQL string

// ErrNotFound is a 404.
var ErrNotFound = errors.New("not found")

// Store owns the database.
type Store struct {
	db *sql.DB
}

// OpenStore creates the data directory if needed, opens the database and
// applies the schema, which is create-only and so safe on every start.
func OpenStore(dataDirectory string) (*Store, error) {
	if dataDirectory == "" {
		return nil, errors.New("open store: data directory is empty")
	}
	if err := os.MkdirAll(dataDirectory, 0o700); err != nil {
		return nil, fmt.Errorf("create data directory: %w", err)
	}
	db, err := sql.Open("sqlite3", filepath.Join(dataDirectory, "voice-intake.db")+"?_foreign_keys=on&_busy_timeout=5000&_txlock=immediate")
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}
	// One connection: writes are small and serialised, and minting an id
	// from MAX(sequence) stays safe.
	db.SetMaxOpenConns(1)
	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		db.Close()
		return nil, fmt.Errorf("set WAL: %w", err)
	}
	if _, err := db.Exec(schemaSQL); err != nil {
		db.Close()
		return nil, fmt.Errorf("apply schema: %w", err)
	}
	if err := addMissingColumns(db); err != nil {
		db.Close()
		return nil, err
	}
	return &Store{db: db}, nil
}

// columnsAddedAfterTheFirstSchema are columns a database made by an earlier
// schema lacks, since CREATE TABLE IF NOT EXISTS leaves an existing table as
// it is. Each is added once, with the same definition as in schema.sql.
var columnsAddedAfterTheFirstSchema = []struct{ table, column, definition string }{
	{"segments", "source", "TEXT NOT NULL DEFAULT 'text'"},
	{"segments", "audio_size_bytes", "INTEGER"},
	{"segments", "audio_duration_ms", "INTEGER"},
	{"segments", "transcription_started_at_ms", "INTEGER"},
	{"segments", "transcribed_at_ms", "INTEGER"},
	{"segments", "transcript_raw", "TEXT NOT NULL DEFAULT ''"},
	{"utterances", "audio_streaming", "INTEGER NOT NULL DEFAULT 0"},
	{"utterances", "audio_format", "TEXT NOT NULL DEFAULT ''"},
	{"utterances", "stream_received_bytes", "INTEGER NOT NULL DEFAULT 0"},
	{"utterances", "stop_recording_reason", "TEXT NOT NULL DEFAULT ''"},
	{"utterances", "stop_recording_sent_at_ms", "INTEGER"},
	{"utterances", "stopped_at_client_ms", "INTEGER"},
	{"utterances", "stopped_at_ms", "INTEGER"},
	{"utterances", "stop_reason", "TEXT NOT NULL DEFAULT ''"},
	{"utterances", "replay_id", "TEXT NOT NULL DEFAULT ''"},
	{"segments", "detected_speech_start_offset_ms", "INTEGER"},
	{"segments", "detected_speech_end_offset_ms", "INTEGER"},
	{"segments", "endpointed_at_ms", "INTEGER"},
	{"segments", "transcriber", "TEXT NOT NULL DEFAULT ''"},
	{"utterance_sessions", "working_directory", "TEXT NOT NULL DEFAULT ''"},
	{"utterance_sessions", "reply_text", "TEXT NOT NULL DEFAULT ''"},
	{"utterance_sessions", "turn_cost_usd", "REAL"},
}

func addMissingColumns(db *sql.DB) error {
	for _, added := range columnsAddedAfterTheFirstSchema {
		var count int
		if err := db.QueryRow(`SELECT COUNT(*) FROM pragma_table_info(?) WHERE name = ?`, added.table, added.column).Scan(&count); err != nil {
			return fmt.Errorf("read columns of %s: %w", added.table, err)
		}
		if count > 0 {
			continue
		}
		if _, err := db.Exec(fmt.Sprintf(`ALTER TABLE %s ADD COLUMN %s %s`, added.table, added.column, added.definition)); err != nil {
			return fmt.Errorf("add column %s.%s: %w", added.table, added.column, err)
		}
	}
	return nil
}

// Close closes the database.
func (s *Store) Close() error { return s.db.Close() }

// StoredSettings is the store as servicesettings.StoredValues.
type StoredSettings struct{ store *Store }

// Settings returns the store's settings table for the registry.
func (s *Store) Settings() StoredSettings { return StoredSettings{store: s} }

// Load returns every stored setting.
func (stored StoredSettings) Load() (map[string]string, error) {
	rows, err := stored.store.db.Query(`SELECT key, value FROM stored_settings`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	values := map[string]string{}
	for rows.Next() {
		var key, value string
		if err := rows.Scan(&key, &value); err != nil {
			return nil, err
		}
		values[key] = value
	}
	return values, rows.Err()
}

// Save writes one setting.
func (stored StoredSettings) Save(key, value string) error {
	_, err := stored.store.db.Exec(`INSERT INTO stored_settings (key, value) VALUES (?, ?)
		ON CONFLICT(key) DO UPDATE SET value = excluded.value`, key, value)
	return err
}

// Utterance is one recording: one press of a trigger.
type Utterance struct {
	UtteranceID             string   `json:"utterance_id"`
	ClientUtteranceID       string   `json:"client_utterance_id"`
	PrincipalID             string   `json:"principal_id"`
	Mode                    string   `json:"mode"`
	Strategy                string   `json:"strategy"`
	Trigger                 string   `json:"trigger"`
	SpeechStartedAtClientMs int64    `json:"speech_started_at_client_ms"`
	SpeechStartedAtMs       int64    `json:"speech_started_at_ms"`
	ClientClockSkewMs       int64    `json:"client_clock_skew_ms"`
	ReceivedAtMs            int64    `json:"received_at_ms"`
	EndedAtClientMs         *int64   `json:"ended_at_client_ms"`
	EndedAtMs               *int64   `json:"ended_at_ms"`
	FinishedAtMs            *int64   `json:"finished_at_ms"`
	FinalizedAtMs           *int64   `json:"finalized_at_ms"`
	CancelledAtMs           *int64   `json:"cancelled_at_ms"`
	AudioFilename           string   `json:"audio_filename"`
	AudioContentType        string   `json:"audio_content_type"`
	AudioSizeBytes          int64    `json:"audio_size_bytes"`
	AudioReceivedAtMs       *int64   `json:"audio_received_at_ms"`
	Errors                  []string `json:"errors"`
	AudioStreaming          bool     `json:"audio_streaming"`
	AudioFormat             string   `json:"audio_format"`
	StreamReceivedBytes     int64    `json:"stream_received_bytes"`
	StopRecordingReason     string   `json:"stop_recording_reason"`
	StopRecordingSentAtMs   *int64   `json:"stop_recording_sent_at_ms"`
	StoppedAtClientMs       *int64   `json:"stopped_at_client_ms"`
	StoppedAtMs             *int64   `json:"stopped_at_ms"`
	StopReason              string   `json:"stop_reason"`
	ReplayID                string   `json:"replay_id"`
}

// Segment is speech between two pauses.
type Segment struct {
	UtteranceID           string `json:"-"`
	Index                 int    `json:"index"`
	Text                  string `json:"text"`
	SpeechEndedAtClientMs int64  `json:"speech_ended_at_client_ms"`
	SpeechEndedAtMs       int64  `json:"speech_ended_at_ms"`
	ClientClockSkewMs     int64  `json:"client_clock_skew_ms"`
	ReceivedAtMs          int64  `json:"received_at_ms"`
	// Source is SegmentSourceText or SegmentSourceAudio. The fields after it
	// are set only for audio: the WAV's size and length, when transcription
	// started and ended, how long it took, and the transcript as the
	// transcription service gave it, before whitespace was joined and a
	// non-speech marker dropped.
	Source                   string `json:"source"`
	AudioSizeBytes           *int64 `json:"audio_size_bytes,omitempty"`
	AudioDurationMs          *int64 `json:"audio_duration_ms,omitempty"`
	TranscriptionStartedAtMs *int64 `json:"transcription_started_at_ms,omitempty"`
	TranscribedAtMs          *int64 `json:"transcribed_at_ms,omitempty"`
	TranscriptionMs          *int64 `json:"transcription_ms,omitempty"`
	TranscriptRaw            string `json:"transcript_raw,omitempty"`
	// For SegmentSourceStream: where the detector heard the speech, as
	// offsets into the streamed audio, when it ended the segment, and the
	// transcriber used.
	DetectedSpeechStartOffsetMs *int64 `json:"detected_speech_start_offset_ms,omitempty"`
	DetectedSpeechEndOffsetMs   *int64 `json:"detected_speech_end_offset_ms,omitempty"`
	EndpointedAtMs              *int64 `json:"endpointed_at_ms,omitempty"`
	Transcriber                 string `json:"transcriber,omitempty"`
}

// Where a segment's text came from.
const (
	SegmentSourceText  = "text"
	SegmentSourceAudio = "audio"
	// SegmentSourceStream is a segment this service cut from streamed audio
	// with its own detector, and transcribed.
	SegmentSourceStream = "stream"
)

// Session roles. A session starts speculative; a decision makes it kept,
// separate or superseded.
const (
	RoleSpeculative = "speculative"
	RoleKept        = "kept"
	RoleSeparate    = "separate"
	RoleSuperseded  = "superseded"
)

// UtteranceSession is one llm-bridge-server session an utterance sent text to.
type UtteranceSession struct {
	SessionID           string   `json:"session_id"`
	UtteranceID         string   `json:"-"`
	SegmentIndexes      []int    `json:"segment_indexes"`
	Text                string   `json:"text"`
	Role                string   `json:"role"`
	Pooled              bool     `json:"pooled"`
	PooledSessionState  string   `json:"pooled_session_state"`
	Effort              string   `json:"effort"`
	Model               string   `json:"model"`
	InstanceID          string   `json:"instance_id"`
	DispatchStartedAtMs int64    `json:"dispatch_started_at_ms"`
	DispatchedAtMs      *int64   `json:"dispatched_at_ms"`
	FirstBlockAtMs      *int64   `json:"first_block_at_ms"`
	ResultAtMs          *int64   `json:"result_at_ms"`
	StoppedAtMs         *int64   `json:"stopped_at_ms"`
	StopReason          string   `json:"stop_reason"`
	Error               string   `json:"error"`
	WorkingDirectory    string   `json:"working_directory"`
	ReplyText           string   `json:"reply_text"`
	TurnCostUSD         *float64 `json:"turn_cost_usd"`
}

// Decision is one role a session was given, and why.
type Decision struct {
	SessionID   string `json:"session_id"`
	Role        string `json:"role"`
	Reason      string `json:"reason"`
	DecidedAtMs int64  `json:"decided_at_ms"`
}

// ContinuationJudgment is one question put to the continuation judge.
type ContinuationJudgment struct {
	SegmentIndex int    `json:"segment_index"`
	EarlierText  string `json:"earlier_text"`
	NewText      string `json:"new_text"`
	Continues    *bool  `json:"continues"`
	Reason       string `json:"reason"`
	Model        string `json:"model"`
	ModelRole    string `json:"model_role"`
	StartedAtMs  int64  `json:"started_at_ms"`
	DurationMs   int64  `json:"duration_ms"`
	Error        string `json:"error"`
}

const utteranceColumns = `utterance_id, client_utterance_id, principal_id, mode, strategy, trigger,
	speech_started_at_client_ms, speech_started_at_ms, client_clock_skew_ms, received_at_ms,
	ended_at_client_ms, ended_at_ms, finished_at_ms, finalized_at_ms, cancelled_at_ms,
	audio_filename, audio_content_type, audio_size_bytes, audio_received_at_ms, errors,
	audio_streaming, audio_format, stream_received_bytes, stop_recording_reason, stop_recording_sent_at_ms,
	stopped_at_client_ms, stopped_at_ms, stop_reason, replay_id`

type rowScanner interface {
	Scan(destination ...any) error
}

func scanUtterance(row rowScanner) (Utterance, error) {
	var utterance Utterance
	var errorsJSON string
	err := row.Scan(&utterance.UtteranceID, &utterance.ClientUtteranceID, &utterance.PrincipalID,
		&utterance.Mode, &utterance.Strategy, &utterance.Trigger,
		&utterance.SpeechStartedAtClientMs, &utterance.SpeechStartedAtMs, &utterance.ClientClockSkewMs, &utterance.ReceivedAtMs,
		&utterance.EndedAtClientMs, &utterance.EndedAtMs, &utterance.FinishedAtMs, &utterance.FinalizedAtMs, &utterance.CancelledAtMs,
		&utterance.AudioFilename, &utterance.AudioContentType, &utterance.AudioSizeBytes, &utterance.AudioReceivedAtMs, &errorsJSON,
		&utterance.AudioStreaming, &utterance.AudioFormat, &utterance.StreamReceivedBytes, &utterance.StopRecordingReason, &utterance.StopRecordingSentAtMs,
		&utterance.StoppedAtClientMs, &utterance.StoppedAtMs, &utterance.StopReason, &utterance.ReplayID)
	if errors.Is(err, sql.ErrNoRows) {
		return Utterance{}, ErrNotFound
	}
	if err != nil {
		return Utterance{}, err
	}
	if err := json.Unmarshal([]byte(errorsJSON), &utterance.Errors); err != nil {
		return Utterance{}, fmt.Errorf("utterance %s errors column: %w", utterance.UtteranceID, err)
	}
	return utterance, nil
}

// CreateUtterance inserts an utterance, or returns the row the same person
// already made with the same client_utterance_id; created says which.
func (s *Store) CreateUtterance(utterance Utterance) (Utterance, bool, error) {
	transaction, err := s.db.Begin()
	if err != nil {
		return Utterance{}, false, err
	}
	defer transaction.Rollback()
	existing, err := scanUtterance(transaction.QueryRow(`SELECT `+utteranceColumns+` FROM utterances WHERE principal_id = ? AND client_utterance_id = ?`,
		utterance.PrincipalID, utterance.ClientUtteranceID))
	if err == nil {
		return existing, false, nil
	}
	if !errors.Is(err, ErrNotFound) {
		return Utterance{}, false, err
	}
	var next int64
	if err := transaction.QueryRow(`SELECT COALESCE(MAX(sequence), 0) + 1 FROM utterances`).Scan(&next); err != nil {
		return Utterance{}, false, err
	}
	utterance.UtteranceID = fmt.Sprintf("utterance_%06d", next)
	utterance.Errors = []string{}
	_, err = transaction.Exec(`INSERT INTO utterances (sequence, utterance_id, client_utterance_id, principal_id, mode, strategy, trigger,
		speech_started_at_client_ms, speech_started_at_ms, client_clock_skew_ms, received_at_ms, audio_streaming, audio_format, replay_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		next, utterance.UtteranceID, utterance.ClientUtteranceID, utterance.PrincipalID, utterance.Mode, utterance.Strategy, utterance.Trigger,
		utterance.SpeechStartedAtClientMs, utterance.SpeechStartedAtMs, utterance.ClientClockSkewMs, utterance.ReceivedAtMs,
		utterance.AudioStreaming, utterance.AudioFormat, utterance.ReplayID)
	if err != nil {
		return Utterance{}, false, err
	}
	return utterance, true, transaction.Commit()
}

// GetUtterance reads one utterance.
func (s *Store) GetUtterance(utteranceID string) (Utterance, error) {
	return scanUtterance(s.db.QueryRow(`SELECT `+utteranceColumns+` FROM utterances WHERE utterance_id = ?`, utteranceID))
}

// ListUtterances returns one person's utterances, newest first.
func (s *Store) ListUtterances(principalID string, limit int) ([]Utterance, error) {
	rows, err := s.db.Query(`SELECT `+utteranceColumns+` FROM utterances WHERE principal_id = ? ORDER BY sequence DESC LIMIT ?`, principalID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	utterances := []Utterance{}
	for rows.Next() {
		utterance, err := scanUtterance(rows)
		if err != nil {
			return nil, err
		}
		utterances = append(utterances, utterance)
	}
	return utterances, rows.Err()
}

// PrincipalsWithUtterances lists every person who has made an utterance.
func (s *Store) PrincipalsWithUtterances() ([]string, error) {
	rows, err := s.db.Query(`SELECT DISTINCT principal_id FROM utterances ORDER BY principal_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var principals []string
	for rows.Next() {
		var principalID string
		if err := rows.Scan(&principalID); err != nil {
			return nil, err
		}
		principals = append(principals, principalID)
	}
	return principals, rows.Err()
}

// MarkUtteranceFinished records the phone's finish.
func (s *Store) MarkUtteranceFinished(utteranceID string, endedAtClientMs, endedAtMs, finishedAtMs int64) error {
	return s.exec(`UPDATE utterances SET ended_at_client_ms = ?, ended_at_ms = ?, finished_at_ms = ? WHERE utterance_id = ? AND finished_at_ms IS NULL`,
		endedAtClientMs, endedAtMs, finishedAtMs, utteranceID)
}

// MarkUtteranceFinalized records when the decisions became final.
func (s *Store) MarkUtteranceFinalized(utteranceID string, atMs int64) error {
	return s.exec(`UPDATE utterances SET finalized_at_ms = ? WHERE utterance_id = ? AND finalized_at_ms IS NULL`, atMs, utteranceID)
}

// MarkUtteranceCancelled records a cancel.
func (s *Store) MarkUtteranceCancelled(utteranceID string, atMs int64) error {
	return s.exec(`UPDATE utterances SET cancelled_at_ms = ? WHERE utterance_id = ? AND cancelled_at_ms IS NULL`, atMs, utteranceID)
}

// RecordUtteranceAudio records the audio the phone uploaded.
func (s *Store) RecordUtteranceAudio(utteranceID, filename, contentType string, sizeBytes, atMs int64) error {
	return s.exec(`UPDATE utterances SET audio_filename = ?, audio_content_type = ?, audio_size_bytes = ?, audio_received_at_ms = ? WHERE utterance_id = ?`,
		filename, contentType, sizeBytes, atMs, utteranceID)
}

// AppendUtteranceError adds one failure to the utterance's record.
func (s *Store) AppendUtteranceError(utteranceID, message string) error {
	return s.exec(`UPDATE utterances SET errors = json_insert(errors, '$[#]', ?) WHERE utterance_id = ?`, message, utteranceID)
}

// InsertSegment stores a segment; inserted is false when the index was
// already received, and then nothing changes.
func (s *Store) InsertSegment(segment Segment) (bool, error) {
	if !segmentSources[segment.Source] {
		return false, fmt.Errorf("segment %d of %s: source %q is not one of %s", segment.Index, segment.UtteranceID, segment.Source, sortedKeys(segmentSources))
	}
	result, err := s.db.Exec(`INSERT INTO segments (utterance_id, segment_index, text, speech_ended_at_client_ms, speech_ended_at_ms, client_clock_skew_ms, received_at_ms,
		source, audio_size_bytes, audio_duration_ms, transcription_started_at_ms, transcribed_at_ms, transcript_raw,
		detected_speech_start_offset_ms, detected_speech_end_offset_ms, endpointed_at_ms, transcriber)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT (utterance_id, segment_index) DO NOTHING`,
		segment.UtteranceID, segment.Index, segment.Text, segment.SpeechEndedAtClientMs, segment.SpeechEndedAtMs, segment.ClientClockSkewMs, segment.ReceivedAtMs,
		segment.Source, segment.AudioSizeBytes, segment.AudioDurationMs, segment.TranscriptionStartedAtMs, segment.TranscribedAtMs, segment.TranscriptRaw,
		segment.DetectedSpeechStartOffsetMs, segment.DetectedSpeechEndOffsetMs, segment.EndpointedAtMs, segment.Transcriber)
	if err != nil {
		return false, err
	}
	affected, err := result.RowsAffected()
	return affected == 1, err
}

// Segments returns an utterance's segments by index.
func (s *Store) Segments(utteranceID string) ([]Segment, error) {
	rows, err := s.db.Query(`SELECT utterance_id, segment_index, text, speech_ended_at_client_ms, speech_ended_at_ms, client_clock_skew_ms, received_at_ms,
		source, audio_size_bytes, audio_duration_ms, transcription_started_at_ms, transcribed_at_ms, transcript_raw,
		detected_speech_start_offset_ms, detected_speech_end_offset_ms, endpointed_at_ms, transcriber
		FROM segments WHERE utterance_id = ? ORDER BY segment_index`, utteranceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	segments := []Segment{}
	for rows.Next() {
		var segment Segment
		if err := rows.Scan(&segment.UtteranceID, &segment.Index, &segment.Text, &segment.SpeechEndedAtClientMs, &segment.SpeechEndedAtMs,
			&segment.ClientClockSkewMs, &segment.ReceivedAtMs, &segment.Source, &segment.AudioSizeBytes, &segment.AudioDurationMs,
			&segment.TranscriptionStartedAtMs, &segment.TranscribedAtMs, &segment.TranscriptRaw,
			&segment.DetectedSpeechStartOffsetMs, &segment.DetectedSpeechEndOffsetMs, &segment.EndpointedAtMs, &segment.Transcriber); err != nil {
			return nil, err
		}
		segment.TranscriptionMs = difference(segment.TranscriptionStartedAtMs, segment.TranscribedAtMs)
		segments = append(segments, segment)
	}
	return segments, rows.Err()
}

// SegmentReceived reports whether the utterance already holds this index.
func (s *Store) SegmentReceived(utteranceID string, index int) (bool, error) {
	var count int
	err := s.db.QueryRow(`SELECT COUNT(*) FROM segments WHERE utterance_id = ? AND segment_index = ?`, utteranceID, index).Scan(&count)
	return count > 0, err
}

// InsertUtteranceSession records a session an utterance sent text to.
func (s *Store) InsertUtteranceSession(session UtteranceSession) error {
	indexes, err := json.Marshal(session.SegmentIndexes)
	if err != nil {
		return err
	}
	return s.exec(`INSERT INTO utterance_sessions (session_id, utterance_id, segment_indexes, text, role, pooled, pooled_session_state, effort, model, instance_id, dispatch_started_at_ms, working_directory)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`,
		session.SessionID, session.UtteranceID, string(indexes), session.Text, session.Role, session.Pooled, session.PooledSessionState,
		session.Effort, session.Model, session.InstanceID, session.DispatchStartedAtMs, session.WorkingDirectory)
}

// SetSessionRole records a session's role.
func (s *Store) SetSessionRole(sessionID, role string) error {
	return s.exec(`UPDATE utterance_sessions SET role = ? WHERE session_id = ?`, role, sessionID)
}

// SetSessionDispatched records when the text was sent.
func (s *Store) SetSessionDispatched(sessionID string, atMs int64) error {
	return s.exec(`UPDATE utterance_sessions SET dispatched_at_ms = ? WHERE session_id = ?`, atMs, sessionID)
}

// SetSessionFirstBlock records the first block event, once.
func (s *Store) SetSessionFirstBlock(sessionID string, atMs int64) error {
	return s.exec(`UPDATE utterance_sessions SET first_block_at_ms = ? WHERE session_id = ? AND first_block_at_ms IS NULL`, atMs, sessionID)
}

// SetSessionResult records the result event, once.
func (s *Store) SetSessionResult(sessionID string, atMs int64) error {
	return s.exec(`UPDATE utterance_sessions SET result_at_ms = ? WHERE session_id = ? AND result_at_ms IS NULL`, atMs, sessionID)
}

// SetSessionReply records the result's text and what the turn cost.
func (s *Store) SetSessionReply(sessionID, text string, costUSD *float64) error {
	return s.exec(`UPDATE utterance_sessions SET reply_text = ?, turn_cost_usd = ? WHERE session_id = ?`, text, costUSD, sessionID)
}

// SetSessionStopped records that the session was stopped, and why.
func (s *Store) SetSessionStopped(sessionID string, atMs int64, reason string) error {
	return s.exec(`UPDATE utterance_sessions SET stopped_at_ms = ?, stop_reason = ? WHERE session_id = ? AND stopped_at_ms IS NULL`, atMs, reason, sessionID)
}

// SetSessionError records a failure on the session.
func (s *Store) SetSessionError(sessionID, message string) error {
	return s.exec(`UPDATE utterance_sessions SET error = CASE WHEN error = '' THEN ? ELSE error || '; ' || ? END WHERE session_id = ?`, message, message, sessionID)
}

// UtteranceSessions returns every session an utterance started, oldest first.
func (s *Store) UtteranceSessions(utteranceID string) ([]UtteranceSession, error) {
	rows, err := s.db.Query(`SELECT session_id, utterance_id, segment_indexes, text, role, pooled, pooled_session_state, effort, model, instance_id,
		dispatch_started_at_ms, dispatched_at_ms, first_block_at_ms, result_at_ms, stopped_at_ms, stop_reason, error,
		working_directory, reply_text, turn_cost_usd
		FROM utterance_sessions WHERE utterance_id = ? ORDER BY rowid`, utteranceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	sessions := []UtteranceSession{}
	for rows.Next() {
		var session UtteranceSession
		var indexes string
		if err := rows.Scan(&session.SessionID, &session.UtteranceID, &indexes, &session.Text, &session.Role, &session.Pooled, &session.PooledSessionState,
			&session.Effort, &session.Model, &session.InstanceID, &session.DispatchStartedAtMs, &session.DispatchedAtMs, &session.FirstBlockAtMs,
			&session.ResultAtMs, &session.StoppedAtMs, &session.StopReason, &session.Error,
			&session.WorkingDirectory, &session.ReplyText, &session.TurnCostUSD); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(indexes), &session.SegmentIndexes); err != nil {
			return nil, fmt.Errorf("session %s segment_indexes: %w", session.SessionID, err)
		}
		sessions = append(sessions, session)
	}
	return sessions, rows.Err()
}

// InsertDecision records a decision.
func (s *Store) InsertDecision(utteranceID string, decision Decision) error {
	return s.exec(`INSERT INTO decisions (utterance_id, session_id, role, reason, decided_at_ms) VALUES (?,?,?,?,?)`,
		utteranceID, decision.SessionID, decision.Role, decision.Reason, decision.DecidedAtMs)
}

// Decisions returns an utterance's decisions in the order they were made.
func (s *Store) Decisions(utteranceID string) ([]Decision, error) {
	rows, err := s.db.Query(`SELECT session_id, role, reason, decided_at_ms FROM decisions WHERE utterance_id = ? ORDER BY sequence`, utteranceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	decisions := []Decision{}
	for rows.Next() {
		var decision Decision
		if err := rows.Scan(&decision.SessionID, &decision.Role, &decision.Reason, &decision.DecidedAtMs); err != nil {
			return nil, err
		}
		decisions = append(decisions, decision)
	}
	return decisions, rows.Err()
}

// InsertContinuationJudgment records one question put to the judge.
func (s *Store) InsertContinuationJudgment(utteranceID string, judgment ContinuationJudgment) error {
	return s.exec(`INSERT INTO continuation_judgments (utterance_id, segment_index, earlier_text, new_text, continues, reason, model, model_role, started_at_ms, duration_ms, error)
		VALUES (?,?,?,?,?,?,?,?,?,?,?)`,
		utteranceID, judgment.SegmentIndex, judgment.EarlierText, judgment.NewText, judgment.Continues, judgment.Reason, judgment.Model,
		judgment.ModelRole, judgment.StartedAtMs, judgment.DurationMs, judgment.Error)
}

// ContinuationJudgments returns an utterance's judgments in order.
func (s *Store) ContinuationJudgments(utteranceID string) ([]ContinuationJudgment, error) {
	rows, err := s.db.Query(`SELECT segment_index, earlier_text, new_text, continues, reason, model, model_role, started_at_ms, duration_ms, error
		FROM continuation_judgments WHERE utterance_id = ? ORDER BY sequence`, utteranceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	judgments := []ContinuationJudgment{}
	for rows.Next() {
		var judgment ContinuationJudgment
		if err := rows.Scan(&judgment.SegmentIndex, &judgment.EarlierText, &judgment.NewText, &judgment.Continues, &judgment.Reason, &judgment.Model,
			&judgment.ModelRole, &judgment.StartedAtMs, &judgment.DurationMs, &judgment.Error); err != nil {
			return nil, err
		}
		judgments = append(judgments, judgment)
	}
	return judgments, rows.Err()
}

// PooledSession is a session started ahead of speech.
type PooledSession struct {
	SessionID        string
	PrincipalID      string
	InstanceID       string
	Effort           string
	Model            string
	WorkingDirectory string
	CreatedAtMs      int64
}

// InsertPooledSession records a session started for the pool.
func (s *Store) InsertPooledSession(session PooledSession) error {
	return s.exec(`INSERT INTO pooled_sessions (session_id, principal_id, instance_id, effort, model, working_directory, created_at_ms) VALUES (?,?,?,?,?,?,?)`,
		session.SessionID, session.PrincipalID, session.InstanceID, session.Effort, session.Model, session.WorkingDirectory, session.CreatedAtMs)
}

// WaitingPooledSessions returns every pooled session neither taken nor
// released, oldest first.
func (s *Store) WaitingPooledSessions() ([]PooledSession, error) {
	rows, err := s.db.Query(`SELECT session_id, principal_id, instance_id, effort, model, working_directory, created_at_ms
		FROM pooled_sessions WHERE taken_at_ms IS NULL AND released_at_ms IS NULL ORDER BY created_at_ms`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var sessions []PooledSession
	for rows.Next() {
		var session PooledSession
		if err := rows.Scan(&session.SessionID, &session.PrincipalID, &session.InstanceID, &session.Effort, &session.Model,
			&session.WorkingDirectory, &session.CreatedAtMs); err != nil {
			return nil, err
		}
		sessions = append(sessions, session)
	}
	return sessions, rows.Err()
}

// TakePooledSession marks a pooled session taken by an utterance.
func (s *Store) TakePooledSession(sessionID, utteranceID string, atMs int64) error {
	return s.exec(`UPDATE pooled_sessions SET taken_at_ms = ?, taken_by_utterance_id = ? WHERE session_id = ? AND taken_at_ms IS NULL`, atMs, utteranceID, sessionID)
}

// ReleasePooledSession marks a pooled session given up, and why.
func (s *Store) ReleasePooledSession(sessionID, reason string, atMs int64) error {
	return s.exec(`UPDATE pooled_sessions SET released_at_ms = ?, release_reason = ? WHERE session_id = ? AND released_at_ms IS NULL`, atMs, reason, sessionID)
}

// LatencySample is one dispatched session with the utterance it served.
type LatencySample struct {
	Strategy string
	Trigger  string
	Pooled   bool
	// Source is the source of the last segment the session's text holds.
	Source                   string
	TranscriptionStartedAtMs *int64
	TranscribedAtMs          *int64
	SpeechEndedAtMs          int64
	SegmentReceivedAtMs      int64
	DispatchStartedAtMs      int64
	DispatchedAtMs           *int64
	FirstBlockAtMs           *int64
	ResultAtMs               *int64
}

// LatencySamples returns every dispatched session, measured from the speech
// end of the last segment its text holds.
func (s *Store) LatencySamples() ([]LatencySample, error) {
	rows, err := s.db.Query(`SELECT u.strategy, u.trigger, us.pooled, us.segment_indexes, us.dispatch_started_at_ms, us.dispatched_at_ms,
		us.first_block_at_ms, us.result_at_ms, us.utterance_id
		FROM utterance_sessions us JOIN utterances u ON u.utterance_id = us.utterance_id`)
	if err != nil {
		return nil, err
	}
	type pending struct {
		sample      LatencySample
		lastIndex   int
		utteranceID string
	}
	var pendings []pending
	for rows.Next() {
		var p pending
		var indexesJSON string
		if err := rows.Scan(&p.sample.Strategy, &p.sample.Trigger, &p.sample.Pooled, &indexesJSON, &p.sample.DispatchStartedAtMs,
			&p.sample.DispatchedAtMs, &p.sample.FirstBlockAtMs, &p.sample.ResultAtMs, &p.utteranceID); err != nil {
			rows.Close()
			return nil, err
		}
		var indexes []int
		if err := json.Unmarshal([]byte(indexesJSON), &indexes); err != nil || len(indexes) == 0 {
			rows.Close()
			return nil, fmt.Errorf("segment_indexes %q of a session of %s: %v", indexesJSON, p.utteranceID, err)
		}
		p.lastIndex = indexes[len(indexes)-1]
		pendings = append(pendings, p)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()
	samples := make([]LatencySample, 0, len(pendings))
	for _, p := range pendings {
		if err := s.db.QueryRow(`SELECT speech_ended_at_ms, received_at_ms, source, transcription_started_at_ms, transcribed_at_ms
			FROM segments WHERE utterance_id = ? AND segment_index = ?`,
			p.utteranceID, p.lastIndex).Scan(&p.sample.SpeechEndedAtMs, &p.sample.SegmentReceivedAtMs, &p.sample.Source,
			&p.sample.TranscriptionStartedAtMs, &p.sample.TranscribedAtMs); err != nil {
			return nil, fmt.Errorf("segment %d of %s: %w", p.lastIndex, p.utteranceID, err)
		}
		samples = append(samples, p.sample)
	}
	return samples, nil
}

func (s *Store) exec(query string, arguments ...any) error {
	_, err := s.db.Exec(query, arguments...)
	if err != nil {
		return fmt.Errorf("%s: %w", strings.Fields(query)[0]+" "+strings.Fields(query)[1], err)
	}
	return nil
}
