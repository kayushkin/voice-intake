package voiceintake

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
)

// segmentSources are the sources a segment may have.
var segmentSources = map[string]bool{SegmentSourceText: true, SegmentSourceAudio: true, SegmentSourceStream: true}

// SetStreamReceivedBytes records how much streamed audio has arrived.
func (s *Store) SetStreamReceivedBytes(utteranceID string, total int64) error {
	return s.exec(`UPDATE utterances SET stream_received_bytes = ? WHERE utterance_id = ?`, total, utteranceID)
}

// MarkStopRecordingSent records that the phone was told to stop, and why.
func (s *Store) MarkStopRecordingSent(utteranceID, reason string, atMs int64) error {
	return s.exec(`UPDATE utterances SET stop_recording_reason = ?, stop_recording_sent_at_ms = ? WHERE utterance_id = ? AND stop_recording_sent_at_ms IS NULL`,
		reason, atMs, utteranceID)
}

// MarkUtteranceStopped records that the microphone stopped.
func (s *Store) MarkUtteranceStopped(utteranceID string, stoppedAtClientMs, stoppedAtMs int64, reason string) error {
	return s.exec(`UPDATE utterances SET stopped_at_client_ms = ?, stopped_at_ms = ?, stop_reason = ? WHERE utterance_id = ? AND stopped_at_ms IS NULL`,
		stoppedAtClientMs, stoppedAtMs, reason, utteranceID)
}

// ReplayRecording is a recording kept for replay.
type ReplayRecording struct {
	RecordingID         string           `json:"recording_id"`
	Filename            string           `json:"filename"`
	ExpectedText        string           `json:"expected_text"`
	DurationMs          int64            `json:"duration_ms"`
	TrueSpeechEndMs     int64            `json:"true_speech_end_ms"`
	TrueSpeechEndSource string           `json:"true_speech_end_source"`
	Words               []TranscriptWord `json:"words"`
	CreatedAtMs         int64            `json:"created_at_ms"`
}

// InsertReplayRecording stores a recording's row and gives it its id; the
// caller names the file after the id it is handed.
func (s *Store) InsertReplayRecording(recording ReplayRecording, filenameFor func(recordingID string) string) (ReplayRecording, error) {
	words, err := json.Marshal(recording.Words)
	if err != nil {
		return ReplayRecording{}, err
	}
	transaction, err := s.db.Begin()
	if err != nil {
		return ReplayRecording{}, err
	}
	defer transaction.Rollback()
	var next int64
	if err := transaction.QueryRow(`SELECT COALESCE(MAX(sequence), 0) + 1 FROM replay_recordings`).Scan(&next); err != nil {
		return ReplayRecording{}, err
	}
	recording.RecordingID = fmt.Sprintf("recording_%06d", next)
	recording.Filename = filenameFor(recording.RecordingID)
	if _, err := transaction.Exec(`INSERT INTO replay_recordings (sequence, recording_id, filename, expected_text, duration_ms, true_speech_end_ms,
		true_speech_end_source, words, created_at_ms) VALUES (?,?,?,?,?,?,?,?,?)`,
		next, recording.RecordingID, recording.Filename, recording.ExpectedText, recording.DurationMs, recording.TrueSpeechEndMs,
		recording.TrueSpeechEndSource, string(words), recording.CreatedAtMs); err != nil {
		return ReplayRecording{}, err
	}
	return recording, transaction.Commit()
}

const replayRecordingColumns = `recording_id, filename, expected_text, duration_ms, true_speech_end_ms, true_speech_end_source, words, created_at_ms`

func scanReplayRecording(row rowScanner) (ReplayRecording, error) {
	var recording ReplayRecording
	var words string
	err := row.Scan(&recording.RecordingID, &recording.Filename, &recording.ExpectedText, &recording.DurationMs, &recording.TrueSpeechEndMs,
		&recording.TrueSpeechEndSource, &words, &recording.CreatedAtMs)
	if errors.Is(err, sql.ErrNoRows) {
		return ReplayRecording{}, ErrNotFound
	}
	if err != nil {
		return ReplayRecording{}, err
	}
	if err := json.Unmarshal([]byte(words), &recording.Words); err != nil {
		return ReplayRecording{}, fmt.Errorf("recording %s words: %w", recording.RecordingID, err)
	}
	return recording, nil
}

// ReplayRecordingByID reads one recording.
func (s *Store) ReplayRecordingByID(recordingID string) (ReplayRecording, error) {
	return scanReplayRecording(s.db.QueryRow(`SELECT `+replayRecordingColumns+` FROM replay_recordings WHERE recording_id = ?`, recordingID))
}

// ReplayRecordings lists every recording, oldest first.
func (s *Store) ReplayRecordings() ([]ReplayRecording, error) {
	rows, err := s.db.Query(`SELECT ` + replayRecordingColumns + ` FROM replay_recordings ORDER BY sequence`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	recordings := []ReplayRecording{}
	for rows.Next() {
		recording, err := scanReplayRecording(rows)
		if err != nil {
			return nil, err
		}
		recordings = append(recordings, recording)
	}
	return recordings, rows.Err()
}

// ReplayConfiguration is a named way to run a replay.
type ReplayConfiguration struct {
	ConfigurationID   string  `json:"configuration_id"`
	Name              string  `json:"name"`
	Detector          string  `json:"detector"`
	EndpointSilenceMs int64   `json:"endpoint_silence_ms"`
	Transcriber       string  `json:"transcriber"`
	Dispatch          string  `json:"dispatch"`
	InstanceID        string  `json:"instance_id"`
	Model             string  `json:"model"`
	Effort            string  `json:"effort"`
	WorkingDirectory  string  `json:"working_directory"`
	PromptVariant     string  `json:"prompt_variant"`
	WarmSession       bool    `json:"warm_session"`
	MaxBudgetUSD      float64 `json:"max_budget_usd"`
	CreatedAtMs       int64   `json:"created_at_ms"`
	// HighPassCutoffHertz and MinimumSegmentMeanSpeechProbability are the
	// live settings of the same names; zero turns each off.
	HighPassCutoffHertz                 int     `json:"high_pass_cutoff_hz"`
	MinimumSegmentMeanSpeechProbability float64 `json:"minimum_segment_mean_speech_probability"`
}

// InsertReplayConfiguration stores a configuration and gives it its id.
func (s *Store) InsertReplayConfiguration(configuration ReplayConfiguration) (ReplayConfiguration, error) {
	transaction, err := s.db.Begin()
	if err != nil {
		return ReplayConfiguration{}, err
	}
	defer transaction.Rollback()
	var next int64
	if err := transaction.QueryRow(`SELECT COALESCE(MAX(sequence), 0) + 1 FROM replay_configurations`).Scan(&next); err != nil {
		return ReplayConfiguration{}, err
	}
	configuration.ConfigurationID = fmt.Sprintf("configuration_%06d", next)
	if _, err := transaction.Exec(`INSERT INTO replay_configurations (sequence, configuration_id, name, detector, endpoint_silence_ms, transcriber, dispatch,
		instance_id, model, effort, working_directory, prompt_variant, warm_session, max_budget_usd, created_at_ms,
		high_pass_cutoff_hz, minimum_segment_mean_speech_probability) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		next, configuration.ConfigurationID, configuration.Name, configuration.Detector, configuration.EndpointSilenceMs, configuration.Transcriber,
		configuration.Dispatch, configuration.InstanceID, configuration.Model, configuration.Effort, configuration.WorkingDirectory,
		configuration.PromptVariant, configuration.WarmSession, configuration.MaxBudgetUSD, configuration.CreatedAtMs,
		configuration.HighPassCutoffHertz, configuration.MinimumSegmentMeanSpeechProbability); err != nil {
		return ReplayConfiguration{}, err
	}
	return configuration, transaction.Commit()
}

const replayConfigurationColumns = `configuration_id, name, detector, endpoint_silence_ms, transcriber, dispatch, instance_id, model, effort,
	working_directory, prompt_variant, warm_session, max_budget_usd, created_at_ms, high_pass_cutoff_hz, minimum_segment_mean_speech_probability`

func scanReplayConfiguration(row rowScanner) (ReplayConfiguration, error) {
	var configuration ReplayConfiguration
	err := row.Scan(&configuration.ConfigurationID, &configuration.Name, &configuration.Detector, &configuration.EndpointSilenceMs,
		&configuration.Transcriber, &configuration.Dispatch, &configuration.InstanceID, &configuration.Model, &configuration.Effort,
		&configuration.WorkingDirectory, &configuration.PromptVariant, &configuration.WarmSession, &configuration.MaxBudgetUSD, &configuration.CreatedAtMs,
		&configuration.HighPassCutoffHertz, &configuration.MinimumSegmentMeanSpeechProbability)
	if errors.Is(err, sql.ErrNoRows) {
		return ReplayConfiguration{}, ErrNotFound
	}
	return configuration, err
}

// ReplayConfigurationByID reads one configuration.
func (s *Store) ReplayConfigurationByID(configurationID string) (ReplayConfiguration, error) {
	return scanReplayConfiguration(s.db.QueryRow(`SELECT `+replayConfigurationColumns+` FROM replay_configurations WHERE configuration_id = ?`, configurationID))
}

// ReplayConfigurations lists every configuration, oldest first.
func (s *Store) ReplayConfigurations() ([]ReplayConfiguration, error) {
	rows, err := s.db.Query(`SELECT ` + replayConfigurationColumns + ` FROM replay_configurations ORDER BY sequence`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	configurations := []ReplayConfiguration{}
	for rows.Next() {
		configuration, err := scanReplayConfiguration(rows)
		if err != nil {
			return nil, err
		}
		configurations = append(configurations, configuration)
	}
	return configurations, rows.Err()
}

// Replay is one run of a recording or a text under a configuration.
type Replay struct {
	ReplayID            string `json:"replay_id"`
	ConfigurationID     string `json:"configuration_id"`
	RecordingID         string `json:"recording_id"`
	Text                string `json:"text"`
	UtteranceID         string `json:"utterance_id"`
	StartedAtMs         int64  `json:"started_at_ms"`
	FinishedAtMs        *int64 `json:"finished_at_ms"`
	OneshotStartedAtMs  *int64 `json:"oneshot_started_at_ms"`
	OneshotAnsweredAtMs *int64 `json:"oneshot_answered_at_ms"`
	OneshotModel        string `json:"oneshot_model"`
	OneshotText         string `json:"oneshot_text"`
	Error               string `json:"error"`
}

// InsertReplay stores a replay and gives it its id.
func (s *Store) InsertReplay(replay Replay) (Replay, error) {
	transaction, err := s.db.Begin()
	if err != nil {
		return Replay{}, err
	}
	defer transaction.Rollback()
	var next int64
	if err := transaction.QueryRow(`SELECT COALESCE(MAX(sequence), 0) + 1 FROM replays`).Scan(&next); err != nil {
		return Replay{}, err
	}
	replay.ReplayID = fmt.Sprintf("replay_%06d", next)
	if _, err := transaction.Exec(`INSERT INTO replays (sequence, replay_id, configuration_id, recording_id, text, started_at_ms) VALUES (?,?,?,?,?,?)`,
		next, replay.ReplayID, replay.ConfigurationID, replay.RecordingID, replay.Text, replay.StartedAtMs); err != nil {
		return Replay{}, err
	}
	return replay, transaction.Commit()
}

// SetReplayUtterance records the utterance a replay made.
func (s *Store) SetReplayUtterance(replayID, utteranceID string) error {
	return s.exec(`UPDATE replays SET utterance_id = ? WHERE replay_id = ?`, utteranceID, replayID)
}

// SetReplayOneshot records a oneshot replay's call.
func (s *Store) SetReplayOneshot(replayID string, startedAtMs, answeredAtMs int64, model, text string) error {
	return s.exec(`UPDATE replays SET oneshot_started_at_ms = ?, oneshot_answered_at_ms = ?, oneshot_model = ?, oneshot_text = ? WHERE replay_id = ?`,
		startedAtMs, answeredAtMs, model, text, replayID)
}

// FinishReplay records the end of a replay, and its failure if it failed.
func (s *Store) FinishReplay(replayID string, atMs int64, failure string) error {
	return s.exec(`UPDATE replays SET finished_at_ms = ?, error = ? WHERE replay_id = ?`, atMs, failure, replayID)
}

const replayColumns = `replay_id, configuration_id, recording_id, text, utterance_id, started_at_ms, finished_at_ms,
	oneshot_started_at_ms, oneshot_answered_at_ms, oneshot_model, oneshot_text, error`

func scanReplay(row rowScanner) (Replay, error) {
	var replay Replay
	err := row.Scan(&replay.ReplayID, &replay.ConfigurationID, &replay.RecordingID, &replay.Text, &replay.UtteranceID, &replay.StartedAtMs,
		&replay.FinishedAtMs, &replay.OneshotStartedAtMs, &replay.OneshotAnsweredAtMs, &replay.OneshotModel, &replay.OneshotText, &replay.Error)
	if errors.Is(err, sql.ErrNoRows) {
		return Replay{}, ErrNotFound
	}
	return replay, err
}

// ReplayByID reads one replay.
func (s *Store) ReplayByID(replayID string) (Replay, error) {
	return scanReplay(s.db.QueryRow(`SELECT `+replayColumns+` FROM replays WHERE replay_id = ?`, replayID))
}

// Replays lists replays, oldest first, of one configuration when
// configurationID is not empty.
func (s *Store) Replays(configurationID string) ([]Replay, error) {
	rows, err := s.db.Query(`SELECT `+replayColumns+` FROM replays WHERE ? = '' OR configuration_id = ? ORDER BY sequence`, configurationID, configurationID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	replays := []Replay{}
	for rows.Next() {
		replay, err := scanReplay(rows)
		if err != nil {
			return nil, err
		}
		replays = append(replays, replay)
	}
	return replays, rows.Err()
}
