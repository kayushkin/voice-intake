package voiceintake

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Replays feed a stored recording through the streaming path in real time,
// or send a stored text, under a named configuration, and record the same
// timings live speech records. They decide settings, so they live here.

// replayDetectors are the detectors a configuration may name.
var replayDetectors = map[string]bool{"silero": true}

// replayDispatches are the ways a configuration may answer.
var replayDispatches = map[string]bool{DispatchSession: true, DispatchOneshot: true, DispatchNone: true}

// replayChunk is how much audio a replay sends at a time, as the phone does.
const replayChunk = 200 * time.Millisecond

// replayLimit bounds one replay from start to done.
const replayLimit = 8 * time.Minute

// replayTrigger is the contract trigger a replay's utterance carries.
const replayTrigger = "app"

// SetReplayRecordingDirectory sets where recordings for replay are kept.
func (d *Dispatcher) SetReplayRecordingDirectory(directory string) {
	d.replayRecordingDirectory = directory
}

func (s *Server) createReplayRecording(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	expectedText := strings.TrimSpace(query.Get("expected_text"))
	if expectedText == "" {
		writeError(w, http.StatusBadRequest, "expected_text is required: what was said")
		return
	}
	wav, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maximumAudioBytes))
	if err != nil {
		writeError(w, http.StatusRequestEntityTooLarge, err.Error())
		return
	}
	durationMs, err := wavDurationMs(wav)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	recording := ReplayRecording{ExpectedText: expectedText, DurationMs: durationMs, Words: []TranscriptWord{}, CreatedAtMs: s.now().UnixMilli()}
	if name := query.Get("ground_truth_transcriber"); name != "" {
		transcriber, err := s.transcribers.Named(name)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		transcript, err := transcriber.Transcribe(r.Context(), wav)
		if err != nil {
			writeError(w, http.StatusBadGateway, err.Error())
			return
		}
		if len(transcript.Words) == 0 {
			writeError(w, http.StatusUnprocessableEntity, fmt.Sprintf("transcriber %s gave no word times (transcript %q)", name, transcript.Text))
			return
		}
		recording.Words = transcript.Words
		recording.TrueSpeechEndMs = transcript.Words[len(transcript.Words)-1].EndMs
		recording.TrueSpeechEndSource = "end of the last word in " + name + "'s word times"
	}
	if raw := query.Get("true_speech_end_ms"); raw != "" {
		given, err := strconv.ParseInt(raw, 10, 64)
		source := strings.TrimSpace(query.Get("true_speech_end_source"))
		if err != nil || given <= 0 || given > durationMs || source == "" {
			writeError(w, http.StatusBadRequest, "true_speech_end_ms must be a time inside the recording, and true_speech_end_source must say how it was found")
			return
		}
		recording.TrueSpeechEndMs, recording.TrueSpeechEndSource = given, source
	}
	if recording.TrueSpeechEndSource == "" {
		writeError(w, http.StatusBadRequest, "name ground_truth_transcriber (one that gives word times) or give true_speech_end_ms with true_speech_end_source")
		return
	}
	directory := s.dispatcher.replayRecordingDirectory
	if err := os.MkdirAll(directory, 0o700); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	stored, err := s.store.InsertReplayRecording(recording, func(recordingID string) string { return recordingID + ".wav" })
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if err := os.WriteFile(filepath.Join(directory, stored.Filename), wav, 0o600); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, stored)
}

func (s *Server) listReplayRecordings(w http.ResponseWriter, r *http.Request) {
	recordings, err := s.store.ReplayRecordings()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, recordings)
}

func (s *Server) createReplayConfiguration(w http.ResponseWriter, r *http.Request) {
	var configuration ReplayConfiguration
	if err := decodeStrict(r, &configuration); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	var problems []string
	if strings.TrimSpace(configuration.Name) == "" {
		problems = append(problems, "name is required")
	}
	if !replayDetectors[configuration.Detector] {
		problems = append(problems, fmt.Sprintf("detector %q is not one of %s", configuration.Detector, sortedKeys(replayDetectors)))
	}
	if configuration.EndpointSilenceMs < 0 {
		problems = append(problems, "endpoint_silence_ms is zero (end only at stop) or more")
	}
	if _, err := s.transcribers.Named(configuration.Transcriber); err != nil {
		problems = append(problems, err.Error())
	}
	if !replayDispatches[configuration.Dispatch] {
		problems = append(problems, fmt.Sprintf("dispatch %q is not one of %s", configuration.Dispatch, sortedKeys(replayDispatches)))
	}
	if configuration.Dispatch != DispatchNone && configuration.InstanceID == "" {
		problems = append(problems, "instance_id is required unless dispatch is none")
	}
	if configuration.Dispatch == DispatchSession && configuration.MaxBudgetUSD <= 0 {
		problems = append(problems, "max_budget_usd is required for dispatch session: every replay session runs with a spend ceiling")
	}
	if configuration.WarmSession && configuration.Dispatch != DispatchSession {
		problems = append(problems, "warm_session is for dispatch session")
	}
	if len(problems) > 0 {
		writeError(w, http.StatusBadRequest, strings.Join(problems, "; "))
		return
	}
	for key, value := range map[string]string{SettingInstanceID: configuration.InstanceID, SettingDefaultModel: configuration.Model} {
		if err := s.validateSetting(r.Context(), key, value); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	configuration.CreatedAtMs = s.now().UnixMilli()
	stored, err := s.store.InsertReplayConfiguration(configuration)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, stored)
}

func (s *Server) listReplayConfigurations(w http.ResponseWriter, r *http.Request) {
	configurations, err := s.store.ReplayConfigurations()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, configurations)
}

func (s *Server) createReplay(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ConfigurationID string `json:"configuration_id"`
		RecordingID     string `json:"recording_id"`
		Text            string `json:"text"`
	}
	if err := decodeStrict(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if (body.RecordingID == "") == (strings.TrimSpace(body.Text) == "") {
		writeError(w, http.StatusBadRequest, "give recording_id or text, one of the two")
		return
	}
	configuration, err := s.store.ReplayConfigurationByID(body.ConfigurationID)
	if errors.Is(err, ErrNotFound) {
		writeError(w, http.StatusNotFound, "no configuration "+body.ConfigurationID)
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	var recording *ReplayRecording
	if body.RecordingID != "" {
		found, err := s.store.ReplayRecordingByID(body.RecordingID)
		if errors.Is(err, ErrNotFound) {
			writeError(w, http.StatusNotFound, "no recording "+body.RecordingID)
			return
		}
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		recording = &found
	}
	replay, err := s.store.InsertReplay(Replay{ConfigurationID: configuration.ConfigurationID, RecordingID: body.RecordingID,
		Text: strings.TrimSpace(body.Text), StartedAtMs: s.now().UnixMilli()})
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	streamSettings := StreamSettingsFrom(s.registry)
	go s.dispatcher.runReplay(principalOf(r), replay, configuration, recording, streamSettings)
	writeJSON(w, http.StatusAccepted, replay)
}

// ReplayRecord is a replay with what its utterance did.
type ReplayRecord struct {
	Replay
	ConfigurationName string           `json:"configuration_name"`
	Transcript        string           `json:"transcript"`
	WordAccuracy      *float64         `json:"word_accuracy"`
	Utterance         *UtteranceRecord `json:"utterance"`
}

func (s *Server) replayRecord(replay Replay) (ReplayRecord, error) {
	record := ReplayRecord{Replay: replay}
	configuration, err := s.store.ReplayConfigurationByID(replay.ConfigurationID)
	if err != nil {
		return record, err
	}
	record.ConfigurationName = configuration.Name
	if replay.UtteranceID == "" {
		return record, nil
	}
	utterance, err := s.store.GetUtterance(replay.UtteranceID)
	if err != nil {
		return record, err
	}
	full := UtteranceRecord{Utterance: utterance}
	if full.Segments, err = s.store.Segments(utterance.UtteranceID); err != nil {
		return record, err
	}
	if full.Sessions, err = s.store.UtteranceSessions(utterance.UtteranceID); err != nil {
		return record, err
	}
	if full.Decisions, err = s.store.Decisions(utterance.UtteranceID); err != nil {
		return record, err
	}
	if full.ContinuationJudgments, err = s.store.ContinuationJudgments(utterance.UtteranceID); err != nil {
		return record, err
	}
	record.Utterance = &full
	record.Transcript = spokenTranscript(full.Segments)
	if replay.RecordingID != "" {
		recording, err := s.store.ReplayRecordingByID(replay.RecordingID)
		if err != nil {
			return record, err
		}
		accuracy := WordAccuracy(recording.ExpectedText, record.Transcript)
		record.WordAccuracy = &accuracy
	}
	return record, nil
}

func (s *Server) listReplays(w http.ResponseWriter, r *http.Request) {
	replays, err := s.store.Replays(r.URL.Query().Get("configuration_id"))
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	records := make([]ReplayRecord, 0, len(replays))
	for _, replay := range replays {
		record, err := s.replayRecord(replay)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		records = append(records, record)
	}
	writeJSON(w, http.StatusOK, records)
}

func (s *Server) getReplay(w http.ResponseWriter, r *http.Request) {
	replay, err := s.store.ReplayByID(r.PathValue("replay_id"))
	if errors.Is(err, ErrNotFound) {
		writeError(w, http.StatusNotFound, "no replay "+r.PathValue("replay_id"))
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	record, err := s.replayRecord(replay)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, record)
}

// spokenTranscript joins the segments that had words, in index order.
func spokenTranscript(segments []Segment) string {
	var texts []string
	for _, segment := range segments {
		if hasWords(segment) {
			texts = append(texts, segment.Text)
		}
	}
	return strings.Join(texts, " ")
}

// runReplay runs one replay to its done event and records how it ended.
func (d *Dispatcher) runReplay(principalID string, replay Replay, configuration ReplayConfiguration, recording *ReplayRecording, streamSettings StreamSettings) {
	failure := ""
	if err := d.replay(principalID, replay, configuration, recording, streamSettings); err != nil {
		failure = err.Error()
		log.Printf("[replay] ERROR %s: %v", replay.ReplayID, err)
	}
	if err := d.store.FinishReplay(replay.ReplayID, d.now().UnixMilli(), failure); err != nil {
		log.Printf("[replay] ERROR %s: %v", replay.ReplayID, err)
	}
}

func (d *Dispatcher) replay(principalID string, replay Replay, configuration ReplayConfiguration, recording *ReplayRecording, streamSettings StreamSettings) error {
	ctx, cancel := context.WithTimeout(context.Background(), replayLimit)
	defer cancel()
	var pcm []byte
	if recording != nil {
		wav, err := os.ReadFile(filepath.Join(d.replayRecordingDirectory, recording.Filename))
		if err != nil {
			return err
		}
		if pcm, err = wavPCM(wav); err != nil {
			return err
		}
	}
	overrides := &dispatchOverrides{replayID: replay.ReplayID, mode: configuration.Dispatch, instanceID: configuration.InstanceID,
		model: configuration.Model, effort: configuration.Effort, workingDirectory: configuration.WorkingDirectory, maxBudgetUSD: configuration.MaxBudgetUSD}
	if configuration.WarmSession {
		sessionID, err := d.startWarmSession(ctx, principalID, configuration)
		if err != nil {
			return fmt.Errorf("start the warm session: %w", err)
		}
		overrides.warmSessionID = sessionID
		defer d.releaseUnusedWarmSession(overrides, sessionID)
	}
	nowMs := d.now().UnixMilli()
	utterance, _, err := d.store.CreateUtterance(Utterance{ClientUtteranceID: replay.ReplayID, PrincipalID: principalID, Mode: "quick",
		Strategy: "endpoint", Trigger: replayTrigger, SpeechStartedAtClientMs: nowMs, SpeechStartedAtMs: nowMs, ReceivedAtMs: nowMs,
		AudioStreaming: recording != nil, AudioFormat: map[bool]string{true: StreamAudioFormat}[recording != nil], ReplayID: replay.ReplayID})
	if err != nil {
		return err
	}
	if err := d.store.SetReplayUtterance(replay.ReplayID, utterance.UtteranceID); err != nil {
		return err
	}
	run, err := d.runFor(utterance)
	if err != nil {
		return err
	}
	run.mutex.Lock()
	run.overrides = overrides
	run.mutex.Unlock()

	if recording == nil {
		segment := Segment{UtteranceID: utterance.UtteranceID, Index: 0, Text: replay.Text, SpeechEndedAtClientMs: nowMs, SpeechEndedAtMs: nowMs,
			ReceivedAtMs: nowMs, Source: SegmentSourceText}
		if _, err := d.store.InsertSegment(segment); err != nil {
			return err
		}
		if err := d.SegmentArrived(utterance, segment); err != nil {
			return err
		}
		if err := d.Finish(utterance); err != nil {
			return err
		}
	} else {
		options := StreamOptions{Silence: time.Duration(configuration.EndpointSilenceMs) * time.Millisecond, NoSpeechTimeout: streamSettings.NoSpeechTimeout,
			MaximumRecording: streamSettings.MaximumRecording, ContinuationWindow: streamSettings.ContinuationWindow,
			TranscriberName: configuration.Transcriber, Replay: true}
		if err := d.StartStream(utterance, options); err != nil {
			return err
		}
		if err := d.feedReplayAudio(ctx, utterance, run, pcm); err != nil {
			return err
		}
	}
	select {
	case <-run.done():
		return nil
	case <-ctx.Done():
		run.cancel()
		return fmt.Errorf("the replay did not finish within %s", replayLimit)
	}
}

// feedReplayAudio sends the recording in phone-sized chunks at the pace it
// was spoken, and stops as the phone would: when told to, or when the
// recording ends, which is when the person pressed stop.
func (d *Dispatcher) feedReplayAudio(ctx context.Context, utterance Utterance, run *utteranceRun, pcm []byte) error {
	run.mutex.Lock()
	stream := run.stream
	run.mutex.Unlock()
	chunkBytes := int(replayChunk.Milliseconds()) * streamBytesPerMillisecond
	started := time.Now()
	reason := StopReasonUser
	for offset := 0; offset < len(pcm); offset += chunkBytes {
		// Send each chunk once its audio would have been recorded.
		wait := time.Duration(offset+chunkBytes)*time.Millisecond/time.Duration(streamBytesPerMillisecond) - time.Since(started)
		select {
		case <-stream.stopRecording:
		case <-time.After(wait):
		case <-ctx.Done():
			return ctx.Err()
		}
		if stopRequested(stream) {
			reason = utteranceStopReason(d, utterance)
			break
		}
		end := min(len(pcm), offset+chunkBytes)
		if _, err := d.AppendStreamAudio(ctx, utterance, int64(offset), pcm[offset:end]); err != nil {
			return err
		}
	}
	nowMs := d.now().UnixMilli()
	return d.StopStream(utterance, nowMs, nowMs, reason)
}

func stopRequested(stream *audioStream) bool {
	select {
	case <-stream.stopRecording:
		return true
	default:
		return false
	}
}

// utteranceStopReason is the stop_recording reason the stream sent, which
// the phone posts back.
func utteranceStopReason(d *Dispatcher, utterance Utterance) string {
	stored, err := d.store.GetUtterance(utterance.UtteranceID)
	if err != nil || stored.StopRecordingReason == "" {
		return StopReasonUser
	}
	return stored.StopRecordingReason
}

// warmSessionSettleTime is how long a warm session is given to start
// before the replay begins. A claude_code session reads "starting" until its
// first event, and it sends none until a message comes, so its state cannot
// say it is ready; a pooled session has usually waited far longer than this.
var warmSessionSettleTime = 15 * time.Second

// startWarmSession starts a session before the replay and gives it time to
// start, as a pooled session would have had.
func (d *Dispatcher) startWarmSession(ctx context.Context, principalID string, configuration ReplayConfiguration) (string, error) {
	sessionID, err := d.bridge.CreateSession(ctx, SessionCreation{PrincipalID: principalID, InstanceID: configuration.InstanceID, Effort: configuration.Effort,
		Model: configuration.Model, WorkingDirectory: configuration.WorkingDirectory, DisplayName: "Voice replay: ready", MaxBudgetUSD: configuration.MaxBudgetUSD})
	if err != nil {
		return "", err
	}
	deadline := time.Now().Add(warmSessionSettleTime)
	for time.Now().Before(deadline) {
		state, err := d.bridge.SessionState(ctx, sessionID)
		if err == nil && sessionStatesThatCannotTakeAMessage[state] {
			d.stopAndArchive(sessionID, "it was "+state+" before the replay began")
			return "", fmt.Errorf("session %s was %s before the replay began", sessionID, state)
		}
		if err == nil && state != "starting" && state != "" {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	return sessionID, nil
}

func (d *Dispatcher) releaseUnusedWarmSession(overrides *dispatchOverrides, sessionID string) {
	if overrides.warmSessionID == sessionID {
		d.stopAndArchive(sessionID, "the replay never used it")
	}
}

func (d *Dispatcher) stopAndArchive(sessionID, reason string) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := d.bridge.StopSession(ctx, sessionID); err != nil {
		log.Printf("[replay] ERROR stop %s (%s): %v", sessionID, reason, err)
	}
	if err := d.bridge.ArchiveSession(ctx, sessionID); err != nil {
		log.Printf("[replay] ERROR archive %s (%s): %v", sessionID, reason, err)
	}
}

// Word accuracy.

var (
	nonWordCharacters = regexp.MustCompile(`[^a-z0-9' ]+`)
	numberWords       = map[string]int{"zero": 0, "oh": 0, "one": 1, "two": 2, "three": 3, "four": 4, "five": 5, "six": 6, "seven": 7, "eight": 8,
		"nine": 9, "ten": 10, "eleven": 11, "twelve": 12, "thirteen": 13, "fourteen": 14, "fifteen": 15, "sixteen": 16, "seventeen": 17,
		"eighteen": 18, "nineteen": 19}
	tensWords = map[string]int{"twenty": 20, "thirty": 30, "forty": 40, "fifty": 50, "sixty": 60, "seventy": 70, "eighty": 80, "ninety": 90}
)

// normalizedWords lowercases, drops punctuation, writes spoken numbers as
// digits ("thirty eight" → 38) and joins runs of digits, so "Testing 123."
// and "testing one, two, three" are the same two words.
func normalizedWords(text string) []string {
	lowered := strings.ToLower(strings.ReplaceAll(text, "-", " "))
	fields := strings.Fields(nonWordCharacters.ReplaceAllString(lowered, " "))
	var digits []string
	for index := 0; index < len(fields); index++ {
		field := strings.Trim(fields[index], "'")
		if tens, ok := tensWords[field]; ok {
			if index+1 < len(fields) {
				if unit, ok := numberWords[fields[index+1]]; ok && unit > 0 && unit < 10 {
					digits = append(digits, strconv.Itoa(tens+unit))
					index++
					continue
				}
			}
			digits = append(digits, strconv.Itoa(tens))
			continue
		}
		if value, ok := numberWords[field]; ok && field != "oh" {
			digits = append(digits, strconv.Itoa(value))
			continue
		}
		if field != "" {
			digits = append(digits, field)
		}
	}
	var words []string
	for _, word := range digits {
		if isDigits(word) && len(words) > 0 && isDigits(words[len(words)-1]) {
			words[len(words)-1] += word
			continue
		}
		words = append(words, word)
	}
	return words
}

func isDigits(word string) bool {
	for _, character := range word {
		if character < '0' || character > '9' {
			return false
		}
	}
	return word != ""
}

// WordAccuracy is one minus the word error rate of actual against expected,
// at least zero.
func WordAccuracy(expected, actual string) float64 {
	want, got := normalizedWords(expected), normalizedWords(actual)
	if len(want) == 0 {
		if len(got) == 0 {
			return 1
		}
		return 0
	}
	previous := make([]int, len(got)+1)
	current := make([]int, len(got)+1)
	for column := range previous {
		previous[column] = column
	}
	for row := 1; row <= len(want); row++ {
		current[0] = row
		for column := 1; column <= len(got); column++ {
			substitution := previous[column-1]
			if want[row-1] != got[column-1] {
				substitution++
			}
			current[column] = min(substitution, previous[column]+1, current[column-1]+1)
		}
		previous, current = current, previous
	}
	return max(0, 1-float64(previous[len(got)])/float64(len(want)))
}

// Latency by configuration.

// ReplayLatencyGroup is one configuration's replays, medians and spend.
type ReplayLatencyGroup struct {
	ConfigurationID   string            `json:"configuration_id"`
	ConfigurationName string            `json:"configuration_name"`
	Replays           int               `json:"replays"`
	Failed            int               `json:"failed"`
	MedianMs          map[string]*int64 `json:"median_ms"`
	Samples           map[string]int    `json:"samples"`
	MeanWordAccuracy  *float64          `json:"mean_word_accuracy"`
	SpendUSD          float64           `json:"spend_usd"`
}

// replayMeasureNames are the intervals reported per configuration, in order.
// "speech end" is the recording's true speech end for a recording, and the
// moment the text was sent for a text.
var replayMeasureNames = []string{"endpoint_delay", "detected_speech_end_error", "transcription", "speech_end_to_transcribed",
	"dispatched_to_first_block", "dispatched_to_result", "speech_end_to_first_block", "speech_end_to_result"}

// ReplayLatencyGroups measures every finished replay and groups by
// configuration.
func (s *Store) ReplayLatencyGroups() ([]ReplayLatencyGroup, error) {
	configurations, err := s.ReplayConfigurations()
	if err != nil {
		return nil, err
	}
	replays, err := s.Replays("")
	if err != nil {
		return nil, err
	}
	recordings := map[string]ReplayRecording{}
	allRecordings, err := s.ReplayRecordings()
	if err != nil {
		return nil, err
	}
	for _, recording := range allRecordings {
		recordings[recording.RecordingID] = recording
	}
	type collected struct {
		group      ReplayLatencyGroup
		values     map[string][]int64
		accuracies []float64
	}
	byConfiguration := map[string]*collected{}
	for _, configuration := range configurations {
		byConfiguration[configuration.ConfigurationID] = &collected{group: ReplayLatencyGroup{ConfigurationID: configuration.ConfigurationID,
			ConfigurationName: configuration.Name, MedianMs: map[string]*int64{}, Samples: map[string]int{}}, values: map[string][]int64{}}
	}
	for _, replay := range replays {
		entry := byConfiguration[replay.ConfigurationID]
		if entry == nil || replay.FinishedAtMs == nil {
			continue
		}
		entry.group.Replays++
		if replay.Error != "" {
			entry.group.Failed++
		}
		measures, accuracy, spend, err := s.replayMeasures(replay, recordings)
		if err != nil {
			return nil, err
		}
		for name, value := range measures {
			entry.values[name] = append(entry.values[name], value)
		}
		if accuracy != nil {
			entry.accuracies = append(entry.accuracies, *accuracy)
		}
		entry.group.SpendUSD += spend
	}
	groups := make([]ReplayLatencyGroup, 0, len(byConfiguration))
	for _, entry := range byConfiguration {
		if entry.group.Replays == 0 {
			continue
		}
		for _, name := range replayMeasureNames {
			entry.group.Samples[name] = len(entry.values[name])
			entry.group.MedianMs[name] = median(entry.values[name])
		}
		if len(entry.accuracies) > 0 {
			total := 0.0
			for _, accuracy := range entry.accuracies {
				total += accuracy
			}
			mean := total / float64(len(entry.accuracies))
			entry.group.MeanWordAccuracy = &mean
		}
		groups = append(groups, entry.group)
	}
	sort.Slice(groups, func(i, j int) bool { return groups[i].ConfigurationID < groups[j].ConfigurationID })
	return groups, nil
}

// replayMeasures reads one replay's intervals, its word accuracy (for a
// recording) and what its sessions cost.
func (s *Store) replayMeasures(replay Replay, recordings map[string]ReplayRecording) (map[string]int64, *float64, float64, error) {
	measures := map[string]int64{}
	if replay.UtteranceID == "" {
		return measures, nil, 0, nil
	}
	utterance, err := s.GetUtterance(replay.UtteranceID)
	if err != nil {
		return nil, nil, 0, err
	}
	segments, err := s.Segments(replay.UtteranceID)
	if err != nil {
		return nil, nil, 0, err
	}
	sessions, err := s.UtteranceSessions(replay.UtteranceID)
	if err != nil {
		return nil, nil, 0, err
	}
	spend := 0.0
	for _, session := range sessions {
		if session.TurnCostUSD != nil {
			spend += *session.TurnCostUSD
		}
	}
	var last *Segment
	for index := range segments {
		if hasWords(segments[index]) || last == nil {
			last = &segments[index]
		}
	}
	var accuracy *float64
	recording, isRecording := recordings[replay.RecordingID]
	if isRecording {
		value := WordAccuracy(recording.ExpectedText, spokenTranscript(segments))
		accuracy = &value
	}
	if last == nil {
		return measures, accuracy, spend, nil
	}
	speechEnd := last.SpeechEndedAtMs
	if isRecording {
		speechEnd = utterance.SpeechStartedAtMs + recording.TrueSpeechEndMs
		if last.EndpointedAtMs != nil {
			measures["endpoint_delay"] = *last.EndpointedAtMs - speechEnd
		}
		if last.DetectedSpeechEndOffsetMs != nil {
			measures["detected_speech_end_error"] = utterance.SpeechStartedAtMs + *last.DetectedSpeechEndOffsetMs - speechEnd
		}
	}
	if last.TranscriptionStartedAtMs != nil && last.TranscribedAtMs != nil {
		measures["transcription"] = *last.TranscribedAtMs - *last.TranscriptionStartedAtMs
		measures["speech_end_to_transcribed"] = *last.TranscribedAtMs - speechEnd
	}
	if replay.OneshotStartedAtMs != nil && replay.OneshotAnsweredAtMs != nil {
		measures["dispatched_to_result"] = *replay.OneshotAnsweredAtMs - *replay.OneshotStartedAtMs
		measures["speech_end_to_result"] = *replay.OneshotAnsweredAtMs - speechEnd
	}
	var answering *UtteranceSession
	for index := range sessions {
		session := &sessions[index]
		holdsLast := len(session.SegmentIndexes) > 0 && session.SegmentIndexes[len(session.SegmentIndexes)-1] == last.Index
		if holdsLast && session.Role != RoleSuperseded && (answering == nil || session.Role == RoleKept) {
			answering = session
		}
	}
	if answering != nil && answering.DispatchedAtMs != nil {
		if answering.FirstBlockAtMs != nil {
			measures["dispatched_to_first_block"] = *answering.FirstBlockAtMs - *answering.DispatchedAtMs
			measures["speech_end_to_first_block"] = *answering.FirstBlockAtMs - speechEnd
		}
		if answering.ResultAtMs != nil {
			measures["dispatched_to_result"] = *answering.ResultAtMs - *answering.DispatchedAtMs
			measures["speech_end_to_result"] = *answering.ResultAtMs - speechEnd
		}
	}
	return measures, accuracy, spend, nil
}
