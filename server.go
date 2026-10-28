package voiceintake

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/kayushkin/llm-bridge/servicesettings"
)

// The contract's enumerations. Each is the whole list the phone may send.
var (
	utteranceModes      = map[string]bool{"quick": true, "long": true}
	utteranceStrategies = map[string]bool{"endpoint": true, "eager": true, "manual": true}
	utteranceTriggers   = map[string]bool{"tile": true, "widget": true, "side_key": true, "assistant": true, "accessibility": true,
		"shortcut": true, "app": true, "notification": true}
	audioContentTypes = map[string]bool{"audio/wav": true, "audio/ogg": true, "audio/flac": true}
)

// followLimit is how long GET …/events stays open at most.
const followLimit = 5 * time.Minute

// maximumAudioBytes bounds an uploaded recording.
const maximumAudioBytes = 64 << 20

// Server is the HTTP API.
type Server struct {
	store      *Store
	dispatcher *Dispatcher
	pool       *SessionPool
	registry   *servicesettings.Registry
	now        func() time.Time
	// transcribers are the transcription services by name; the transcriber
	// setting picks the one live audio uses.
	transcribers Transcribers
	// settingValidators check a new value with the service that owns what
	// it names.
	settingValidators map[string]func(ctx context.Context, value string) error
	// appReleaseDirectory holds the published APKs of the phone app.
	appReleaseDirectory string
	// appReleasePublishToken is what the admin route demands to publish one.
	appReleasePublishToken string
}

// NewServer builds the API.
func NewServer(store *Store, dispatcher *Dispatcher, pool *SessionPool, registry *servicesettings.Registry, transcribers Transcribers, now func() time.Time) *Server {
	return &Server{store: store, dispatcher: dispatcher, pool: pool, registry: registry, transcribers: transcribers, now: now,
		settingValidators: map[string]func(ctx context.Context, value string) error{}}
}

// Handler is every route.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	RegisterSettingsHandler(mux, s.registry)
	voice := http.NewServeMux()
	voice.HandleFunc("POST /api/voice/utterances", s.createUtterance)
	voice.HandleFunc("GET /api/voice/utterances", s.listUtterances)
	voice.HandleFunc("GET /api/voice/utterances/{id}", s.getUtterance)
	voice.HandleFunc("POST /api/voice/utterances/{id}/segments", s.postSegment)
	voice.HandleFunc("POST /api/voice/utterances/{id}/segments/{index}/audio", s.postSegmentAudio)
	voice.HandleFunc("POST /api/voice/utterances/{id}/finish", s.finishUtterance)
	voice.HandleFunc("POST /api/voice/utterances/{id}/cancel", s.cancelUtterance)
	voice.HandleFunc("POST /api/voice/utterances/{id}/confirm", s.confirmUtterance)
	voice.HandleFunc("POST /api/voice/utterances/{id}/audio", s.postAudio)
	voice.HandleFunc("GET /api/voice/utterances/{id}/events", s.followUtterance)
	voice.HandleFunc("GET /api/voice/settings", s.getVoiceSettings)
	voice.HandleFunc("PATCH /api/voice/settings", s.patchVoiceSettings)
	voice.HandleFunc("GET /api/voice/latency", s.getLatency)
	voice.HandleFunc("POST /api/voice/utterances/{id}/audio-stream", s.postAudioStream)
	voice.HandleFunc("POST /api/voice/utterances/{id}/stop", s.stopUtterance)
	voice.HandleFunc("POST /api/voice/replay-recordings", s.createReplayRecording)
	voice.HandleFunc("GET /api/voice/replay-recordings", s.listReplayRecordings)
	voice.HandleFunc("POST /api/voice/replay-configurations", s.createReplayConfiguration)
	voice.HandleFunc("GET /api/voice/replay-configurations", s.listReplayConfigurations)
	voice.HandleFunc("POST /api/voice/replays", s.createReplay)
	voice.HandleFunc("GET /api/voice/replays", s.listReplays)
	voice.HandleFunc("GET /api/voice/replays/{replay_id}", s.getReplay)
	voice.HandleFunc("GET /api/voice/app/releases/latest", s.latestAppRelease)
	voice.HandleFunc("GET /api/voice/app/releases/{version_code}/apk", s.downloadAppRelease)
	mux.Handle("/api/voice/", requirePrincipalFromLoopback(voice))
	// Not under /api/voice/, so dash does not carry it: a build machine on
	// this host publishes the phone app's releases here.
	mux.HandleFunc("POST /admin/app/releases", s.requirePublishTokenFromLoopback(s.publishAppRelease))
	mux.HandleFunc("GET /admin/app/releases", s.requirePublishTokenFromLoopback(s.listAppReleases))
	return mux
}

type principalContextKey struct{}

// requirePrincipalFromLoopback trusts X-Principal-Id only on a request from
// this host, which is how dash, the front door, reaches the service.
func requirePrincipalFromLoopback(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil || !net.ParseIP(host).IsLoopback() {
			writeError(w, http.StatusForbidden, "voice-intake takes requests only from this host, through dash")
			return
		}
		principalID := strings.TrimSpace(r.Header.Get("X-Principal-Id"))
		if principalID == "" {
			writeError(w, http.StatusUnauthorized, "X-Principal-Id is required; dash sets it for the signed-in person")
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), principalContextKey{}, principalID)))
	})
}

func principalOf(r *http.Request) string { return r.Context().Value(principalContextKey{}).(string) }

// clientClockSkewMs is server time minus phone time, from X-Client-Now-Ms.
func (s *Server) clientClockSkewMs(r *http.Request, receivedAtMs int64) (int64, error) {
	raw := strings.TrimSpace(r.Header.Get("X-Client-Now-Ms"))
	if raw == "" {
		return 0, errors.New("X-Client-Now-Ms is required: the phone's clock, in epoch milliseconds, so its timestamps can be turned into server time")
	}
	clientNowMs, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("X-Client-Now-Ms %q is not epoch milliseconds", raw)
	}
	return receivedAtMs - clientNowMs, nil
}

func decodeStrict(r *http.Request, into any) error {
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(into); err != nil {
		return fmt.Errorf("invalid body: %v", err)
	}
	return nil
}

// ownedUtterance reads the utterance in the path if it is the caller's; any
// other is a 404, the same answer as a missing one.
func (s *Server) ownedUtterance(w http.ResponseWriter, r *http.Request) (Utterance, bool) {
	utterance, err := s.store.GetUtterance(r.PathValue("id"))
	if errors.Is(err, ErrNotFound) || (err == nil && utterance.PrincipalID != principalOf(r)) {
		writeError(w, http.StatusNotFound, "no utterance "+r.PathValue("id"))
		return Utterance{}, false
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return Utterance{}, false
	}
	return utterance, true
}

func (s *Server) createUtterance(w http.ResponseWriter, r *http.Request) {
	receivedAtMs := s.now().UnixMilli()
	var body struct {
		ClientUtteranceID string `json:"client_utterance_id"`
		Mode              string `json:"mode"`
		Strategy          string `json:"strategy"`
		Trigger           string `json:"trigger"`
		SpeechStartedAtMs *int64 `json:"speech_started_at_ms"`
		AudioStreaming    bool   `json:"audio_streaming"`
		AudioFormat       string `json:"audio_format"`
		// Target is optional; left out, the words go to the pool.
		Target *UtteranceTarget `json:"target"`
		// ConfirmBeforeSend is optional and off when left out. A client that
		// can show the words and send a confirm asks for it; a client that
		// cannot would leave the words held until they expire, as app 0.6.2
		// did with utterance_000341.
		ConfirmBeforeSend *bool `json:"confirm_before_send"`
	}
	if err := decodeStrict(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	var problems []string
	if strings.TrimSpace(body.ClientUtteranceID) == "" {
		problems = append(problems, "client_utterance_id is required")
	}
	if !utteranceModes[body.Mode] {
		problems = append(problems, fmt.Sprintf("mode %q is not one of %s", body.Mode, sortedKeys(utteranceModes)))
	}
	if !utteranceStrategies[body.Strategy] {
		problems = append(problems, fmt.Sprintf("strategy %q is not one of %s", body.Strategy, sortedKeys(utteranceStrategies)))
	}
	if !utteranceTriggers[body.Trigger] {
		problems = append(problems, fmt.Sprintf("trigger %q is not one of %s", body.Trigger, sortedKeys(utteranceTriggers)))
	}
	if body.SpeechStartedAtMs == nil {
		problems = append(problems, "speech_started_at_ms is required")
	}
	if body.AudioStreaming && body.AudioFormat != StreamAudioFormat {
		problems = append(problems, fmt.Sprintf("audio_format %q: a streaming utterance sends %q", body.AudioFormat, StreamAudioFormat))
	}
	if !body.AudioStreaming && body.AudioFormat != "" {
		problems = append(problems, "audio_format is for a streaming utterance; audio_streaming is not true")
	}
	target := PoolTarget
	if body.Target != nil {
		target = *body.Target
		if err := target.validate(); err != nil {
			problems = append(problems, err.Error())
		}
	}
	confirmBeforeSend := false
	if body.ConfirmBeforeSend != nil {
		if *body.ConfirmBeforeSend && !target.heldForConfirmation() {
			problems = append(problems, fmt.Sprintf("confirm_before_send is for targets %s, %s and %s; the pool's quick questions are sent at once",
				TargetSession, TargetSignal, TargetNewSession))
		}
		confirmBeforeSend = *body.ConfirmBeforeSend
	}
	skew, err := s.clientClockSkewMs(r, receivedAtMs)
	if err != nil {
		problems = append(problems, err.Error())
	}
	if len(problems) > 0 {
		writeError(w, http.StatusBadRequest, strings.Join(problems, "; "))
		return
	}
	if status, err := s.checkTarget(r.Context(), principalOf(r), target); err != nil {
		writeError(w, status, err.Error())
		return
	}
	if body.AudioStreaming {
		if err := s.dispatcher.streamingAvailable(StreamSettingsFrom(s.registry).Transcriber); err != nil {
			writeError(w, http.StatusServiceUnavailable, err.Error())
			return
		}
	}
	utterance, created, err := s.store.CreateUtterance(Utterance{
		ClientUtteranceID: body.ClientUtteranceID, PrincipalID: principalOf(r), Mode: body.Mode, Strategy: body.Strategy, Trigger: body.Trigger,
		SpeechStartedAtClientMs: *body.SpeechStartedAtMs, SpeechStartedAtMs: *body.SpeechStartedAtMs + skew, ClientClockSkewMs: skew, ReceivedAtMs: receivedAtMs,
		AudioStreaming: body.AudioStreaming, AudioFormat: body.AudioFormat, Target: target, ConfirmBeforeSend: confirmBeforeSend,
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
		if err := s.dispatcher.Start(utterance); err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		if utterance.AudioStreaming {
			if err := s.dispatcher.StartStream(utterance, StreamOptionsFor(utterance, StreamSettingsFrom(s.registry))); err != nil {
				writeError(w, http.StatusInternalServerError, err.Error())
				return
			}
		}
	}
	writeJSON(w, status, map[string]string{"utterance_id": utterance.UtteranceID})
}

// targetCheckLimit bounds asking the bridge about a target at create.
const targetCheckLimit = 15 * time.Second

// checkTarget asks the bridge, as the person, whether the session or signal
// the phone named is there to receive the words, so a wrong one is refused
// before any audio is sent. The answer is checked again when the words go.
func (s *Server) checkTarget(ctx context.Context, principalID string, target UtteranceTarget) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, targetCheckLimit)
	defer cancel()
	var err error
	switch target.Kind {
	case TargetSession:
		err = s.dispatcher.bridge.CheckSessionOwner(ctx, principalID, target.SessionID)
	case TargetSignal:
		_, err = s.dispatcher.bridge.OpenQuestionOf(ctx, principalID, target.SignalID)
	default:
		return 0, nil
	}
	switch {
	case err == nil:
		return 0, nil
	case errors.Is(err, ErrTargetNotFound):
		return http.StatusNotFound, fmt.Errorf("target: %v", err)
	case errors.Is(err, ErrSignalNotAnswerable):
		return http.StatusConflict, fmt.Errorf("target: %v", err)
	default:
		return http.StatusBadGateway, fmt.Errorf("target: could not ask llm-bridge-server: %v", err)
	}
}

func (s *Server) listUtterances(w http.ResponseWriter, r *http.Request) {
	limit := 50
	if raw := r.URL.Query().Get("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 || parsed > 1000 {
			writeError(w, http.StatusBadRequest, "limit must be a whole number from 1 to 1000")
			return
		}
		limit = parsed
	}
	utterances, err := s.store.ListUtterances(principalOf(r), limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, utterances)
}

// UtteranceRecord is GET /api/voice/utterances/{id}.
type UtteranceRecord struct {
	Utterance
	Segments              []Segment              `json:"segments"`
	Sessions              []UtteranceSession     `json:"sessions"`
	Decisions             []Decision             `json:"decisions"`
	ContinuationJudgments []ContinuationJudgment `json:"continuation_judgments"`
}

func (s *Server) getUtterance(w http.ResponseWriter, r *http.Request) {
	utterance, ok := s.ownedUtterance(w, r)
	if !ok {
		return
	}
	record := UtteranceRecord{Utterance: utterance}
	var err error
	if record.Segments, err = s.store.Segments(utterance.UtteranceID); err == nil {
		if record.Sessions, err = s.store.UtteranceSessions(utterance.UtteranceID); err == nil {
			if record.Decisions, err = s.store.Decisions(utterance.UtteranceID); err == nil {
				record.ContinuationJudgments, err = s.store.ContinuationJudgments(utterance.UtteranceID)
			}
		}
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, record)
}

func (s *Server) postSegment(w http.ResponseWriter, r *http.Request) {
	receivedAtMs := s.now().UnixMilli()
	utterance, ok := s.ownedUtterance(w, r)
	if !ok {
		return
	}
	var body struct {
		Index           *int   `json:"index"`
		Text            string `json:"text"`
		SpeechEndedAtMs *int64 `json:"speech_ended_at_ms"`
	}
	if err := decodeStrict(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	skew, err := s.clientClockSkewMs(r, receivedAtMs)
	switch {
	case body.Index == nil || *body.Index < 0:
		err = errors.New("index is required and starts at 0")
	case body.SpeechEndedAtMs == nil:
		err = errors.New("speech_ended_at_ms is required")
	case strings.TrimSpace(body.Text) == "":
		err = errors.New("text is required")
	}
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if utterance.CancelledAtMs != nil {
		writeError(w, http.StatusConflict, utterance.UtteranceID+" was cancelled")
		return
	}
	segment := Segment{UtteranceID: utterance.UtteranceID, Index: *body.Index, Text: body.Text, SpeechEndedAtClientMs: *body.SpeechEndedAtMs,
		SpeechEndedAtMs: *body.SpeechEndedAtMs + skew, ClientClockSkewMs: skew, ReceivedAtMs: receivedAtMs, Source: SegmentSourceText}
	inserted, err := s.store.InsertSegment(segment)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if !inserted {
		writeJSON(w, http.StatusOK, map[string]any{"utterance_id": utterance.UtteranceID, "index": segment.Index, "status": "already received"})
		return
	}
	if err := s.dispatcher.SegmentArrived(utterance, segment); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"utterance_id": utterance.UtteranceID, "index": segment.Index, "status": "accepted"})
}

// segmentTranscriptionLimit bounds one transcription: a whole long recording
// sent as one segment takes the longest.
const segmentTranscriptionLimit = 5 * time.Minute

// postSegmentAudio takes a segment as sound: it has the WAV transcribed and
// then handles the text as postSegment would. A transcript with no words is
// stored, so its index is used, and not dispatched. A failed transcription
// stores nothing, so the index stays free.
func (s *Server) postSegmentAudio(w http.ResponseWriter, r *http.Request) {
	receivedAtMs := s.now().UnixMilli()
	utterance, ok := s.ownedUtterance(w, r)
	if !ok {
		return
	}
	index, err := strconv.Atoi(r.PathValue("index"))
	if err != nil || index < 0 {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("segment index %q must be a whole number from 0", r.PathValue("index")))
		return
	}
	skew, err := s.clientClockSkewMs(r, receivedAtMs)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	// Without speech_ended_at_ms, the speech is taken to have ended when the
	// request arrived.
	speechEndedAtClientMs := receivedAtMs - skew
	if raw := r.URL.Query().Get("speech_ended_at_ms"); raw != "" {
		if speechEndedAtClientMs, err = strconv.ParseInt(raw, 10, 64); err != nil {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("speech_ended_at_ms %q is not epoch milliseconds", raw))
			return
		}
	}
	contentType := strings.TrimSpace(strings.Split(r.Header.Get("Content-Type"), ";")[0])
	if contentType != "audio/wav" {
		writeError(w, http.StatusUnsupportedMediaType, fmt.Sprintf("Content-Type %q: this route takes audio/wav, 16 kHz mono 16-bit PCM", contentType))
		return
	}
	if utterance.CancelledAtMs != nil {
		writeError(w, http.StatusConflict, utterance.UtteranceID+" was cancelled")
		return
	}
	alreadyReceived := func() {
		writeJSON(w, http.StatusOK, map[string]any{"utterance_id": utterance.UtteranceID, "index": index, "status": "already received"})
	}
	// Ask before transcribing, so a repeated upload costs no transcription.
	received, err := s.store.SegmentReceived(utterance.UtteranceID, index)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if received {
		alreadyReceived()
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
	transcriberName := s.registry.String(SettingTranscriber)
	transcriber, err := s.transcribers.Named(transcriberName)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "no transcription service: "+err.Error())
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), segmentTranscriptionLimit)
	defer cancel()
	heardWAV, err := highPassFilterWAV(wav, s.registry.Integer(SettingHighPassCutoffHertz))
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	transcriptionStartedAtMs := s.now().UnixMilli()
	transcript, err := transcriber.Transcribe(ctx, heardWAV)
	rawTranscript := transcript.Text
	transcribedAtMs := s.now().UnixMilli()
	if err != nil {
		message := fmt.Sprintf("segment %d: transcription failed after %d ms: %v", index, transcribedAtMs-transcriptionStartedAtMs, err)
		if recordErr := s.dispatcher.RecordError(utterance, message); recordErr != nil {
			log.Printf("[segments] ERROR %s: record error: %v", utterance.UtteranceID, recordErr)
		}
		writeError(w, http.StatusBadGateway, message)
		return
	}
	sizeBytes := int64(len(wav))
	segment := Segment{UtteranceID: utterance.UtteranceID, Index: index, Text: spokenText(rawTranscript), SpeechEndedAtClientMs: speechEndedAtClientMs,
		SpeechEndedAtMs: speechEndedAtClientMs + skew, ClientClockSkewMs: skew, ReceivedAtMs: receivedAtMs, Source: SegmentSourceAudio,
		AudioSizeBytes: &sizeBytes, AudioDurationMs: &durationMs, TranscriptionStartedAtMs: &transcriptionStartedAtMs, TranscribedAtMs: &transcribedAtMs,
		TranscriptRaw: rawTranscript, Transcriber: transcriberName}
	inserted, err := s.store.InsertSegment(segment)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if !inserted {
		alreadyReceived()
		return
	}
	// A segment with no words still goes to the dispatcher, which steps past
	// its index.
	if err := s.dispatcher.SegmentArrived(utterance, segment); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	answer := map[string]any{"utterance_id": utterance.UtteranceID, "index": index, "text": segment.Text,
		"transcription_ms": transcribedAtMs - transcriptionStartedAtMs, "audio_duration_ms": durationMs}
	if segment.Text == "" {
		answer["status"] = "no words"
		writeJSON(w, http.StatusOK, answer)
		return
	}
	answer["status"] = "accepted"
	writeJSON(w, http.StatusAccepted, answer)
}

func (s *Server) finishUtterance(w http.ResponseWriter, r *http.Request) {
	receivedAtMs := s.now().UnixMilli()
	utterance, ok := s.ownedUtterance(w, r)
	if !ok {
		return
	}
	var body struct {
		EndedAtMs *int64 `json:"ended_at_ms"`
	}
	if err := decodeStrict(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	skew, err := s.clientClockSkewMs(r, receivedAtMs)
	if err == nil && body.EndedAtMs == nil {
		err = errors.New("ended_at_ms is required")
	}
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := s.store.MarkUtteranceFinished(utterance.UtteranceID, *body.EndedAtMs, *body.EndedAtMs+skew, receivedAtMs); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if err := s.dispatcher.Finish(utterance); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"utterance_id": utterance.UtteranceID, "status": "finished"})
}

// maximumStreamChunkBytes bounds one audio-stream request: 60 s of audio.
const maximumStreamChunkBytes = 60 * 1000 * streamBytesPerMillisecond

// postAudioStream takes the next raw PCM bytes of a streaming utterance at
// offset_bytes (contract v2).
func (s *Server) postAudioStream(w http.ResponseWriter, r *http.Request) {
	utterance, ok := s.ownedUtterance(w, r)
	if !ok {
		return
	}
	if !utterance.AudioStreaming {
		writeError(w, http.StatusConflict, utterance.UtteranceID+" was not made with audio_streaming true")
		return
	}
	if utterance.CancelledAtMs != nil {
		writeError(w, http.StatusConflict, utterance.UtteranceID+" was cancelled")
		return
	}
	offset, err := strconv.ParseInt(r.URL.Query().Get("offset_bytes"), 10, 64)
	if err != nil || offset < 0 || offset%2 != 0 {
		writeError(w, http.StatusBadRequest, "offset_bytes is required: the even byte offset of the chunk in the stream")
		return
	}
	chunk, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maximumStreamChunkBytes))
	if err != nil {
		writeError(w, http.StatusRequestEntityTooLarge, err.Error())
		return
	}
	if len(chunk)%2 != 0 {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("the chunk is %d bytes, not a whole number of 16-bit samples", len(chunk)))
		return
	}
	total, err := s.dispatcher.AppendStreamAudio(r.Context(), utterance, offset, chunk)
	var ahead StreamOffsetAheadError
	switch {
	case errors.As(err, &ahead):
		writeJSON(w, http.StatusConflict, map[string]any{"expected_offset_bytes": ahead.ExpectedOffsetBytes})
	case errors.Is(err, ErrStreamStopped):
		writeJSON(w, http.StatusConflict, map[string]any{"error": err.Error(), "received_bytes": total})
	case err != nil && total == 0:
		writeError(w, http.StatusInternalServerError, err.Error())
	case err != nil:
		// The bytes are kept; the detector could not score them.
		writeJSON(w, http.StatusBadGateway, map[string]any{"error": err.Error(), "received_bytes": total})
	default:
		writeJSON(w, http.StatusOK, map[string]any{"received_bytes": total})
	}
}

// stopUtterance records that the phone stopped the microphone (contract v2).
func (s *Server) stopUtterance(w http.ResponseWriter, r *http.Request) {
	receivedAtMs := s.now().UnixMilli()
	utterance, ok := s.ownedUtterance(w, r)
	if !ok {
		return
	}
	var body struct {
		EndedAtMs *int64 `json:"ended_at_ms"`
		Reason    string `json:"reason"`
	}
	if err := decodeStrict(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	skew, err := s.clientClockSkewMs(r, receivedAtMs)
	switch {
	case err != nil:
	case body.EndedAtMs == nil:
		err = errors.New("ended_at_ms is required")
	case !stopReasons[body.Reason]:
		err = fmt.Errorf("reason %q is not one of %s", body.Reason, sortedKeys(stopReasons))
	}
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if !utterance.AudioStreaming {
		writeError(w, http.StatusConflict, utterance.UtteranceID+" was not made with audio_streaming true; it ends with finish")
		return
	}
	if err := s.dispatcher.StopStream(utterance, *body.EndedAtMs, *body.EndedAtMs+skew, body.Reason); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"utterance_id": utterance.UtteranceID, "status": "stopped"})
}

func (s *Server) cancelUtterance(w http.ResponseWriter, r *http.Request) {
	utterance, ok := s.ownedUtterance(w, r)
	if !ok {
		return
	}
	if err := s.dispatcher.Cancel(utterance); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"utterance_id": utterance.UtteranceID, "status": "cancelled"})
}

// confirmUtterance sends the words held for confirmation. The body is
// optional: {"text"} must then be the words held.
func (s *Server) confirmUtterance(w http.ResponseWriter, r *http.Request) {
	utterance, ok := s.ownedUtterance(w, r)
	if !ok {
		return
	}
	var body struct {
		Text *string `json:"text"`
	}
	raw, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if strings.TrimSpace(string(raw)) != "" {
		decoder := json.NewDecoder(strings.NewReader(string(raw)))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&body); err != nil {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid body: %v", err))
			return
		}
	}
	err = s.dispatcher.ConfirmHeldWords(utterance, body.Text)
	switch {
	case errors.Is(err, ErrNothingAwaitingConfirmation), errors.Is(err, ErrConfirmationTextDiffers):
		writeError(w, http.StatusConflict, err.Error())
		return
	case err != nil:
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"utterance_id": utterance.UtteranceID, "status": "confirmed"})
}

func (s *Server) postAudio(w http.ResponseWriter, r *http.Request) {
	utterance, ok := s.ownedUtterance(w, r)
	if !ok {
		return
	}
	filename := r.URL.Query().Get("filename")
	if filename == "" || filename != filepath.Base(filename) || filename == "." || filename == ".." || strings.ContainsAny(filename, `/\`) {
		writeError(w, http.StatusBadRequest, "filename is required and must be a bare file name")
		return
	}
	contentType := strings.TrimSpace(strings.Split(r.Header.Get("Content-Type"), ";")[0])
	if !audioContentTypes[contentType] {
		writeError(w, http.StatusUnsupportedMediaType, fmt.Sprintf("Content-Type %q is not one of %s", contentType, sortedKeys(audioContentTypes)))
		return
	}
	content, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maximumAudioBytes))
	if err != nil {
		writeError(w, http.StatusRequestEntityTooLarge, err.Error())
		return
	}
	if len(content) == 0 {
		writeError(w, http.StatusBadRequest, "the body is empty")
		return
	}
	shared, err := s.dispatcher.ReceiveAudio(utterance, filename, contentType, content)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{"error": err.Error(), "shared_into": shared})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"utterance_id": utterance.UtteranceID, "shared_into": shared})
}

func (s *Server) followUtterance(w http.ResponseWriter, r *http.Request) {
	utterance, ok := s.ownedUtterance(w, r)
	if !ok {
		return
	}
	flusher, canFlush := w.(http.Flusher)
	if !canFlush {
		writeError(w, http.StatusInternalServerError, "this response cannot be streamed")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	ctx, cancel := context.WithTimeout(r.Context(), followLimit)
	defer cancel()
	events := make(chan UtteranceEvent)
	followDone := make(chan error, 1)
	go func() {
		followDone <- s.dispatcher.Follow(ctx, utterance, func(event UtteranceEvent) error {
			select {
			case events <- event:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	}()
	// A comment line every 15 s keeps proxies from closing a quiet stream.
	keepalive := time.NewTicker(15 * time.Second)
	defer keepalive.Stop()
	for {
		select {
		case event := <-events:
			data, err := json.Marshal(event.Data)
			if err != nil {
				log.Printf("[events] ERROR %s: encode %s: %v", utterance.UtteranceID, event.Name, err)
				return
			}
			if _, err := fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event.Name, data); err != nil {
				return
			}
			flusher.Flush()
		case <-keepalive.C:
			if _, err := fmt.Fprint(w, ": keepalive\n\n"); err != nil {
				return
			}
			flusher.Flush()
		case err := <-followDone:
			if errors.Is(err, context.DeadlineExceeded) {
				fmt.Fprintf(w, "event: %s\ndata: {}\n\n", eventDone)
				flusher.Flush()
			}
			return
		}
	}
}

func (s *Server) getVoiceSettings(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, VoiceSettings(s.registry))
}

func (s *Server) patchVoiceSettings(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	changes, err := voiceSettingsPatch(body)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	// Check every value with its owner before writing any, so a refused
	// field leaves the others unchanged.
	for key, value := range changes {
		if err := s.validateSetting(r.Context(), key, value); err != nil {
			status := http.StatusBadRequest
			if errors.Is(err, servicesettings.ErrSettingOwnerUnavailable) {
				status = http.StatusBadGateway
			}
			writeError(w, status, err.Error())
			return
		}
	}
	keys := make([]string, 0, len(changes))
	for key := range changes {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if err := s.registry.Set(key, changes[key]); err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
	}
	log.Printf("[settings] %s changed %s", principalOf(r), strings.Join(keys, ", "))
	writeJSON(w, http.StatusOK, VoiceSettings(s.registry))
}

// SetSettingValidator registers the check PATCH runs on a new value of one
// setting before anything is written.
func (s *Server) SetSettingValidator(key string, validator func(ctx context.Context, value string) error) {
	s.settingValidators[key] = validator
}

func (s *Server) validateSetting(ctx context.Context, key, value string) error {
	validator, ok := s.settingValidators[key]
	if !ok || value == "" {
		return nil
	}
	if err := validator(ctx, value); err != nil {
		return fmt.Errorf("%s: %w", key, err)
	}
	return nil
}

func (s *Server) getLatency(w http.ResponseWriter, r *http.Request) {
	groupBy := r.URL.Query().Get("group_by")
	if groupBy == "configuration" {
		groups, err := s.store.ReplayLatencyGroups()
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"group_by": groupBy, "groups": groups})
		return
	}
	if !latencyGroupings[groupBy] {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("group_by must be configuration or one of %s", sortedKeys(latencyGroupings)))
		return
	}
	samples, err := s.store.LatencySamples()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"group_by": groupBy, "groups": LatencyMedians(samples, groupBy)})
}

func sortedKeys(set map[string]bool) string {
	keys := make([]string, 0, len(set))
	for key := range set {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return strings.Join(keys, ", ")
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		log.Printf("[http] ERROR encode answer: %v", err)
	}
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}
