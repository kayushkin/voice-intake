package voiceintake

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeDetector scores a 512-sample window as speech (0.9) when its samples
// are loud and as silence (0.05) when they are quiet, keeping per-stream
// state the way speech-server does.
type fakeDetector struct {
	mutex   sync.Mutex
	pending map[string][]byte
	scored  map[string]int
	closed  []string
}

func newFakeDetector() *fakeDetector {
	return &fakeDetector{pending: map[string][]byte{}, scored: map[string]int{}}
}

func (d *fakeDetector) Score(ctx context.Context, streamID string, pcm []byte) (DetectorScores, error) {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	buffer := append(d.pending[streamID], pcm...)
	scores := DetectorScores{WindowSamples: 512, FirstWindow: d.scored[streamID], Probabilities: []float64{}}
	for len(buffer) >= 1024 {
		sample := int16(binary.LittleEndian.Uint16(buffer[0:2]))
		probability := 0.05
		if sample > 1000 || sample < -1000 {
			probability = 0.9
		}
		scores.Probabilities = append(scores.Probabilities, probability)
		buffer = buffer[1024:]
	}
	d.pending[streamID] = buffer
	d.scored[streamID] += len(scores.Probabilities)
	return scores, nil
}

func (d *fakeDetector) Close(ctx context.Context, streamID string) error {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	d.closed = append(d.closed, streamID)
	return nil
}

// queueTranscriber answers each call with the next text in its queue.
type queueTranscriber struct {
	mutex sync.Mutex
	texts []string
	calls int
}

func (q *queueTranscriber) Transcribe(ctx context.Context, wav []byte) (Transcript, error) {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	q.calls++
	if len(q.texts) == 0 {
		return Transcript{}, fmt.Errorf("the queue is empty")
	}
	text := q.texts[0]
	q.texts = q.texts[1:]
	return Transcript{Text: text, Words: []TranscriptWord{{Word: text, StartMs: 0, EndMs: 500}}}, nil
}

// audio builds PCM: each part is milliseconds of loud (speech) or quiet
// (silence) samples.
type audioPart struct {
	speech       bool
	milliseconds int
}

func pcmOf(parts ...audioPart) []byte {
	var pcm []byte
	for _, part := range parts {
		value := int16(0)
		if part.speech {
			value = 8000
		}
		for range part.milliseconds * 16 {
			pcm = binary.LittleEndian.AppendUint16(pcm, uint16(value))
		}
	}
	return pcm
}

func newStreamingHarness(t *testing.T, environment map[string]string, texts ...string) (*harness, *queueTranscriber) {
	t.Helper()
	queue := &queueTranscriber{texts: texts}
	variables := map[string]string{"VOICE_INTAKE_TRANSCRIBER": "queue"}
	for key, value := range environment {
		variables[key] = value
	}
	h := newHarnessWithTranscribers(t, variables, Transcribers{"queue": queue})
	return h, queue
}

func (h *harness) createStreamingUtterance(clientID, mode, strategy string) string {
	h.t.Helper()
	status, answer := h.call("POST", "/api/voice/utterances", map[string]any{"client_utterance_id": clientID, "mode": mode, "strategy": strategy,
		"trigger": "tile", "speech_started_at_ms": time.Now().UnixMilli(), "audio_streaming": true, "audio_format": StreamAudioFormat}, testPrincipal)
	if status != http.StatusCreated {
		h.t.Fatalf("create streaming utterance = %d %v", status, answer)
	}
	return answer["utterance_id"].(string)
}

func (h *harness) postStream(utteranceID string, offset int, chunk []byte) (int, map[string]any) {
	h.t.Helper()
	request, _ := http.NewRequest("POST", h.server.URL+"/api/voice/utterances/"+utteranceID+"/audio-stream?offset_bytes="+strconv.Itoa(offset), bytes.NewReader(chunk))
	request.Header.Set("Content-Type", "application/octet-stream")
	request.Header.Set("X-Principal-Id", testPrincipal)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		h.t.Fatal(err)
	}
	defer response.Body.Close()
	raw, _ := io.ReadAll(response.Body)
	answer := map[string]any{}
	_ = json.Unmarshal(raw, &answer)
	return response.StatusCode, answer
}

// streamAll posts pcm in 200 ms chunks, as the phone does.
func (h *harness) streamAll(utteranceID string, pcm []byte) {
	h.t.Helper()
	const chunk = 6400
	for offset := 0; offset < len(pcm); offset += chunk {
		end := min(len(pcm), offset+chunk)
		if status, answer := h.postStream(utteranceID, offset, pcm[offset:end]); status != http.StatusOK {
			h.t.Fatalf("audio-stream at %d = %d %v", offset, status, answer)
		}
	}
}

func (h *harness) stop(utteranceID, reason string) (int, map[string]any) {
	h.t.Helper()
	return h.call("POST", "/api/voice/utterances/"+utteranceID+"/stop", map[string]any{"ended_at_ms": time.Now().UnixMilli(), "reason": reason}, testPrincipal)
}

func eventsNamed(events []receivedEvent, name string) []receivedEvent {
	var named []receivedEvent
	for _, event := range events {
		if event.name == name {
			named = append(named, event)
		}
	}
	return named
}

func TestAStreamedSegmentEndsOnSilenceIsTranscribedAndDispatched(t *testing.T) {
	h, _ := newStreamingHarness(t, map[string]string{"VOICE_INTAKE_ENDPOINT_SILENCE_MS": "500"}, "what's the weather like")
	utteranceID := h.createStreamingUtterance("stream-1", "quick", "endpoint")
	h.streamAll(utteranceID, pcmOf(audioPart{false, 400}, audioPart{true, 1000}, audioPart{false, 800}))
	if status, answer := h.stop(utteranceID, StopReasonUser); status != http.StatusOK {
		t.Fatalf("stop = %d %v", status, answer)
	}
	events := h.followUntilDone(utteranceID)
	hearing := eventsNamed(events, eventHearing)
	if len(hearing) != 2 || hearing[0].data["speech"] != true || hearing[1].data["speech"] != false {
		t.Fatalf("hearing events = %v", hearing)
	}
	transcripts := eventsNamed(events, eventTranscript)
	if len(transcripts) != 1 || transcripts[0].data["text"] != "what's the weather like" || transcripts[0].data["index"] != float64(0) {
		t.Fatalf("transcript events = %v", transcripts)
	}
	_, ids, sent, _ := h.bridge.snapshot()
	if len(ids) != 1 || sent[ids[0]][0] != "what's the weather like" {
		t.Fatalf("sessions %v sent %v", ids, sent)
	}
	_, record := h.call("GET", "/api/voice/utterances/"+utteranceID, nil, testPrincipal)
	segment := record["segments"].([]any)[0].(map[string]any)
	if segment["source"] != SegmentSourceStream || segment["detected_speech_start_offset_ms"] == nil || segment["endpointed_at_ms"] == nil {
		t.Fatalf("segment = %v", segment)
	}
	// Speech ran 400..1400 ms; the detector's windows are 32 ms.
	if end := segment["detected_speech_end_offset_ms"].(float64); end < 1380 || end > 1440 {
		t.Fatalf("detected speech end = %v ms, want about 1400", end)
	}
	if record["stop_reason"] != StopReasonUser {
		t.Fatalf("stop_reason = %v", record["stop_reason"])
	}
	if len(h.detector.closed) != 1 {
		t.Fatalf("detector streams closed = %v", h.detector.closed)
	}
}

func TestTwoPausesMakeTwoSegmentsAndAShortClickMakesNone(t *testing.T) {
	h, queue := newStreamingHarness(t, map[string]string{"VOICE_INTAKE_ENDPOINT_SILENCE_MS": "300"}, "what's the weather like", "in São Paulo")
	utteranceID := h.createStreamingUtterance("stream-2", "quick", "endpoint")
	// A 64 ms tick first: shorter than minimumSpeechMs, so no segment.
	h.streamAll(utteranceID, pcmOf(audioPart{true, 64}, audioPart{false, 600}, audioPart{true, 800}, audioPart{false, 500}, audioPart{true, 600}, audioPart{false, 500}))
	h.stop(utteranceID, StopReasonUser)
	events := h.followUntilDone(utteranceID)
	if got := len(eventsNamed(events, eventTranscript)); got != 2 || queue.calls != 2 {
		t.Fatalf("transcripts = %d, transcriber calls = %d, want 2 and 2", got, queue.calls)
	}
	roles := decisionsOf(events)
	_, ids, sent, _ := h.bridge.snapshot()
	joined := sessionByText(t, ids, sent, "what's the weather like in São Paulo")
	if roles[joined] != RoleKept {
		t.Fatalf("joined session role = %q (roles %v)", roles[joined], roles)
	}
}

func TestLongModeIsOneSegmentCutAtStop(t *testing.T) {
	h, queue := newStreamingHarness(t, map[string]string{"VOICE_INTAKE_ENDPOINT_SILENCE_MS": "300"}, "a long dictation with a pause in it")
	utteranceID := h.createStreamingUtterance("stream-3", "long", "endpoint")
	h.streamAll(utteranceID, pcmOf(audioPart{true, 800}, audioPart{false, 1500}, audioPart{true, 800}))
	h.stop(utteranceID, StopReasonUser)
	events := h.followUntilDone(utteranceID)
	if got := len(eventsNamed(events, eventTranscript)); got != 1 || queue.calls != 1 {
		t.Fatalf("transcripts = %d, transcriber calls = %d, want 1", got, queue.calls)
	}
	created, _, _, _ := h.bridge.snapshot()
	if len(created) != 1 || created[0].Effort != "high" {
		t.Fatalf("long mode sessions = %+v, want one at the raised effort", created)
	}
}

func TestNoSpeechAskesThePhoneToStop(t *testing.T) {
	h, _ := newStreamingHarness(t, map[string]string{"VOICE_INTAKE_NO_SPEECH_TIMEOUT_MS": "1000"})
	utteranceID := h.createStreamingUtterance("stream-4", "quick", "endpoint")
	h.streamAll(utteranceID, pcmOf(audioPart{false, 1200}))
	if status, answer := h.stop(utteranceID, StopReasonNoSpeech); status != http.StatusOK {
		t.Fatalf("stop with the server's reason = %d %v", status, answer)
	}
	events := h.followUntilDone(utteranceID)
	stops := eventsNamed(events, eventStopRecording)
	if len(stops) != 1 || stops[0].data["reason"] != StopReasonNoSpeech {
		t.Fatalf("stop_recording events = %v", stops)
	}
}

func TestTheContinuationWindowClosingAsksThePhoneToStop(t *testing.T) {
	h, _ := newStreamingHarness(t, map[string]string{"VOICE_INTAKE_ENDPOINT_SILENCE_MS": "300", "VOICE_INTAKE_CONTINUATION_WINDOW_MS": "1000"}, "hello")
	utteranceID := h.createStreamingUtterance("stream-5", "quick", "endpoint")
	h.streamAll(utteranceID, pcmOf(audioPart{true, 600}, audioPart{false, 1400}))
	h.stop(utteranceID, StopReasonContinuationWindowClosed)
	stops := eventsNamed(h.followUntilDone(utteranceID), eventStopRecording)
	if len(stops) != 1 || stops[0].data["reason"] != StopReasonContinuationWindowClosed {
		t.Fatalf("stop_recording events = %v", stops)
	}
}

func TestStreamOffsetsRepeatsGapsAndStopReasons(t *testing.T) {
	h, _ := newStreamingHarness(t, nil)
	utteranceID := h.createStreamingUtterance("stream-6", "quick", "endpoint")
	chunk := pcmOf(audioPart{false, 200})
	if status, answer := h.postStream(utteranceID, 0, chunk); status != http.StatusOK || answer["received_bytes"] != float64(6400) {
		t.Fatalf("first chunk = %d %v", status, answer)
	}
	if status, answer := h.postStream(utteranceID, 0, chunk); status != http.StatusOK || answer["received_bytes"] != float64(6400) {
		t.Fatalf("repeated chunk = %d %v", status, answer)
	}
	// Half overlapping: only the new half is added.
	if status, answer := h.postStream(utteranceID, 3200, chunk); status != http.StatusOK || answer["received_bytes"] != float64(9600) {
		t.Fatalf("overlapping chunk = %d %v", status, answer)
	}
	if status, answer := h.postStream(utteranceID, 20000, chunk); status != http.StatusConflict || answer["expected_offset_bytes"] != float64(9600) {
		t.Fatalf("chunk past the end = %d %v", status, answer)
	}
	if status, _ := h.stop(utteranceID, "bored"); status != http.StatusBadRequest {
		t.Fatalf("stop with an unknown reason = %d, want 400", status)
	}
	if status, _ := h.call("POST", "/api/voice/utterances", map[string]any{"client_utterance_id": "stream-7", "mode": "quick", "strategy": "endpoint",
		"trigger": "tile", "speech_started_at_ms": 1, "audio_streaming": true, "audio_format": "mp3"}, testPrincipal); status != http.StatusBadRequest {
		t.Fatalf("streaming with another format = %d, want 400", status)
	}
}

func TestEndpointerDropsATickAndFinishesOpenSpeechAtTheEndOfTheAudio(t *testing.T) {
	e := newEndpointer(512, 320*time.Millisecond)
	window := 0
	feed := func(probability float64, count int) []endpointerEvent {
		var events []endpointerEvent
		for range count {
			events = append(events, e.feed(window, probability))
			window++
		}
		return events
	}
	for _, event := range feed(0.95, 2) {
		if event.SegmentEnded {
			t.Fatal("a 2-window tick started a segment that ended")
		}
	}
	for _, event := range feed(0.05, 20) {
		if event.SegmentEnded {
			t.Fatal("a 2-window tick ended as a segment")
		}
	}
	feed(0.9, 30)
	finished := e.finish(window*512 + 100)
	if !finished.SegmentEnded || finished.StartSample != 22*512 || finished.EndSample != window*512+100 {
		t.Fatalf("finish = %+v", finished)
	}
}

func TestWordAccuracyReadsSpokenNumbersAsDigits(t *testing.T) {
	cases := []struct {
		expected, actual string
		want             float64
	}{
		{"Testing 123.", "Testing one, two, three.", 1},
		{"Testing 38.", "Testing thirty-eight.", 1},
		{"Hey, what's up?", "Hey what's up?", 1},
		{"What's on my schedule for today?", "What's on my schedule today?", 5.0 / 6},
		{"Hey, what's up?", "", 0},
	}
	for _, c := range cases {
		if got := WordAccuracy(c.expected, c.actual); got < c.want-0.001 || got > c.want+0.001 {
			t.Errorf("WordAccuracy(%q, %q) = %.3f, want %.3f (%v vs %v)", c.expected, c.actual, got, c.want, normalizedWords(c.expected), normalizedWords(c.actual))
		}
	}
}

func TestAReplayStreamsARecordingAndStopsItsSessionAfterTheTurn(t *testing.T) {
	warmSessionSettleTime = 100 * time.Millisecond
	h, _ := newStreamingHarness(t, nil, "testing one two three")
	wav := pcmToWAV(pcmOf(audioPart{false, 200}, audioPart{true, 600}, audioPart{false, 800}))
	request, _ := http.NewRequest("POST", h.server.URL+"/api/voice/replay-recordings?expected_text=Testing+123&true_speech_end_ms=800&true_speech_end_source=made+up",
		bytes.NewReader(wav))
	request.Header.Set("X-Principal-Id", testPrincipal)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(response.Body)
	response.Body.Close()
	if response.StatusCode != http.StatusCreated || !strings.Contains(string(raw), "recording_000001") {
		t.Fatalf("create recording = %d %s", response.StatusCode, raw)
	}
	status, configuration := h.call("POST", "/api/voice/replay-configurations", map[string]any{"name": "silero 300 ms", "detector": "silero",
		"endpoint_silence_ms": 300, "transcriber": "queue", "dispatch": "session", "instance_id": "instance-for-tests", "effort": "low",
		"max_budget_usd": 0.5, "warm_session": true}, testPrincipal)
	if status != http.StatusCreated {
		t.Fatalf("create configuration = %d %v", status, configuration)
	}
	status, replay := h.call("POST", "/api/voice/replays", map[string]any{"configuration_id": configuration["configuration_id"], "recording_id": "recording_000001"}, testPrincipal)
	if status != http.StatusAccepted {
		t.Fatalf("create replay = %d %v", status, replay)
	}
	var record map[string]any
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		_, record = h.call("GET", "/api/voice/replays/"+replay["replay_id"].(string), nil, testPrincipal)
		if record["finished_at_ms"] != nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if record["finished_at_ms"] == nil || record["error"] != "" {
		t.Fatalf("replay = %v", record)
	}
	if record["word_accuracy"] != float64(1) {
		t.Fatalf("word accuracy = %v", record["word_accuracy"])
	}
	created, ids, _, stopped := h.bridge.snapshot()
	if len(ids) != 1 || created[0].MaxBudgetUSD != 0.5 || !contains(stopped, ids[0]) {
		t.Fatalf("sessions %v created %+v stopped %v: want the one warm session, with its budget, stopped after the turn", ids, created, stopped)
	}
	sessions := record["utterance"].(map[string]any)["sessions"].([]any)
	if session := sessions[0].(map[string]any); session["pooled"] != true || !strings.HasPrefix(session["reply_text"].(string), "answer to") {
		t.Fatalf("session = %v", session)
	}
	_, latency := h.call("GET", "/api/voice/latency?group_by=configuration", nil, testPrincipal)
	group := latency["groups"].([]any)[0].(map[string]any)
	medians := group["median_ms"].(map[string]any)
	if group["replays"] != float64(1) || medians["endpoint_delay"] == nil || medians["speech_end_to_first_block"] == nil {
		t.Fatalf("latency group = %v", group)
	}
	if _, live := h.call("GET", "/api/voice/latency?group_by=source", nil, testPrincipal); len(live["groups"].([]any)) != 0 {
		t.Fatalf("the live latency table counts the replay: %v", live)
	}
}
