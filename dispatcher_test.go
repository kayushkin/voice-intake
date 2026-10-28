package voiceintake

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kayushkin/llm-bridge/servicesettings"
)

// fakeBridge stands in for llm-bridge-server. A sent message produces a
// block event and then a result event on the session's stream.
type fakeBridge struct {
	mutex      sync.Mutex
	next       int
	created    []SessionCreation
	createdIDs []string
	sent       map[string][]string
	stopped    []string
	archived   []string
	renamed    map[string]string
	shared     map[string][]string
	streams    map[string]chan BridgeEvent
	resultWait time.Duration

	// The person's own sessions and open signals, for target tests.
	sessionOwners map[string]string
	openSignals   []bridgeSignal
	personSent    map[string][]string
	answers       []map[string]string
	answerVia     string
	chatCreated   []string
}

func newFakeBridge() *fakeBridge {
	return &fakeBridge{sent: map[string][]string{}, renamed: map[string]string{}, shared: map[string][]string{},
		streams: map[string]chan BridgeEvent{}, resultWait: 20 * time.Millisecond,
		sessionOwners: map[string]string{}, personSent: map[string][]string{}, answerVia: "message"}
}

func (b *fakeBridge) stream(sessionID string) chan BridgeEvent {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	if b.streams[sessionID] == nil {
		b.streams[sessionID] = make(chan BridgeEvent, 16)
	}
	return b.streams[sessionID]
}

func (b *fakeBridge) CreateSession(ctx context.Context, creation SessionCreation) (string, error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	b.next++
	sessionID := fmt.Sprintf("br_fake_%d", b.next)
	b.created = append(b.created, creation)
	b.createdIDs = append(b.createdIDs, sessionID)
	return sessionID, nil
}

func (b *fakeBridge) SendMessage(ctx context.Context, sessionID, text string) error {
	b.mutex.Lock()
	b.sent[sessionID] = append(b.sent[sessionID], text)
	wait := b.resultWait
	b.mutex.Unlock()
	events := b.stream(sessionID)
	go func() {
		events <- BridgeEvent{Type: "block", Data: json.RawMessage(`{"type":"block","bridge_session_id":"` + sessionID + `"}`)}
		time.Sleep(wait)
		events <- BridgeEvent{Type: "result", Data: json.RawMessage(`{"type":"result","bridge_session_id":"` + sessionID + `","result":{"text":"answer to ` + strings.ReplaceAll(text, `"`, `'`) + `"}}`)}
	}()
	return nil
}

func (b *fakeBridge) StopSession(ctx context.Context, sessionID string) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	b.stopped = append(b.stopped, sessionID)
	return nil
}

func (b *fakeBridge) ArchiveSession(ctx context.Context, sessionID string) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	b.archived = append(b.archived, sessionID)
	return nil
}

func (b *fakeBridge) RenameSession(ctx context.Context, sessionID, displayName string) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	b.renamed[sessionID] = displayName
	return nil
}

func (b *fakeBridge) SessionState(ctx context.Context, sessionID string) (string, error) {
	return "idle", nil
}

func (b *fakeBridge) StreamSessionEvents(ctx context.Context, sessionID string, connected func(), deliver func(BridgeEvent) bool) error {
	events := b.stream(sessionID)
	connected()
	for {
		select {
		case event := <-events:
			if !deliver(event) {
				return nil
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (b *fakeBridge) ShareFile(ctx context.Context, sessionID, filename, contentType string, content []byte) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	b.shared[sessionID] = append(b.shared[sessionID], filename)
	return nil
}

func (b *fakeBridge) InstanceExists(ctx context.Context, instanceID string) (bool, error) {
	return true, nil
}

func (b *fakeBridge) InstanceOneshot(ctx context.Context, instanceID, model, systemPrompt, prompt string) (OneshotAnswer, error) {
	return OneshotAnswer{Text: "oneshot answer to " + prompt, Model: model}, nil
}

// ownSession gives the person a session that has already finished a turn:
// like the bridge, its stream first sends that turn again to a subscriber.
func (b *fakeBridge) ownSession(sessionID, principalID string) {
	b.mutex.Lock()
	b.sessionOwners[sessionID] = principalID
	b.mutex.Unlock()
	events := b.stream(sessionID)
	events <- BridgeEvent{Type: "block", Data: json.RawMessage(`{"type":"block","bridge_session_id":"` + sessionID + `"}`)}
	events <- BridgeEvent{Type: "result", Data: json.RawMessage(`{"type":"result","bridge_session_id":"` + sessionID + `","result":{"text":"stale reply from the turn before"}}`)}
}

// replyAfterOwnMessage sends what the bridge sends for a new message: the
// user_message, then a block and a result.
func (b *fakeBridge) replyAfterOwnMessage(sessionID, text string, withUserMessage bool) {
	events := b.stream(sessionID)
	go func() {
		if withUserMessage {
			events <- BridgeEvent{Type: "user_message", Data: json.RawMessage(`{"type":"user_message","bridge_session_id":"` + sessionID + `"}`)}
		}
		events <- BridgeEvent{Type: "block", Data: json.RawMessage(`{"type":"block","bridge_session_id":"` + sessionID + `"}`)}
		time.Sleep(b.resultWait)
		events <- BridgeEvent{Type: "result", Data: json.RawMessage(`{"type":"result","bridge_session_id":"` + sessionID + `","result":{"text":"answer to ` + strings.ReplaceAll(text, `"`, `'`) + `"}}`)}
	}()
}

func (b *fakeBridge) CreateChatSession(ctx context.Context, principalID, displayName string) (string, string, error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	b.next++
	sessionID := fmt.Sprintf("br_chat_%d", b.next)
	b.chatCreated = append(b.chatCreated, sessionID)
	b.createdIDs = append(b.createdIDs, sessionID)
	return sessionID, "instance-the-chat-picks", nil
}

func (b *fakeBridge) CheckSessionOwner(ctx context.Context, principalID, sessionID string) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	if b.sessionOwners[sessionID] != principalID {
		return fmt.Errorf("no session %s: %w", sessionID, ErrTargetNotFound)
	}
	return nil
}

func (b *fakeBridge) OpenQuestionOf(ctx context.Context, principalID, signalID string) (OpenQuestion, error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	var mine []bridgeSignal
	for _, signal := range b.openSignals {
		if b.sessionOwners[signal.SessionID] == principalID {
			mine = append(mine, signal)
		}
	}
	return openQuestionFrom(mine, signalID)
}

func (b *fakeBridge) SendMessageAsPerson(ctx context.Context, principalID, sessionID, text string) error {
	if err := b.CheckSessionOwner(ctx, principalID, sessionID); err != nil {
		return err
	}
	b.mutex.Lock()
	b.personSent[sessionID] = append(b.personSent[sessionID], text)
	b.mutex.Unlock()
	b.replyAfterOwnMessage(sessionID, text, true)
	return nil
}

func (b *fakeBridge) AnswerSignal(ctx context.Context, principalID, signalID string, answers map[string]string) (string, error) {
	question, err := b.OpenQuestionOf(ctx, principalID, signalID)
	if err != nil {
		return "", err
	}
	b.mutex.Lock()
	b.answers = append(b.answers, answers)
	via := b.answerVia
	remaining := b.openSignals[:0]
	for _, signal := range b.openSignals {
		if _, answered := answers[signal.ID]; !answered {
			remaining = append(remaining, signal)
		}
	}
	b.openSignals = remaining
	b.mutex.Unlock()
	b.replyAfterOwnMessage(question.SessionID, answers[signalID], via == "message")
	return via, nil
}

func (b *fakeBridge) snapshot() (created []SessionCreation, ids []string, sent map[string][]string, stopped []string) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	sent = map[string][]string{}
	for key, value := range b.sent {
		sent[key] = append([]string(nil), value...)
	}
	return append([]SessionCreation(nil), b.created...), append([]string(nil), b.createdIDs...), sent, append([]string(nil), b.stopped...)
}

// fakeJudge answers by the new text, after a delay like a real model's.
type fakeJudge struct {
	mutex     sync.Mutex
	continues map[string]bool
	asked     []string
	delay     time.Duration
}

func (j *fakeJudge) Judge(ctx context.Context, modelRole, earlierText, newText string) (ContinuationVerdict, error) {
	time.Sleep(j.delay)
	j.mutex.Lock()
	defer j.mutex.Unlock()
	j.asked = append(j.asked, earlierText+" | "+newText)
	continues, known := j.continues[newText]
	if !known {
		return ContinuationVerdict{}, fmt.Errorf("the fake judge has no answer for %q", newText)
	}
	return ContinuationVerdict{Continues: continues, Reason: "fake", Model: "fake-model"}, nil
}

type harness struct {
	t        *testing.T
	bridge   *fakeBridge
	judge    *fakeJudge
	pool     *SessionPool
	server   *httptest.Server
	detector *fakeDetector
}

func newHarness(t *testing.T, environment map[string]string) *harness {
	t.Helper()
	return newHarnessWithTranscribers(t, environment, nil)
}

// newHarnessWithTranscribers adds transcribers beside "fake", the fake
// whisper-server.
func newHarnessWithTranscribers(t *testing.T, environment map[string]string, extra Transcribers) *harness {
	t.Helper()
	variables := map[string]string{"LLMBRIDGE_SERVICE_TOKEN": "test-token", "VOICE_INTAKE_INSTANCE_ID": "instance-for-tests", "VOICE_INTAKE_POOL_SIZE": "0",
		"VOICE_INTAKE_TRANSCRIBER": "fake",
		// The fakes read meaning out of raw sample bytes, which a filter would
		// change; tests that want the filter or the weak-speech gate set them.
		"VOICE_INTAKE_HIGH_PASS_CUTOFF_HZ": "0", "VOICE_INTAKE_MINIMUM_SEGMENT_MEAN_SPEECH_PROBABILITY": "0"}
	for key, value := range environment {
		variables[key] = value
	}
	registry, err := NewSettingsRegistry(servicesettings.MapEnvironment(variables))
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	store, err := OpenStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	if err := registry.AttachStoredValues(store.Settings(), nil); err != nil {
		t.Fatal(err)
	}
	fakeWhisper := httptest.NewServer(http.HandlerFunc(fakeWhisperServer))
	t.Cleanup(fakeWhisper.Close)
	transcriber := NewWhisperServerTranscriber(fakeWhisper.URL + "/inference")
	bridge := newFakeBridge()
	judge := &fakeJudge{continues: map[string]bool{"in São Paulo": true, "can you add cheese to my burger": false}, delay: 30 * time.Millisecond}
	settings := func() DispatchSettings { return DispatchSettingsFrom(registry) }
	pool := NewSessionPool(store, bridge, settings, time.Now)
	dispatcher := NewDispatcher(store, bridge, judge, pool, settings, time.Now, directory+"/audio")
	detector := newFakeDetector()
	transcribers := Transcribers{"fake": transcriber}
	for name, more := range extra {
		transcribers[name] = more
	}
	dispatcher.SetSpeechServices(detector, transcribers)
	dispatcher.SetReplayRecordingDirectory(directory + "/replay-recordings")
	server := NewServer(store, dispatcher, pool, registry, transcribers, time.Now)
	registry.OnChange(func(string) { pool.Resync(context.Background()) })
	httpServer := httptest.NewServer(server.Handler())
	t.Cleanup(httpServer.Close)
	return &harness{t: t, bridge: bridge, judge: judge, pool: pool, server: httpServer, detector: detector}
}

func (h *harness) call(method, path string, body any, principalID string) (int, map[string]any) {
	h.t.Helper()
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			h.t.Fatal(err)
		}
		reader = bytes.NewReader(encoded)
	}
	request, err := http.NewRequest(method, h.server.URL+path, reader)
	if err != nil {
		h.t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Client-Now-Ms", strconv.FormatInt(time.Now().UnixMilli(), 10))
	if principalID != "" {
		request.Header.Set("X-Principal-Id", principalID)
	}
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

const testPrincipal = "principal_000001"

func (h *harness) createUtterance(clientID string) string {
	h.t.Helper()
	status, answer := h.call("POST", "/api/voice/utterances", map[string]any{"client_utterance_id": clientID, "mode": "quick", "strategy": "endpoint",
		"trigger": "tile", "speech_started_at_ms": time.Now().UnixMilli() - 3000}, testPrincipal)
	if status != http.StatusCreated {
		h.t.Fatalf("create utterance = %d %v", status, answer)
	}
	return answer["utterance_id"].(string)
}

func (h *harness) postSegment(utteranceID string, index int, text string, speechEndedAtMs int64) int {
	h.t.Helper()
	status, answer := h.call("POST", "/api/voice/utterances/"+utteranceID+"/segments",
		map[string]any{"index": index, "text": text, "speech_ended_at_ms": speechEndedAtMs}, testPrincipal)
	if status != http.StatusAccepted && status != http.StatusOK {
		h.t.Fatalf("segment %d = %d %v", index, status, answer)
	}
	return status
}

type receivedEvent struct {
	name string
	data map[string]any
}

// followUntilDone reads the utterance's event stream until done.
func (h *harness) followUntilDone(utteranceID string) []receivedEvent {
	h.t.Helper()
	return h.followUntil(utteranceID, eventDone)
}

// followUntil reads the utterance's event stream, from the start, until the
// first event named last.
func (h *harness) followUntil(utteranceID, last string) []receivedEvent {
	h.t.Helper()
	request, _ := http.NewRequest("GET", h.server.URL+"/api/voice/utterances/"+utteranceID+"/events", nil)
	request.Header.Set("X-Principal-Id", testPrincipal)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	response, err := http.DefaultClient.Do(request.WithContext(ctx))
	if err != nil {
		h.t.Fatal(err)
	}
	defer response.Body.Close()
	if got := response.Header.Get("Content-Type"); got != "text/event-stream" {
		h.t.Fatalf("events Content-Type = %q", got)
	}
	var events []receivedEvent
	scanner := bufio.NewScanner(response.Body)
	name := ""
	for scanner.Scan() {
		line := scanner.Text()
		switch {
		case strings.HasPrefix(line, "event: "):
			name = strings.TrimPrefix(line, "event: ")
		case strings.HasPrefix(line, "data: "):
			data := map[string]any{}
			if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &data); err != nil {
				h.t.Fatalf("event %s data: %v", name, err)
			}
			events = append(events, receivedEvent{name: name, data: data})
			if name == last {
				return events
			}
		}
	}
	h.t.Fatalf("the event stream ended without %s; got %v (err %v)", last, events, scanner.Err())
	return nil
}

func decisionsOf(events []receivedEvent) map[string]string {
	roles := map[string]string{}
	for _, event := range events {
		if event.name == eventDecision {
			roles[event.data["session_id"].(string)] = event.data["role"].(string)
		}
	}
	return roles
}

func sessionByText(t *testing.T, ids []string, sent map[string][]string, text string) string {
	t.Helper()
	for _, id := range ids {
		if len(sent[id]) == 1 && sent[id][0] == text {
			return id
		}
	}
	t.Fatalf("no session was sent %q; sent %v", text, sent)
	return ""
}

func contains(list []string, item string) bool {
	for _, element := range list {
		if element == item {
			return true
		}
	}
	return false
}

func TestAContinuingSegmentKeepsTheJoinedSessionAndSupersedesTheEarlierOne(t *testing.T) {
	h := newHarness(t, map[string]string{"VOICE_INTAKE_POOL_SIZE": "1"})
	// Let the pool start its session before the first utterance takes it.
	h.pool.Fill(testPrincipal)
	h.pool.WaitForFills()

	utteranceID := h.createUtterance("weather-sao-paulo")
	now := time.Now().UnixMilli()
	h.postSegment(utteranceID, 0, "what's the weather like", now-1500)
	h.postSegment(utteranceID, 1, "in São Paulo", now)
	if status, answer := h.call("POST", "/api/voice/utterances/"+utteranceID+"/finish", map[string]any{"ended_at_ms": now}, testPrincipal); status != 200 {
		t.Fatalf("finish = %d %v", status, answer)
	}
	events := h.followUntilDone(utteranceID)
	h.pool.WaitForFills()

	_, ids, sent, stopped := h.bridge.snapshot()
	earlier := sessionByText(t, ids, sent, "what's the weather like")
	joined := sessionByText(t, ids, sent, "what's the weather like in São Paulo")
	alone := sessionByText(t, ids, sent, "in São Paulo")
	roles := decisionsOf(events)
	if roles[joined] != RoleKept || roles[earlier] != RoleSuperseded || roles[alone] != RoleSuperseded {
		t.Fatalf("decisions = %v; want joined %s kept, earlier %s and alone %s superseded", roles, joined, earlier, alone)
	}
	if !contains(stopped, earlier) || !contains(stopped, alone) || contains(stopped, joined) {
		t.Fatalf("stopped = %v; want %s and %s stopped and %s running", stopped, earlier, alone, joined)
	}
	if earlier != ids[0] {
		t.Fatalf("segment 0 went to %s, not the pooled session %s", earlier, ids[0])
	}

	status, record := h.call("GET", "/api/voice/utterances/"+utteranceID, nil, testPrincipal)
	if status != 200 {
		t.Fatalf("get = %d", status)
	}
	sessions := record["sessions"].([]any)
	var sawPooled bool
	for _, raw := range sessions {
		session := raw.(map[string]any)
		if session["session_id"] == earlier {
			sawPooled = session["pooled"] == true
		}
		if session["session_id"] == joined && session["result_at_ms"] == nil {
			t.Fatalf("the kept session's result time was not recorded: %v", session)
		}
	}
	if !sawPooled {
		t.Fatalf("the record does not say segment 0 used a pooled session: %v", sessions)
	}
	if record["finalized_at_ms"] == nil {
		t.Fatalf("finalized_at_ms not set after finish: %v", record)
	}
	// No session event of a superseded session may follow its decision.
	supersededAt := map[string]int{}
	for position, event := range events {
		if event.name == eventDecision && event.data["role"] == RoleSuperseded {
			supersededAt[event.data["session_id"].(string)] = position
		}
	}
	for position, event := range events {
		if event.name != eventSessionEvent {
			continue
		}
		if at, ok := supersededAt[event.data["session_id"].(string)]; ok && position > at {
			t.Fatalf("event %d passes on a session_event of %s after it was superseded at %d", position, event.data["session_id"], at)
		}
	}
}

func TestASeparateSegmentKeepsTheEarlierSessionAndStartsAnotherBesideIt(t *testing.T) {
	h := newHarness(t, nil)
	utteranceID := h.createUtterance("weather-burger")
	now := time.Now().UnixMilli()
	h.postSegment(utteranceID, 0, "what's the weather like", now-2000)
	h.postSegment(utteranceID, 1, "can you add cheese to my burger", now)
	h.call("POST", "/api/voice/utterances/"+utteranceID+"/finish", map[string]any{"ended_at_ms": now}, testPrincipal)
	events := h.followUntilDone(utteranceID)

	_, ids, sent, stopped := h.bridge.snapshot()
	earlier := sessionByText(t, ids, sent, "what's the weather like")
	joined := sessionByText(t, ids, sent, "what's the weather like can you add cheese to my burger")
	burger := sessionByText(t, ids, sent, "can you add cheese to my burger")
	roles := decisionsOf(events)
	if roles[earlier] != RoleKept || roles[burger] != RoleSeparate || roles[joined] != RoleSuperseded {
		t.Fatalf("decisions = %v; want earlier %s kept, burger %s separate, joined %s superseded", roles, earlier, burger, joined)
	}
	if len(stopped) != 1 || stopped[0] != joined {
		t.Fatalf("stopped = %v; want only the joined session %s", stopped, joined)
	}
	results := 0
	for _, event := range events {
		if event.name == eventSessionEvent {
			if inner := event.data["event"].(map[string]any); inner["type"] == "result" {
				results++
			}
		}
	}
	if results < 2 {
		t.Fatalf("want the results of both kept sessions passed on, got %d", results)
	}
}

func TestARepeatedSegmentIndexAndARepeatedUtteranceAreIgnored(t *testing.T) {
	h := newHarness(t, nil)
	utteranceID := h.createUtterance("repeat-me")
	status, answer := h.call("POST", "/api/voice/utterances", map[string]any{"client_utterance_id": "repeat-me", "mode": "quick", "strategy": "endpoint",
		"trigger": "tile", "speech_started_at_ms": time.Now().UnixMilli()}, testPrincipal)
	if status != http.StatusOK || answer["utterance_id"] != utteranceID {
		t.Fatalf("repeated create = %d %v; want 200 with %s", status, answer, utteranceID)
	}
	now := time.Now().UnixMilli()
	if got := h.postSegment(utteranceID, 0, "turn on the lights", now); got != http.StatusAccepted {
		t.Fatalf("first segment 0 = %d, want 202", got)
	}
	if got := h.postSegment(utteranceID, 0, "turn on the lights", now); got != http.StatusOK {
		t.Fatalf("repeated segment 0 = %d, want 200", got)
	}
	h.call("POST", "/api/voice/utterances/"+utteranceID+"/finish", map[string]any{"ended_at_ms": now}, testPrincipal)
	h.followUntilDone(utteranceID)
	created, _, sent, _ := h.bridge.snapshot()
	if len(created) != 1 || len(sent) != 1 {
		t.Fatalf("created %d sessions and sent to %d; want one of each", len(created), len(sent))
	}
}

func TestCancelStopsEverySessionTheUtteranceStarted(t *testing.T) {
	h := newHarness(t, nil)
	h.bridge.resultWait = time.Hour
	utteranceID := h.createUtterance("cancel-me")
	now := time.Now().UnixMilli()
	h.postSegment(utteranceID, 0, "what's the weather like", now-1000)
	h.postSegment(utteranceID, 1, "in São Paulo", now)
	// Wait for the three sessions of segment 1 to be decided.
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, record := h.call("GET", "/api/voice/utterances/"+utteranceID, nil, testPrincipal)
		if decisions, _ := record["decisions"].([]any); len(decisions) >= 3 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("segment 1 was never decided: %v", record)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if status, answer := h.call("POST", "/api/voice/utterances/"+utteranceID+"/cancel", nil, testPrincipal); status != 200 {
		t.Fatalf("cancel = %d %v", status, answer)
	}
	h.followUntilDone(utteranceID)
	_, ids, _, stopped := h.bridge.snapshot()
	for _, id := range ids {
		if !contains(stopped, id) {
			t.Fatalf("session %s was not stopped by cancel; stopped %v", id, stopped)
		}
	}
	if status, _ := h.call("POST", "/api/voice/utterances/"+utteranceID+"/segments",
		map[string]any{"index": 2, "text": "more", "speech_ended_at_ms": now}, testPrincipal); status != http.StatusConflict {
		t.Fatalf("segment after cancel = %d, want 409", status)
	}
}

func TestASegmentPastTheContinuationWindowIsSeparateWithoutAskingTheJudge(t *testing.T) {
	h := newHarness(t, map[string]string{"VOICE_INTAKE_CONTINUATION_WINDOW_MS": "2000"})
	utteranceID := h.createUtterance("late-segment")
	now := time.Now().UnixMilli()
	h.postSegment(utteranceID, 0, "what's the weather like", now-9000)
	h.postSegment(utteranceID, 1, "in São Paulo", now)
	h.call("POST", "/api/voice/utterances/"+utteranceID+"/finish", map[string]any{"ended_at_ms": now}, testPrincipal)
	events := h.followUntilDone(utteranceID)
	_, ids, sent, stopped := h.bridge.snapshot()
	if len(ids) != 2 || len(stopped) != 0 {
		t.Fatalf("created %v and stopped %v; want two sessions and none stopped", ids, stopped)
	}
	roles := decisionsOf(events)
	if roles[sessionByText(t, ids, sent, "what's the weather like")] != RoleKept || roles[sessionByText(t, ids, sent, "in São Paulo")] != RoleSeparate {
		t.Fatalf("decisions = %v", roles)
	}
	if len(h.judge.asked) != 0 {
		t.Fatalf("the judge was asked %v for a segment past the window", h.judge.asked)
	}
}

func TestTheWindowPassingMakesTheDecisionsFinalWithoutFinish(t *testing.T) {
	h := newHarness(t, map[string]string{"VOICE_INTAKE_CONTINUATION_WINDOW_MS": "100"})
	utteranceID := h.createUtterance("no-finish")
	h.postSegment(utteranceID, 0, "what time is it", time.Now().UnixMilli())
	events := h.followUntilDone(utteranceID)
	_, ids, _, _ := h.bridge.snapshot()
	if roles := decisionsOf(events); len(ids) != 1 || roles[ids[0]] != RoleKept {
		t.Fatalf("decisions = %v for sessions %v; want the one session kept", roles, ids)
	}
}

func TestAnEffortRaisingPhraseStartsAFreshSessionAtTheRaisedEffort(t *testing.T) {
	h := newHarness(t, map[string]string{"VOICE_INTAKE_POOL_SIZE": "1"})
	h.pool.Fill(testPrincipal)
	h.pool.WaitForFills()
	utteranceID := h.createUtterance("think-hard")
	now := time.Now().UnixMilli()
	h.postSegment(utteranceID, 0, "Think hard about the trip budget", now)
	h.call("POST", "/api/voice/utterances/"+utteranceID+"/finish", map[string]any{"ended_at_ms": now}, testPrincipal)
	h.followUntilDone(utteranceID)
	h.pool.WaitForFills()
	created, ids, sent, _ := h.bridge.snapshot()
	sessionID := sessionByText(t, ids, sent, "Think hard about the trip budget")
	for index, id := range ids {
		if id == sessionID && created[index].Effort != "high" {
			t.Fatalf("session %s started at effort %q, want high", id, created[index].Effort)
		}
	}
	if sessionID == ids[0] {
		t.Fatalf("the raised-effort text went to the pooled session %s, which runs at the default effort", ids[0])
	}
}

func TestAudioIsSharedIntoTheSessionsTheUtteranceKeeps(t *testing.T) {
	h := newHarness(t, nil)
	utteranceID := h.createUtterance("with-audio")
	now := time.Now().UnixMilli()
	h.postSegment(utteranceID, 0, "listen to this song", now)
	h.call("POST", "/api/voice/utterances/"+utteranceID+"/finish", map[string]any{"ended_at_ms": now}, testPrincipal)
	h.followUntilDone(utteranceID)
	request, _ := http.NewRequest("POST", h.server.URL+"/api/voice/utterances/"+utteranceID+"/audio?filename="+utteranceID+".wav", strings.NewReader("RIFF fake"))
	request.Header.Set("Content-Type", "audio/wav")
	request.Header.Set("X-Principal-Id", testPrincipal)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != 200 {
		t.Fatalf("audio = %d", response.StatusCode)
	}
	_, ids, _, _ := h.bridge.snapshot()
	h.bridge.mutex.Lock()
	defer h.bridge.mutex.Unlock()
	if got := h.bridge.shared[ids[0]]; len(got) != 1 || got[0] != utteranceID+".wav" {
		t.Fatalf("shared into %s: %v", ids[0], got)
	}
}

func TestOnlyALoopbackCallerNamingAPrincipalReachesItsOwnUtterances(t *testing.T) {
	h := newHarness(t, nil)
	utteranceID := h.createUtterance("mine")
	if status, _ := h.call("GET", "/api/voice/utterances/"+utteranceID, nil, ""); status != http.StatusUnauthorized {
		t.Fatalf("no principal = %d, want 401", status)
	}
	if status, _ := h.call("GET", "/api/voice/utterances/"+utteranceID, nil, "principal_000002"); status != http.StatusNotFound {
		t.Fatalf("another principal = %d, want 404", status)
	}
}

func TestSettingsPatchRefusesAnUnknownFieldAndAppliesKnownOnes(t *testing.T) {
	h := newHarness(t, nil)
	if status, _ := h.call("PATCH", "/api/voice/settings", map[string]any{"pool_sise": 2}, testPrincipal); status != http.StatusBadRequest {
		t.Fatalf("unknown field = %d, want 400", status)
	}
	if status, _ := h.call("PATCH", "/api/voice/settings", map[string]any{"pool_size": "two"}, testPrincipal); status != http.StatusBadRequest {
		t.Fatalf("wrong type = %d, want 400", status)
	}
	status, answer := h.call("PATCH", "/api/voice/settings", map[string]any{"eager_silence_ms": 400, "effort_raising_phrases": []string{"ultrathink"}}, testPrincipal)
	if status != 200 || answer["eager_silence_ms"] != float64(400) {
		t.Fatalf("patch = %d %v", status, answer)
	}
	_, answer = h.call("GET", "/api/voice/settings", nil, testPrincipal)
	if phrases, _ := answer["effort_raising_phrases"].([]any); len(phrases) != 1 || phrases[0] != "ultrathink" || answer["default_effort"] != "low" {
		t.Fatalf("settings = %v", answer)
	}
	for _, key := range voiceSettingKeys {
		if _, ok := answer[key]; !ok {
			t.Fatalf("GET /api/voice/settings lacks %s: %v", key, answer)
		}
	}
}

func TestLatencyMediansGroupDispatchedSessions(t *testing.T) {
	h := newHarness(t, nil)
	utteranceID := h.createUtterance("latency")
	now := time.Now().UnixMilli()
	h.postSegment(utteranceID, 0, "what's the weather like", now)
	h.call("POST", "/api/voice/utterances/"+utteranceID+"/finish", map[string]any{"ended_at_ms": now}, testPrincipal)
	h.followUntilDone(utteranceID)
	for _, groupBy := range []string{"strategy", "pooled", "trigger"} {
		status, answer := h.call("GET", "/api/voice/latency?group_by="+groupBy, nil, testPrincipal)
		groups, _ := answer["groups"].([]any)
		if status != 200 || len(groups) != 1 {
			t.Fatalf("latency by %s = %d %v", groupBy, status, answer)
		}
		medians := groups[0].(map[string]any)["median_ms"].(map[string]any)
		if medians["dispatched_to_result"] == nil {
			t.Fatalf("latency by %s has no dispatched_to_result median: %v", groupBy, medians)
		}
	}
	if status, _ := h.call("GET", "/api/voice/latency?group_by=model", nil, testPrincipal); status != http.StatusBadRequest {
		t.Fatalf("unknown group_by = %d, want 400", status)
	}
}
