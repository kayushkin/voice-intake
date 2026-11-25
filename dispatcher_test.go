package voiceintake

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
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
	// turns counts the messages sent to each session; interruptedTurns is
	// the last turn of each that was interrupted, whose result never comes.
	turns            map[string]int
	interruptedTurns map[string]int
	interrupted      []string
	// interruptEndDelay is how long an interrupted turn takes to end.
	interruptEndDelay time.Duration

	// The person's own sessions and open signals, for target tests.
	sessionOwners map[string]string
	openSignals   []bridgeSignal
	personSent    map[string][]string
	answers       []map[string]string
	answerVia     string
	chatCreated   []string

	// states holds a session's bridge state where a test sets one; any
	// other session reads idle.
	states map[string]string
}

func newFakeBridge() *fakeBridge {
	return &fakeBridge{sent: map[string][]string{}, renamed: map[string]string{}, shared: map[string][]string{},
		streams: map[string]chan BridgeEvent{}, resultWait: 20 * time.Millisecond, turns: map[string]int{}, interruptedTurns: map[string]int{}, interruptEndDelay: 30 * time.Millisecond,
		sessionOwners: map[string]string{}, personSent: map[string][]string{}, answerVia: "message", states: map[string]string{}}
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

// fakeEvent builds a bridge event as llm-bridge-server shapes it: message_id
// names the assistant message a block or result belongs to, and a
// user_message carries its text under result.
func fakeEvent(eventType, sessionID, messageID, text string) BridgeEvent {
	data, _ := json.Marshal(map[string]any{"type": eventType, "bridge_session_id": sessionID, "message_id": messageID, "result": map[string]any{"text": text}})
	return BridgeEvent{Type: eventType, Data: data}
}

// SendMessage answers as the bridge does: its own echo of the message at
// once, the harness's echo after it, a block, and after resultWait the
// result, unless the turn was interrupted first.
func (b *fakeBridge) SendMessage(ctx context.Context, sessionID, text string) error {
	b.mutex.Lock()
	b.sent[sessionID] = append(b.sent[sessionID], text)
	wait := b.resultWait
	b.turns[sessionID]++
	turn := b.turns[sessionID]
	b.mutex.Unlock()
	events := b.stream(sessionID)
	messageID := fmt.Sprintf("msg_%s_%d", sessionID, turn)
	go func() {
		events <- fakeEvent("user_message", sessionID, "", text)
		events <- fakeEvent("user_message", sessionID, "", text)
		events <- fakeEvent("block", sessionID, messageID, "")
		time.Sleep(wait)
		b.mutex.Lock()
		interrupted := b.interruptedTurns[sessionID] >= turn
		b.mutex.Unlock()
		if interrupted {
			return
		}
		events <- fakeEvent("result", sessionID, messageID, "answer to "+text)
	}()
	return nil
}

// InterruptSession stops the turn the session is running. As the real
// bridge does, the stopped turn then ends by itself, interruptEndDelay
// later, with one more block and an empty result that carry its message
// id: the reply the turn would have given never comes.
func (b *fakeBridge) InterruptSession(ctx context.Context, sessionID string) error {
	b.mutex.Lock()
	b.interrupted = append(b.interrupted, sessionID)
	turn := b.turns[sessionID]
	b.interruptedTurns[sessionID] = turn
	delay := b.interruptEndDelay
	b.mutex.Unlock()
	events := b.stream(sessionID)
	messageID := fmt.Sprintf("msg_%s_%d", sessionID, turn)
	go func() {
		time.Sleep(delay)
		events <- fakeEvent("block", sessionID, messageID, "")
		events <- fakeEvent("result", sessionID, messageID, "")
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
	b.mutex.Lock()
	defer b.mutex.Unlock()
	if state, set := b.states[sessionID]; set {
		return state, nil
	}
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

// fakeAddresseeJudge says the words are for the assistant unless they hold
// one of its notForAssistant phrases, after a delay like a real model's.
type fakeAddresseeJudge struct {
	mutex           sync.Mutex
	notForAssistant []string
	asked           []string
	delay           time.Duration
	failure         error
}

func (j *fakeAddresseeJudge) Judge(ctx context.Context, modelRole, text string) (AddresseeVerdict, error) {
	time.Sleep(j.delay)
	j.mutex.Lock()
	defer j.mutex.Unlock()
	j.asked = append(j.asked, text)
	if j.failure != nil {
		return AddresseeVerdict{}, j.failure
	}
	for _, phrase := range j.notForAssistant {
		if strings.Contains(text, phrase) {
			return AddresseeVerdict{ForAssistant: false, Reason: "talk with someone else", Model: "fake-model"}, nil
		}
	}
	return AddresseeVerdict{ForAssistant: true, Reason: "a request", Model: "fake-model"}, nil
}

func (j *fakeAddresseeJudge) askedTexts() []string {
	j.mutex.Lock()
	defer j.mutex.Unlock()
	return append([]string(nil), j.asked...)
}

func (b *fakeBridge) interruptedSessions() []string {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return append([]string(nil), b.interrupted...)
}

func (b *fakeBridge) archivedSessions() []string {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return append([]string(nil), b.archived...)
}

type harness struct {
	t        *testing.T
	bridge   *fakeBridge
	judge    *fakeAddresseeJudge
	pool     *SessionPool
	store    *Store
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
	judge := &fakeAddresseeJudge{notForAssistant: []string{"they went over it yesterday"}, delay: 30 * time.Millisecond}
	settings := func() DispatchSettings { return DispatchSettingsFrom(registry) }
	_, principalStoreAddress := newFakePrincipalStore(t, knownTestPrincipals())
	principals := NewPrincipalStoreVerifier(principalStoreAddress, time.Now)
	pool := NewSessionPool(store, bridge, settings, principals, time.Now)
	dispatcher := NewDispatcher(store, bridge, judge, pool, settings, time.Now, directory+"/audio")
	detector := newFakeDetector()
	transcribers := Transcribers{"fake": transcriber}
	for name, more := range extra {
		transcribers[name] = more
	}
	dispatcher.SetSpeechServices(detector, transcribers)
	dispatcher.SetReplayRecordingDirectory(directory + "/replay-recordings")
	server := NewServer(store, dispatcher, pool, registry, transcribers, principals, time.Now)
	registry.OnChange(func(string) { pool.Resync(context.Background()) })
	httpServer := httptest.NewServer(server.Handler())
	t.Cleanup(httpServer.Close)
	return &harness{t: t, bridge: bridge, judge: judge, pool: pool, store: store, server: httpServer, detector: detector}
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

// sessionEventTypes lists the types of the session events passed on, in
// order.
func sessionEventTypes(events []receivedEvent) []string {
	var types []string
	for _, event := range events {
		if event.name == eventSessionEvent {
			types = append(types, event.data["event"].(map[string]any)["type"].(string))
		}
	}
	return types
}

func countOf(list []string, item string) int {
	count := 0
	for _, element := range list {
		if element == item {
			count++
		}
	}
	return count
}

// waitFor polls until condition holds, or fails the test with what.
func waitFor(t *testing.T, what string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestALaterSegmentGoesIntoTheSameSessionAsItsNextMessage(t *testing.T) {
	h := newHarness(t, map[string]string{"VOICE_INTAKE_POOL_SIZE": "1"})
	// Let the pool start its session before the first utterance takes it.
	h.pool.Fill(testPrincipal)
	h.pool.WaitForFills()

	utteranceID := h.createUtterance("weather-sao-paulo")
	now := time.Now().UnixMilli()
	h.postSegment(utteranceID, 0, "what's the weather like", now-1500)
	// The first reply is out before the person speaks again.
	waitFor(t, "the first turn's result", func() bool {
		_, record := h.call("GET", "/api/voice/utterances/"+utteranceID, nil, testPrincipal)
		sessions, _ := record["sessions"].([]any)
		return len(sessions) == 1 && sessions[0].(map[string]any)["result_at_ms"] != nil
	})
	h.postSegment(utteranceID, 1, "in São Paulo", now)
	if status, answer := h.call("POST", "/api/voice/utterances/"+utteranceID+"/finish", map[string]any{"ended_at_ms": now}, testPrincipal); status != 200 {
		t.Fatalf("finish = %d %v", status, answer)
	}
	events := h.followUntilDone(utteranceID)
	h.pool.WaitForFills()

	_, ids, sent, stopped := h.bridge.snapshot()
	conversation := ids[0]
	if got := sent[conversation]; len(got) != 2 || got[0] != "what's the weather like" || got[1] != "in São Paulo" {
		t.Fatalf("the pooled session %s was sent %v; want the two segments as two messages", conversation, got)
	}
	for _, id := range ids[1:] {
		if len(sent[id]) != 0 {
			t.Fatalf("a second session %s was sent %v: one press is one session", id, sent[id])
		}
	}
	if roles := decisionsOf(events); len(roles) != 1 || roles[conversation] != RoleKept {
		t.Fatalf("decisions = %v; want only %s, kept", roles, conversation)
	}
	if len(stopped) != 0 || len(h.bridge.interruptedSessions()) != 0 {
		t.Fatalf("stopped %v, interrupted %v; want neither: the first turn had finished", stopped, h.bridge.interruptedSessions())
	}
	if results := countOf(sessionEventTypes(events), "result"); results != 2 {
		t.Fatalf("%d results passed on, want both turns': %v", results, sessionEventTypes(events))
	}
	followUps := eventsNamed(events, eventFollowUp)
	if len(followUps) != 1 || followUps[0].data["text"] != "in São Paulo" || followUps[0].data["interrupted_reply"] != false {
		t.Fatalf("follow_up events = %v", followUps)
	}

	_, record := h.call("GET", "/api/voice/utterances/"+utteranceID, nil, testPrincipal)
	sessions := record["sessions"].([]any)
	if len(sessions) != 1 {
		t.Fatalf("the record holds %d sessions, want one: %v", len(sessions), sessions)
	}
	session := sessions[0].(map[string]any)
	if session["pooled"] != true || session["reply_text"] != "answer to what's the weather like" {
		t.Fatalf("the session's own row should be the pooled first turn: %v", session)
	}
	recorded := session["follow_ups"].([]any)
	if len(recorded) != 1 {
		t.Fatalf("follow_ups = %v", recorded)
	}
	followUp := recorded[0].(map[string]any)
	if followUp["text"] != "in São Paulo" || followUp["result_at_ms"] == nil || followUp["reply_text"] != "answer to in São Paulo" || followUp["interrupted_reply"] != false {
		t.Fatalf("the follow-up's turn was not recorded: %v", followUp)
	}
	if record["finalized_at_ms"] == nil {
		t.Fatalf("finalized_at_ms not set after finish: %v", record)
	}
}

func TestALaterSegmentInterruptsAReplyStillRunningAndItsResultIsNeverPassedOn(t *testing.T) {
	h := newHarness(t, nil)
	h.bridge.resultWait = 300 * time.Millisecond
	utteranceID := h.createUtterance("weather-interrupted")
	now := time.Now().UnixMilli()
	h.postSegment(utteranceID, 0, "what's the weather like", now-1000)
	h.postSegment(utteranceID, 1, "in São Paulo", now)
	h.call("POST", "/api/voice/utterances/"+utteranceID+"/finish", map[string]any{"ended_at_ms": now}, testPrincipal)
	events := h.followUntilDone(utteranceID)

	_, ids, sent, stopped := h.bridge.snapshot()
	if len(ids) != 1 || len(sent[ids[0]]) != 2 || len(stopped) != 0 {
		t.Fatalf("sessions %v sent %v stopped %v; want one session with two messages, still running", ids, sent, stopped)
	}
	if interrupted := h.bridge.interruptedSessions(); len(interrupted) != 1 || interrupted[0] != ids[0] {
		t.Fatalf("interrupted = %v; want the running turn of %s interrupted once", interrupted, ids[0])
	}
	if results := countOf(sessionEventTypes(events), "result"); results != 1 {
		t.Fatalf("%d results passed on, want only the second turn's: %v", results, sessionEventTypes(events))
	}
	followUps := eventsNamed(events, eventFollowUp)
	if len(followUps) != 1 || followUps[0].data["interrupted_reply"] != true {
		t.Fatalf("follow_up events = %v; want one that says it interrupted the reply", followUps)
	}
	_, record := h.call("GET", "/api/voice/utterances/"+utteranceID, nil, testPrincipal)
	session := record["sessions"].([]any)[0].(map[string]any)
	if session["result_at_ms"] != nil {
		t.Fatalf("the interrupted first turn has a result time: %v", session)
	}
	if followUp := session["follow_ups"].([]any)[0].(map[string]any); followUp["interrupted_reply"] != true || followUp["result_at_ms"] == nil {
		t.Fatalf("follow-up = %v", followUp)
	}
}

func TestAnInterruptedTurnThatEndsLateIsStillNeverPassedOn(t *testing.T) {
	defer func(wait time.Duration) { interruptedTurnEndWait = wait }(interruptedTurnEndWait)
	interruptedTurnEndWait = 20 * time.Millisecond
	h := newHarness(t, nil)
	h.bridge.resultWait = 300 * time.Millisecond
	// The interrupted turn ends only after the next message has been sent
	// and echoed: its block and result are known by their message id.
	h.bridge.interruptEndDelay = 120 * time.Millisecond
	utteranceID := h.createUtterance("weather-late-interrupt-end")
	now := time.Now().UnixMilli()
	h.postSegment(utteranceID, 0, "what's the weather like", now-1000)
	waitFor(t, "the first turn's block", func() bool {
		_, record := h.call("GET", "/api/voice/utterances/"+utteranceID, nil, testPrincipal)
		sessions, _ := record["sessions"].([]any)
		return len(sessions) == 1 && sessions[0].(map[string]any)["first_block_at_ms"] != nil
	})
	h.postSegment(utteranceID, 1, "in São Paulo", now)
	h.call("POST", "/api/voice/utterances/"+utteranceID+"/finish", map[string]any{"ended_at_ms": now}, testPrincipal)
	events := h.followUntilDone(utteranceID)
	var replies []string
	for _, event := range events {
		if event.name != eventSessionEvent {
			continue
		}
		if inner := event.data["event"].(map[string]any); inner["type"] == "result" {
			replies = append(replies, inner["result"].(map[string]any)["text"].(string))
		}
	}
	if len(replies) != 1 || replies[0] != "answer to in São Paulo" {
		t.Fatalf("results passed on = %q; want only the reply to the second message", replies)
	}
}

func TestWordsNotMeantForTheAssistantAreWithheldAndTheSessionArchivedWhenNobodySendsThem(t *testing.T) {
	defer func(limit time.Duration) { confirmationLimit = limit }(confirmationLimit)
	confirmationLimit = 200 * time.Millisecond
	h := newHarness(t, nil)
	h.bridge.resultWait = time.Hour
	utteranceID := h.createUtterance("overheard")
	now := time.Now().UnixMilli()
	h.postSegment(utteranceID, 0, "yeah they went over it yesterday", now)
	events := h.followUntilDone(utteranceID)

	_, ids, sent, stopped := h.bridge.snapshot()
	if len(ids) != 1 || len(sent[ids[0]]) != 1 {
		t.Fatalf("sessions %v sent %v; want the one session the first words started", ids, sent)
	}
	withheld := eventsNamed(events, eventNotForAssistant)
	if len(withheld) != 1 || withheld[0].data["text"] != "yeah they went over it yesterday" || withheld[0].data["reason"] != "talk with someone else" || withheld[0].data["model"] != "fake-model" {
		t.Fatalf("not_for_assistant events = %v", withheld)
	}
	if len(eventsNamed(events, eventAwaitingConfirmation)) != 0 {
		t.Fatal("withheld words went out as awaiting_confirmation, which a phone may send by a countdown")
	}
	if ended := eventsNamed(events, eventConfirmationEnded); len(ended) != 1 || ended[0].data["state"] != ConfirmationExpired {
		t.Fatalf("confirmation_ended = %v, want expired", ended)
	}
	if interrupted := h.bridge.interruptedSessions(); len(interrupted) != 1 {
		t.Fatalf("interrupted = %v; want the session's running turn interrupted", interrupted)
	}
	if !contains(stopped, ids[0]) || !contains(h.bridge.archivedSessions(), ids[0]) {
		t.Fatalf("stopped %v archived %v; want %s stopped and archived once nobody sent the words", stopped, h.bridge.archivedSessions(), ids[0])
	}
	if roles := decisionsOf(events); roles[ids[0]] != RoleSuperseded {
		t.Fatalf("decisions = %v; want the session superseded at the end", roles)
	}
	_, record := h.call("GET", "/api/voice/utterances/"+utteranceID, nil, testPrincipal)
	if record["confirmation_reason"] != ConfirmationReasonNotForAssistant || record["confirmation_state"] != ConfirmationExpired {
		t.Fatalf("record confirmation = %v / %v", record["confirmation_reason"], record["confirmation_state"])
	}
	checks := record["addressee_checks"].([]any)
	if len(checks) != 1 || checks[0].(map[string]any)["for_assistant"] != false {
		t.Fatalf("addressee_checks = %v", checks)
	}
}

func TestWithheldWordsThePersonSendsAnywayGoAgainIntoTheSameSession(t *testing.T) {
	h := newHarness(t, nil)
	h.bridge.resultWait = 150 * time.Millisecond
	utteranceID := h.createUtterance("overheard-but-meant")
	now := time.Now().UnixMilli()
	h.postSegment(utteranceID, 0, "yeah they went over it yesterday", now)
	h.followUntil(utteranceID, eventNotForAssistant)
	// A segment that arrives after the words were withheld is not sent.
	if status, answer := h.call("POST", "/api/voice/utterances/"+utteranceID+"/confirm", map[string]any{"text": "yeah they went over it yesterday"}, testPrincipal); status != 200 {
		t.Fatalf("confirm = %d %v", status, answer)
	}
	events := h.followUntilDone(utteranceID)
	_, ids, sent, stopped := h.bridge.snapshot()
	if len(ids) != 1 || len(stopped) != 0 {
		t.Fatalf("sessions %v stopped %v; want the one session, still running", ids, stopped)
	}
	if got := sent[ids[0]]; len(got) != 2 || got[1] != "yeah they went over it yesterday" {
		t.Fatalf("the session was sent %v; want the words again after the person sent them anyway", got)
	}
	if results := countOf(sessionEventTypes(events), "result"); results != 1 {
		t.Fatalf("%d results passed on, want the one reply to the words sent anyway: %v", results, sessionEventTypes(events))
	}
	if ended := eventsNamed(events, eventConfirmationEnded); len(ended) != 1 || ended[0].data["state"] != ConfirmationConfirmed {
		t.Fatalf("confirmation_ended = %v, want confirmed", ended)
	}
	if asked := h.judge.askedTexts(); len(asked) != 1 {
		t.Fatalf("the addressee check was asked %v; words the person sent anyway are not checked again", asked)
	}
}

func TestAFailedAddresseeCheckIsReportedAndHoldsNothingBack(t *testing.T) {
	h := newHarness(t, nil)
	h.judge.failure = errors.New("every model of the role failed")
	utteranceID := h.createUtterance("check-down")
	now := time.Now().UnixMilli()
	h.postSegment(utteranceID, 0, "what time is it", now)
	// The check runs beside the session: wait for its failure to be
	// recorded before the utterance ends.
	waitFor(t, "the failed check's record", func() bool {
		_, record := h.call("GET", "/api/voice/utterances/"+utteranceID, nil, testPrincipal)
		checks, _ := record["addressee_checks"].([]any)
		return len(checks) == 1
	})
	h.call("POST", "/api/voice/utterances/"+utteranceID+"/finish", map[string]any{"ended_at_ms": now}, testPrincipal)
	events := h.followUntilDone(utteranceID)
	if results := countOf(sessionEventTypes(events), "result"); results != 1 {
		t.Fatalf("%d results passed on, want the reply: a failed check holds nothing back", results)
	}
	problems := eventsNamed(events, eventError)
	if len(problems) != 1 || !strings.Contains(problems[0].data["message"].(string), "did not run") {
		t.Fatalf("error events = %v; want one saying the check did not run", problems)
	}
	_, record := h.call("GET", "/api/voice/utterances/"+utteranceID, nil, testPrincipal)
	check := record["addressee_checks"].([]any)[0].(map[string]any)
	if check["for_assistant"] != nil || check["error"] != "every model of the role failed" {
		t.Fatalf("addressee check = %v", check)
	}
}

func TestSegmentsThePhoneCutsAreNotSentPastTheLimitOfOneQuickRecording(t *testing.T) {
	h := newHarness(t, map[string]string{"VOICE_INTAKE_MAXIMUM_QUICK_SEGMENTS": "2"})
	utteranceID := h.createUtterance("too-many-segments")
	now := time.Now().UnixMilli()
	for index, text := range []string{"one", "two", "three", "four"} {
		h.postSegment(utteranceID, index, text, now)
	}
	h.call("POST", "/api/voice/utterances/"+utteranceID+"/finish", map[string]any{"ended_at_ms": now}, testPrincipal)
	events := h.followUntilDone(utteranceID)
	_, ids, sent, _ := h.bridge.snapshot()
	if len(ids) != 1 || len(sent[ids[0]]) != 2 {
		t.Fatalf("sessions %v sent %v; want one session with the first two segments", ids, sent)
	}
	if problems := eventsNamed(events, eventError); len(problems) != 1 || !strings.Contains(problems[0].data["message"].(string), "at most 2 segments") {
		t.Fatalf("error events = %v; want the limit reported once", problems)
	}
}

func TestTheTriggerDetailIsStoredAndAnOverlongOneRefused(t *testing.T) {
	h := newHarness(t, nil)
	body := map[string]any{"client_utterance_id": "with-detail", "mode": "quick", "strategy": "endpoint", "trigger": "assistant",
		"trigger_detail": "show_flags=SHOW_SOURCE_ASSIST_GESTURE", "speech_started_at_ms": time.Now().UnixMilli()}
	status, answer := h.call("POST", "/api/voice/utterances", body, testPrincipal)
	if status != http.StatusCreated {
		t.Fatalf("create = %d %v", status, answer)
	}
	_, record := h.call("GET", "/api/voice/utterances/"+answer["utterance_id"].(string), nil, testPrincipal)
	if record["trigger_detail"] != "show_flags=SHOW_SOURCE_ASSIST_GESTURE" {
		t.Fatalf("trigger_detail = %v", record["trigger_detail"])
	}
	body["client_utterance_id"], body["trigger_detail"] = "detail-too-long", strings.Repeat("x", maximumTriggerDetailCharacters+1)
	if status, answer := h.call("POST", "/api/voice/utterances", body, testPrincipal); status != http.StatusBadRequest {
		t.Fatalf("an overlong trigger_detail = %d %v, want 400", status, answer)
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

// A typed question is an utterance with trigger keyboard and one text
// segment; it is dispatched like a spoken one and shows as its own group in
// the latency table.
func TestATypedQuestionIsDispatchedAndGroupedUnderTheKeyboardTrigger(t *testing.T) {
	h := newHarness(t, nil)
	now := time.Now().UnixMilli()
	status, answer := h.call("POST", "/api/voice/utterances", map[string]any{"client_utterance_id": "typed", "mode": "quick", "strategy": "manual",
		"trigger": "keyboard", "speech_started_at_ms": now}, testPrincipal)
	if status != http.StatusCreated {
		t.Fatalf("create typed utterance = %d %v; want 201", status, answer)
	}
	utteranceID := answer["utterance_id"].(string)
	h.postSegment(utteranceID, 0, "what time is it in Lisbon", now)
	h.call("POST", "/api/voice/utterances/"+utteranceID+"/finish", map[string]any{"ended_at_ms": now}, testPrincipal)
	events := h.followUntilDone(utteranceID)
	sawResult := false
	for _, event := range events {
		if event.name == eventSessionEvent {
			sawResult = true
		}
	}
	if !sawResult {
		t.Fatalf("typed question streamed no session event: %v", events)
	}
	_, latency := h.call("GET", "/api/voice/latency?group_by=trigger", nil, testPrincipal)
	groups, _ := latency["groups"].([]any)
	if len(groups) != 1 || groups[0].(map[string]any)["group"] != "keyboard" {
		t.Fatalf("latency by trigger = %v; want one group, keyboard", latency)
	}
}

// detail=record makes each row of the list the full record, segments and
// sessions with their timings, as GET /api/voice/utterances/{id} answers.
func TestTheListWithDetailRecordCarriesSegmentsAndSessions(t *testing.T) {
	h := newHarness(t, nil)
	utteranceID := h.createUtterance("listed")
	now := time.Now().UnixMilli()
	h.postSegment(utteranceID, 0, "what's on today", now)
	h.call("POST", "/api/voice/utterances/"+utteranceID+"/finish", map[string]any{"ended_at_ms": now}, testPrincipal)
	h.followUntilDone(utteranceID)

	listWithDetail := func(detail string) (int, []byte) {
		request, err := http.NewRequest("GET", h.server.URL+"/api/voice/utterances?limit=5&detail="+detail, nil)
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("X-Principal-Id", testPrincipal)
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		raw, _ := io.ReadAll(response.Body)
		return response.StatusCode, raw
	}
	status, raw := listWithDetail("record")
	var rows []map[string]any
	if err := json.Unmarshal(raw, &rows); status != 200 || err != nil || len(rows) != 1 {
		t.Fatalf("list with detail=record = %d %s", status, raw)
	}
	segments, _ := rows[0]["segments"].([]any)
	sessions, _ := rows[0]["sessions"].([]any)
	if len(segments) != 1 || len(sessions) != 1 || sessions[0].(map[string]any)["dispatched_at_ms"] == nil {
		t.Fatalf("row = %v; want one segment and one dispatched session", rows[0])
	}
	if status, _ := listWithDetail("everything"); status != http.StatusBadRequest {
		t.Fatalf("detail=everything = %d, want 400", status)
	}
}

func TestCancelStopsTheSessionTheUtteranceStarted(t *testing.T) {
	h := newHarness(t, nil)
	h.bridge.resultWait = time.Hour
	utteranceID := h.createUtterance("cancel-me")
	now := time.Now().UnixMilli()
	h.postSegment(utteranceID, 0, "what's the weather like", now-1000)
	h.postSegment(utteranceID, 1, "in São Paulo", now)
	// Wait for segment 1 to reach the session.
	waitFor(t, "segment 1 to be sent", func() bool {
		_, ids, sent, _ := h.bridge.snapshot()
		return len(ids) == 1 && len(sent[ids[0]]) == 2
	})
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

func TestASegmentPastTheContinuationWindowStillGoesIntoTheSameSession(t *testing.T) {
	h := newHarness(t, map[string]string{"VOICE_INTAKE_CONTINUATION_WINDOW_MS": "2000"})
	h.bridge.resultWait = 200 * time.Millisecond
	utteranceID := h.createUtterance("late-segment")
	now := time.Now().UnixMilli()
	h.postSegment(utteranceID, 0, "what's the weather like", now-9000)
	h.postSegment(utteranceID, 1, "in São Paulo", now)
	h.call("POST", "/api/voice/utterances/"+utteranceID+"/finish", map[string]any{"ended_at_ms": now}, testPrincipal)
	events := h.followUntilDone(utteranceID)
	_, ids, sent, stopped := h.bridge.snapshot()
	if len(ids) != 1 || len(sent[ids[0]]) != 2 || len(stopped) != 0 {
		t.Fatalf("sessions %v sent %v stopped %v; want one session with both segments", ids, sent, stopped)
	}
	if roles := decisionsOf(events); roles[ids[0]] != RoleKept {
		t.Fatalf("decisions = %v", roles)
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

// A pooled session the bridge's idle reaper stopped reads completed. It never
// had a turn, so the bridge cannot resume it: the utterance must go to a new
// session and the stopped one must be given up.
func TestAPooledSessionTheReaperStoppedIsGivenUpForANewSession(t *testing.T) {
	h := newHarness(t, map[string]string{"VOICE_INTAKE_POOL_SIZE": "1"})
	h.pool.Fill(testPrincipal)
	h.pool.WaitForFills()
	_, ids, _, _ := h.bridge.snapshot()
	if len(ids) != 1 {
		t.Fatalf("the pool started %d sessions, want 1", len(ids))
	}
	stoppedByReaper := ids[0]
	h.bridge.mutex.Lock()
	h.bridge.states[stoppedByReaper] = "completed"
	h.bridge.mutex.Unlock()

	utteranceID := h.createUtterance("stopped-pooled-session")
	now := time.Now().UnixMilli()
	h.postSegment(utteranceID, 0, "add milk to my grocery list", now)
	if status, answer := h.call("POST", "/api/voice/utterances/"+utteranceID+"/finish", map[string]any{"ended_at_ms": now}, testPrincipal); status != 200 {
		t.Fatalf("finish = %d %v", status, answer)
	}
	h.followUntilDone(utteranceID)
	h.pool.WaitForFills()

	_, ids, sent, stopped := h.bridge.snapshot()
	if len(sent[stoppedByReaper]) != 0 {
		t.Fatalf("the words went to the stopped pooled session %s: %v", stoppedByReaper, sent[stoppedByReaper])
	}
	answered := sessionByText(t, ids, sent, "add milk to my grocery list")
	if answered == stoppedByReaper {
		t.Fatalf("the utterance went to the stopped pooled session %s", stoppedByReaper)
	}
	if !contains(stopped, stoppedByReaper) {
		t.Fatalf("stopped = %v; want the stopped pooled session %s given up", stopped, stoppedByReaper)
	}
}

func (b *fakeBridge) sharedInto(sessionID string) []string {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return append([]string(nil), b.shared[sessionID]...)
}

func TestAPooledSessionTheReaperStoppedIsReplacedWhenARecordingStarts(t *testing.T) {
	h := newHarness(t, map[string]string{"VOICE_INTAKE_POOL_SIZE": "1"})
	h.pool.Fill(testPrincipal)
	h.pool.WaitForFills()
	_, ids, _, _ := h.bridge.snapshot()
	stoppedByReaper := ids[0]
	h.bridge.mutex.Lock()
	h.bridge.states[stoppedByReaper] = "completed"
	h.bridge.mutex.Unlock()

	// The recording starts; no segment has ended yet.
	utteranceID := h.createUtterance("recording-starts-after-a-quiet-spell")
	h.pool.WaitForFills()
	_, ids, _, stopped := h.bridge.snapshot()
	if !contains(stopped, stoppedByReaper) || len(ids) != 2 {
		t.Fatalf("sessions %v, stopped %v; want %s given up and its replacement started before the first segment", ids, stopped, stoppedByReaper)
	}
	now := time.Now().UnixMilli()
	h.postSegment(utteranceID, 0, "add milk to my grocery list", now)
	h.call("POST", "/api/voice/utterances/"+utteranceID+"/finish", map[string]any{"ended_at_ms": now}, testPrincipal)
	h.followUntilDone(utteranceID)
	h.pool.WaitForFills()
	_, _, sent, _ := h.bridge.snapshot()
	if got := sent[ids[1]]; len(got) != 1 || got[0] != "add milk to my grocery list" {
		t.Fatalf("the replacement %s was sent %v; want the first segment", ids[1], got)
	}
}
