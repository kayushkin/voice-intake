package voiceintake

import (
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"
)

func (h *harness) createTargetedUtterance(clientID string, target map[string]any) (int, map[string]any) {
	h.t.Helper()
	return h.call("POST", "/api/voice/utterances", map[string]any{"client_utterance_id": clientID, "mode": "quick", "strategy": "endpoint",
		"trigger": "widget", "speech_started_at_ms": time.Now().UnixMilli() - 3000, "target": target}, testPrincipal)
}

func (h *harness) finish(utteranceID string) {
	h.t.Helper()
	if status, answer := h.call("POST", "/api/voice/utterances/"+utteranceID+"/finish", map[string]any{"ended_at_ms": time.Now().UnixMilli()}, testPrincipal); status != http.StatusOK {
		h.t.Fatalf("finish = %d %v", status, answer)
	}
}

// resultTexts are the reply texts the phone was passed, in order.
func resultTexts(events []receivedEvent) []string {
	var texts []string
	for _, event := range eventsNamed(events, eventSessionEvent) {
		inner, _ := event.data["event"].(map[string]any)
		if inner["type"] != "result" {
			continue
		}
		result, _ := inner["result"].(map[string]any)
		texts = append(texts, result["text"].(string))
	}
	return texts
}

func TestATargetThatIsMalformedIsRefused(t *testing.T) {
	h := newHarness(t, nil)
	for name, target := range map[string]map[string]any{
		"unknown kind":            {"kind": "somewhere"},
		"session with no id":      {"kind": "session"},
		"signal with no id":       {"kind": "signal"},
		"pool naming a session":   {"kind": "pool", "session_id": "br_1"},
		"session naming a signal": {"kind": "session", "session_id": "br_1", "signal_id": "sig_1"},
		"unknown field":           {"kind": "pool", "sessionId": "br_1"},
	} {
		if status, answer := h.createTargetedUtterance("bad-"+name, target); status != http.StatusBadRequest {
			t.Errorf("%s: create = %d %v, want 400", name, status, answer)
		}
	}
	if status, answer := h.createTargetedUtterance("pool-explicit", map[string]any{"kind": "pool"}); status != http.StatusCreated {
		t.Fatalf("explicit pool target = %d %v", status, answer)
	}
}

func TestASessionTargetGetsOneMessageAfterFinishAndOnlyItsNewReply(t *testing.T) {
	h := newHarness(t, nil)
	h.bridge.ownSession("br_mine", testPrincipal)
	h.bridge.ownSession("br_theirs", "principal_000002")

	if status, answer := h.createTargetedUtterance("not-mine", map[string]any{"kind": "session", "session_id": "br_theirs"}); status != http.StatusNotFound {
		t.Fatalf("someone else's session = %d %v, want 404", status, answer)
	}
	status, answer := h.createTargetedUtterance("reply-1", map[string]any{"kind": "session", "session_id": "br_mine"})
	if status != http.StatusCreated {
		t.Fatalf("create = %d %v", status, answer)
	}
	utteranceID := answer["utterance_id"].(string)
	now := time.Now().UnixMilli()
	h.postSegment(utteranceID, 0, "what's the weather like", now-2000)
	h.postSegment(utteranceID, 1, "in São Paulo", now-500)
	time.Sleep(100 * time.Millisecond)
	if _, _, _, _ = h.bridge.snapshot(); len(h.bridge.personSent["br_mine"]) != 0 {
		t.Fatalf("sent before finish: %v", h.bridge.personSent)
	}
	h.finish(utteranceID)
	events := h.followUntilDone(utteranceID)

	created, _, sent, stopped := h.bridge.snapshot()
	if len(created) != 0 || len(sent) != 0 {
		t.Fatalf("a session target started sessions %v or sent through the pool path %v", created, sent)
	}
	if got := h.bridge.personSent["br_mine"]; len(got) != 1 || got[0] != "what's the weather like in São Paulo" {
		t.Fatalf("sent to the person's session = %v, want one joined message", got)
	}
	if len(h.judge.asked) != 0 {
		t.Fatalf("the judge was asked %v", h.judge.asked)
	}
	if replies := resultTexts(events); len(replies) != 1 || replies[0] != "answer to what's the weather like in São Paulo" {
		t.Fatalf("replies passed on = %v, want only the new one", replies)
	}
	if roles := decisionsOf(events); roles["br_mine"] != RoleKept || len(roles) != 1 {
		t.Fatalf("decisions = %v", roles)
	}
	if len(stopped) != 0 || len(h.bridge.archived) != 0 {
		t.Fatalf("the person's session was stopped %v or archived %v", stopped, h.bridge.archived)
	}
	_, record := h.call("GET", "/api/voice/utterances/"+utteranceID, nil, testPrincipal)
	target := record["target"].(map[string]any)
	if target["kind"] != TargetSession || target["session_id"] != "br_mine" {
		t.Fatalf("stored target = %v", target)
	}
}

func TestCancellingASessionTargetLeavesThePersonsSessionAlone(t *testing.T) {
	h := newHarness(t, nil)
	h.bridge.ownSession("br_mine", testPrincipal)
	_, answer := h.createTargetedUtterance("cancel-1", map[string]any{"kind": "session", "session_id": "br_mine"})
	utteranceID := answer["utterance_id"].(string)
	h.postSegment(utteranceID, 0, "never mind", time.Now().UnixMilli())
	if status, _ := h.call("POST", "/api/voice/utterances/"+utteranceID+"/cancel", nil, testPrincipal); status != http.StatusOK {
		t.Fatalf("cancel = %d", status)
	}
	h.followUntilDone(utteranceID)
	_, _, _, stopped := h.bridge.snapshot()
	if len(stopped) != 0 || len(h.bridge.personSent) != 0 {
		t.Fatalf("stopped %v, sent %v", stopped, h.bridge.personSent)
	}
}

func TestASignalTargetAnswersEveryQuestionOfItsGroupWithTheWords(t *testing.T) {
	h := newHarness(t, nil)
	h.bridge.ownSession("br_asking", testPrincipal)
	h.bridge.openSignals = []bridgeSignal{
		{ID: "sig_first", SessionID: "br_asking", Kind: "question", RequestID: "dqgroup_1", Title: "Which colour?", State: "open"},
		{ID: "sig_second", SessionID: "br_asking", Kind: "question", RequestID: "dqgroup_1", Title: "Which size?", State: "open"},
		{ID: "sig_note", SessionID: "br_asking", Kind: "notification", Title: "Done", State: "open"},
	}
	if status, answer := h.createTargetedUtterance("notification", map[string]any{"kind": "signal", "signal_id": "sig_note"}); status != http.StatusConflict {
		t.Fatalf("a notification as target = %d %v, want 409", status, answer)
	}
	if status, answer := h.createTargetedUtterance("missing", map[string]any{"kind": "signal", "signal_id": "sig_gone"}); status != http.StatusNotFound {
		t.Fatalf("a missing signal = %d %v, want 404", status, answer)
	}
	_, answer := h.createTargetedUtterance("answer-1", map[string]any{"kind": "signal", "signal_id": "sig_first"})
	utteranceID := answer["utterance_id"].(string)
	h.postSegment(utteranceID, 0, "blue, and large", time.Now().UnixMilli())
	h.finish(utteranceID)
	events := h.followUntilDone(utteranceID)

	if len(h.bridge.answers) != 1 {
		t.Fatalf("answers = %v", h.bridge.answers)
	}
	got := h.bridge.answers[0]
	if len(got) != 2 || got["sig_first"] != "blue, and large" || got["sig_second"] != "blue, and large" {
		t.Fatalf("answers posted = %v, want both questions of the group", got)
	}
	if replies := resultTexts(events); len(replies) != 1 || replies[0] != "answer to blue, and large" {
		t.Fatalf("replies = %v", replies)
	}
	dispatches := eventsNamed(events, eventDispatch)
	if len(dispatches) != 1 || dispatches[0].data["session_id"] != "br_asking" {
		t.Fatalf("dispatch events = %v", dispatches)
	}
}

func TestASignalAnsweredThroughAParkedHookPassesTheRunningTurnOn(t *testing.T) {
	h := newHarness(t, nil)
	// A parked question: the turn is still running, so no earlier result is replayed.
	h.bridge.sessionOwners["br_parked"] = testPrincipal
	h.bridge.answerVia = "parked_hook"
	h.bridge.openSignals = []bridgeSignal{{ID: "sig_parked", SessionID: "br_parked", Kind: "question", RequestID: "toolu_1", Title: "Go ahead?", State: "open"}}
	_, answer := h.createTargetedUtterance("parked-1", map[string]any{"kind": "signal", "signal_id": "sig_parked"})
	utteranceID := answer["utterance_id"].(string)
	h.postSegment(utteranceID, 0, "yes go ahead", time.Now().UnixMilli())
	h.finish(utteranceID)
	events := h.followUntilDone(utteranceID)
	if replies := resultTexts(events); len(replies) != 1 || replies[0] != "answer to yes go ahead" {
		t.Fatalf("replies = %v", replies)
	}
}

func TestANewSessionTargetStartsAChatSessionNotAVoiceOne(t *testing.T) {
	h := newHarness(t, map[string]string{"VOICE_INTAKE_POOL_SIZE": "1"})
	_, answer := h.createTargetedUtterance("new-1", map[string]any{"kind": "new_session"})
	utteranceID := answer["utterance_id"].(string)
	h.postSegment(utteranceID, 0, "plan my week", time.Now().UnixMilli())
	h.finish(utteranceID)
	events := h.followUntilDone(utteranceID)
	created, ids, sent, _ := h.bridge.snapshot()
	if len(h.bridge.chatCreated) != 1 {
		t.Fatalf("chat sessions = %v", h.bridge.chatCreated)
	}
	chat := h.bridge.chatCreated[0]
	if len(sent[chat]) != 1 || sent[chat][0] != "plan my week" {
		t.Fatalf("sent = %v (ids %v)", sent, ids)
	}
	// The pool may fill with voice sessions; none of them got the words.
	for _, id := range ids {
		if id != chat && len(sent[id]) > 0 {
			t.Fatalf("voice session %s got %v", id, sent[id])
		}
	}
	_ = created
	if replies := resultTexts(events); len(replies) != 1 || replies[0] != "answer to plan my week" {
		t.Fatalf("replies = %v", replies)
	}
	_, record := h.call("GET", "/api/voice/utterances/"+utteranceID, nil, testPrincipal)
	session := record["sessions"].([]any)[0].(map[string]any)
	if session["instance_id"] != "instance-the-chat-picks" || session["pooled"] != false {
		t.Fatalf("session row = %v", session)
	}
}

func TestAStreamedReplyToASessionIsOneMessageAcrossPauses(t *testing.T) {
	h, _ := newStreamingHarness(t, map[string]string{"VOICE_INTAKE_ENDPOINT_SILENCE_MS": "300"}, "what's the weather like", "in São Paulo")
	h.bridge.ownSession("br_mine", testPrincipal)
	status, answer := h.call("POST", "/api/voice/utterances", map[string]any{"client_utterance_id": "stream-reply", "mode": "quick", "strategy": "endpoint",
		"trigger": "widget", "speech_started_at_ms": time.Now().UnixMilli(), "audio_streaming": true, "audio_format": StreamAudioFormat,
		"target": map[string]any{"kind": "session", "session_id": "br_mine"}}, testPrincipal)
	if status != http.StatusCreated {
		t.Fatalf("create = %d %v", status, answer)
	}
	utteranceID := answer["utterance_id"].(string)
	h.streamAll(utteranceID, pcmOf(audioPart{true, 800}, audioPart{false, 500}, audioPart{true, 600}, audioPart{false, 500}))
	h.stop(utteranceID, StopReasonUser)
	events := h.followUntilDone(utteranceID)
	if got := len(eventsNamed(events, eventTranscript)); got != 2 {
		t.Fatalf("transcripts = %d, want 2", got)
	}
	if got := h.bridge.personSent["br_mine"]; len(got) != 1 || got[0] != "what's the weather like in São Paulo" {
		t.Fatalf("sent = %v", got)
	}
	if shared := h.bridge.shared["br_mine"]; len(shared) != 0 {
		t.Fatalf("the recording was shared into the person's session: %v", shared)
	}
}

func TestChatInstanceForFollowsTheChatsLadder(t *testing.T) {
	instances := []bridgeInstance{
		{ID: "inst-a", HarnessType: "codex", Enabled: true},
		{ID: "inst-b", HarnessType: "claude_code", Enabled: false},
		{ID: "inst-c", HarnessType: "claude_code", Enabled: true},
		{ID: "inst-d", HarnessType: "claude_code", Enabled: true},
	}
	cases := []struct {
		name        string
		preferences bridgePreferences
		want        string
	}{
		{"recorded instance", bridgePreferences{LastInstanceID: "inst-a", LastHarness: "claude_code"}, "inst-a"},
		{"disabled instance falls to harness", bridgePreferences{LastInstanceID: "inst-b", LastHarness: "claude_code"}, "inst-c"},
		{"harness prefers a used instance", bridgePreferences{LastHarness: "claude_code", LastSession: map[string]string{"inst-d": "br_1"}}, "inst-d"},
		{"nothing recorded", bridgePreferences{}, ""},
		{"harness with no enabled instance", bridgePreferences{LastHarness: "hermes"}, ""},
	}
	for _, c := range cases {
		got, found := chatInstanceFor(c.preferences, instances)
		if (c.want == "") == found || got.ID != c.want {
			t.Errorf("%s: got %q found=%v, want %q", c.name, got.ID, found, c.want)
		}
	}
}

func TestOpenQuestionFromGathersOnlyOpenQuestionsOfTheSameRequest(t *testing.T) {
	open := []bridgeSignal{
		{ID: "sig_1", SessionID: "br_1", Kind: "question", RequestID: "req"},
		{ID: "sig_2", SessionID: "br_1", Kind: "question", RequestID: "req"},
		{ID: "sig_3", SessionID: "br_2", Kind: "question", RequestID: "req"},
		{ID: "sig_4", SessionID: "br_1", Kind: "notification", RequestID: "req"},
		{ID: "sig_5", SessionID: "br_1", Kind: "question"},
	}
	question, err := openQuestionFrom(open, "sig_1")
	if err != nil || strings.Join(question.GroupSignalIDs, ",") != "sig_1,sig_2" || question.SessionID != "br_1" {
		t.Fatalf("group = %+v, %v", question, err)
	}
	if question, err := openQuestionFrom(open, "sig_5"); err != nil || len(question.GroupSignalIDs) != 1 {
		t.Fatalf("lone question = %+v, %v", question, err)
	}
	if _, err := openQuestionFrom(open, "sig_4"); !errors.Is(err, ErrSignalNotAnswerable) {
		t.Fatalf("notification: %v", err)
	}
	if _, err := openQuestionFrom(open, "sig_9"); !errors.Is(err, ErrTargetNotFound) {
		t.Fatalf("missing: %v", err)
	}
}
