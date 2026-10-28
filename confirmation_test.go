package voiceintake

import (
	"net/http"
	"testing"
	"time"
)

func (h *harness) confirm(utteranceID string, body any) (int, map[string]any) {
	h.t.Helper()
	return h.call("POST", "/api/voice/utterances/"+utteranceID+"/confirm", body, testPrincipal)
}

func TestASessionTargetHoldsItsWordsUntilTheyAreConfirmed(t *testing.T) {
	h := newHarness(t, nil)
	h.bridge.ownSession("br_mine", testPrincipal)
	utteranceID := h.createHeldUtterance("held-1", map[string]any{"kind": "session", "session_id": "br_mine"})
	if status, _ := h.confirm(utteranceID, nil); status != http.StatusConflict {
		t.Fatalf("confirm before anything is held = %d, want 409", status)
	}
	now := time.Now().UnixMilli()
	h.postSegment(utteranceID, 0, "what's the weather like", now-2000)
	h.postSegment(utteranceID, 1, "in São Paulo", now-500)
	h.finish(utteranceID)
	events := h.followUntil(utteranceID, eventAwaitingConfirmation)
	held := events[len(events)-1].data
	if held["text"] != "what's the weather like in São Paulo" || held["expires_at_ms"].(float64) < float64(time.Now().Add(110*time.Second).UnixMilli()) {
		t.Fatalf("awaiting_confirmation = %v", held)
	}
	time.Sleep(100 * time.Millisecond)
	if got := h.bridge.personSent["br_mine"]; len(got) != 0 {
		t.Fatalf("sent before confirm: %v", got)
	}
	_, record := h.call("GET", "/api/voice/utterances/"+utteranceID, nil, testPrincipal)
	if record["confirm_before_send"] != true || record["confirmation_state"] != ConfirmationAwaiting || record["finalized_at_ms"] != nil {
		t.Fatalf("held record = %v", record)
	}
	if status, answer := h.confirm(utteranceID, map[string]any{"text": "something else"}); status != http.StatusConflict {
		t.Fatalf("confirm with other words = %d %v, want 409", status, answer)
	}
	if status, answer := h.confirm(utteranceID, map[string]any{"text": "what's the weather like in São Paulo"}); status != http.StatusOK {
		t.Fatalf("confirm = %d %v", status, answer)
	}
	events = h.followUntilDone(utteranceID)
	if ended := eventsNamed(events, eventConfirmationEnded); len(ended) != 1 || ended[0].data["state"] != ConfirmationConfirmed {
		t.Fatalf("confirmation_ended = %v", ended)
	}
	if got := h.bridge.personSent["br_mine"]; len(got) != 1 || got[0] != "what's the weather like in São Paulo" {
		t.Fatalf("sent = %v, want one joined message", got)
	}
	if replies := resultTexts(events); len(replies) != 1 {
		t.Fatalf("replies = %v", replies)
	}
	if status, _ := h.confirm(utteranceID, nil); status != http.StatusConflict {
		t.Fatalf("a second confirm = %d, want 409", status)
	}
	_, record = h.call("GET", "/api/voice/utterances/"+utteranceID, nil, testPrincipal)
	if record["confirmation_state"] != ConfirmationConfirmed || record["confirmation_resolved_at_ms"] == nil {
		t.Fatalf("confirmed record = %v", record)
	}
}

func TestCancellingHeldWordsSendsNothing(t *testing.T) {
	h := newHarness(t, nil)
	h.bridge.ownSession("br_mine", testPrincipal)
	utteranceID := h.createHeldUtterance("held-cancel", map[string]any{"kind": "session", "session_id": "br_mine"})
	h.postSegment(utteranceID, 0, "using continuous summary", time.Now().UnixMilli()-500)
	h.finish(utteranceID)
	h.followUntil(utteranceID, eventAwaitingConfirmation)
	if status, answer := h.call("POST", "/api/voice/utterances/"+utteranceID+"/cancel", nil, testPrincipal); status != http.StatusOK {
		t.Fatalf("cancel = %d %v", status, answer)
	}
	events := h.followUntilDone(utteranceID)
	if ended := eventsNamed(events, eventConfirmationEnded); len(ended) != 1 || ended[0].data["state"] != ConfirmationCancelled {
		t.Fatalf("confirmation_ended = %v", ended)
	}
	if status, _ := h.confirm(utteranceID, nil); status != http.StatusConflict {
		t.Fatalf("confirm after cancel = %d, want 409", status)
	}
	if got := h.bridge.personSent["br_mine"]; len(got) != 0 {
		t.Fatalf("sent = %v, want nothing", got)
	}
}

func TestHeldWordsNobodyConfirmsExpireUnsent(t *testing.T) {
	saved := confirmationLimit
	confirmationLimit = 300 * time.Millisecond
	t.Cleanup(func() { confirmationLimit = saved })
	h := newHarness(t, nil)
	h.bridge.ownSession("br_mine", testPrincipal)
	utteranceID := h.createHeldUtterance("held-expire", map[string]any{"kind": "session", "session_id": "br_mine"})
	h.postSegment(utteranceID, 0, "mm-hmm", time.Now().UnixMilli()-500)
	h.finish(utteranceID)
	events := h.followUntilDone(utteranceID)
	if ended := eventsNamed(events, eventConfirmationEnded); len(ended) != 1 || ended[0].data["state"] != ConfirmationExpired {
		t.Fatalf("confirmation_ended = %v", ended)
	}
	if got := h.bridge.personSent["br_mine"]; len(got) != 0 {
		t.Fatalf("sent = %v, want nothing", got)
	}
	_, record := h.call("GET", "/api/voice/utterances/"+utteranceID, nil, testPrincipal)
	if record["confirmation_state"] != ConfirmationExpired {
		t.Fatalf("record = %v", record)
	}
	if status, _ := h.confirm(utteranceID, nil); status != http.StatusConflict {
		t.Fatalf("confirm after expiry = %d, want 409", status)
	}
}

func TestAHeldNewSessionStartsOneChatSessionOnlyWhenConfirmed(t *testing.T) {
	h := newHarness(t, nil)
	utteranceID := h.createHeldUtterance("held-new", map[string]any{"kind": "new_session"})
	now := time.Now().UnixMilli()
	h.postSegment(utteranceID, 0, "plan my week", now-2000)
	h.postSegment(utteranceID, 1, "and book the dentist", now-500)
	h.finish(utteranceID)
	h.followUntil(utteranceID, eventAwaitingConfirmation)
	if created, _, _, _ := h.bridge.snapshot(); len(created) != 0 || len(h.bridge.chatCreated) != 0 {
		t.Fatalf("sessions started before confirm: %v %v", created, h.bridge.chatCreated)
	}
	if status, answer := h.confirm(utteranceID, nil); status != http.StatusOK {
		t.Fatalf("confirm = %d %v", status, answer)
	}
	events := h.followUntilDone(utteranceID)
	_, ids, sent, _ := h.bridge.snapshot()
	if len(ids) != 1 || len(sent[ids[0]]) != 1 || sent[ids[0]][0] != "plan my week and book the dentist" || len(h.judge.asked) != 0 {
		t.Fatalf("sessions %v sent %v judge %v", ids, sent, h.judge.asked)
	}
	if roles := decisionsOf(events); roles[ids[0]] != RoleKept {
		t.Fatalf("decisions = %v", roles)
	}
}

func TestAStreamedReplyIsHeldAndThePoolIsNever(t *testing.T) {
	h, _ := newStreamingHarness(t, map[string]string{"VOICE_INTAKE_ENDPOINT_SILENCE_MS": "300"}, "what's the weather like", "in São Paulo")
	h.bridge.ownSession("br_mine", testPrincipal)
	status, answer := h.call("POST", "/api/voice/utterances", map[string]any{"client_utterance_id": "stream-held", "mode": "quick", "strategy": "endpoint",
		"trigger": "widget", "speech_started_at_ms": time.Now().UnixMilli(), "audio_streaming": true, "audio_format": StreamAudioFormat,
		"target": map[string]any{"kind": "session", "session_id": "br_mine"}, "confirm_before_send": true}, testPrincipal)
	if status != http.StatusCreated {
		t.Fatalf("create = %d %v", status, answer)
	}
	utteranceID := answer["utterance_id"].(string)
	h.streamAll(utteranceID, pcmOf(audioPart{true, 800}, audioPart{false, 500}, audioPart{true, 600}, audioPart{false, 500}))
	h.stop(utteranceID, StopReasonUser)
	events := h.followUntil(utteranceID, eventAwaitingConfirmation)
	if text := events[len(events)-1].data["text"]; text != "what's the weather like in São Paulo" {
		t.Fatalf("held text = %v", text)
	}
	if status, _ := h.confirm(utteranceID, map[string]any{}); status != http.StatusOK {
		t.Fatalf("confirm with an empty body = %d", status)
	}
	h.followUntilDone(utteranceID)
	if got := h.bridge.personSent["br_mine"]; len(got) != 1 {
		t.Fatalf("sent = %v", got)
	}

	if status, _ := h.call("POST", "/api/voice/utterances", map[string]any{"client_utterance_id": "pool-held", "mode": "quick", "strategy": "endpoint",
		"trigger": "widget", "speech_started_at_ms": time.Now().UnixMilli(), "confirm_before_send": true}, testPrincipal); status != http.StatusBadRequest {
		t.Fatalf("confirm_before_send on the pool = %d, want 400", status)
	}
}

// A client that does not ask for the hold, such as an app version with no
// confirm screen, gets its words sent at once rather than held until they
// expire unsent.
func TestASessionTargetIsSentAtOnceWhenTheClientDoesNotAskForTheHold(t *testing.T) {
	h := newHarness(t, nil)
	h.bridge.ownSession("br_mine", testPrincipal)
	status, answer := h.call("POST", "/api/voice/utterances", map[string]any{"client_utterance_id": "old-client", "mode": "quick", "strategy": "endpoint",
		"trigger": "widget", "speech_started_at_ms": time.Now().UnixMilli() - 3000,
		"target": map[string]any{"kind": "session", "session_id": "br_mine"}}, testPrincipal)
	if status != http.StatusCreated {
		t.Fatalf("create = %d %v", status, answer)
	}
	utteranceID := answer["utterance_id"].(string)
	h.postSegment(utteranceID, 0, "carry on with the caching work", time.Now().UnixMilli()-500)
	h.finish(utteranceID)
	h.followUntilDone(utteranceID)
	h.bridge.snapshot()
	if got := h.bridge.personSent["br_mine"]; len(got) != 1 || got[0] != "carry on with the caching work" {
		t.Fatalf("sent to the person's session = %v, want the words once", got)
	}
}
