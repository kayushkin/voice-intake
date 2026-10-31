package voiceintake

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// fakePrincipalStore answers GET /principals/{id} like principal-store: 404
// for an id it does not have, else the principal with its disabled_at.
type fakePrincipalStore struct {
	disabledAt map[string]int64
	asked      atomic.Int64
}

func newFakePrincipalStore(t *testing.T, disabledAt map[string]int64) (*fakePrincipalStore, string) {
	t.Helper()
	store := &fakePrincipalStore{disabledAt: disabledAt}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		store.asked.Add(1)
		id, found := strings.CutPrefix(r.URL.Path, "/principals/")
		disabled, known := store.disabledAt[id]
		if r.Method != http.MethodGet || !found || !known {
			http.NotFound(w, r)
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"id": id, "kind": "human", "disabled_at": disabled})
	}))
	t.Cleanup(server.Close)
	return store, server.URL
}

// knownTestPrincipals are the enabled principals every test harness's fake
// principal-store holds.
func knownTestPrincipals() map[string]int64 {
	return map[string]int64{testPrincipal: 0, "principal_000002": 0}
}

func TestAnUnknownOrDisabledPrincipalIsRefusedAtTheDoor(t *testing.T) {
	h := newHarness(t, nil)
	for _, principalID := range []string{"confirmation-smoke-check", "principal_000099", "principal_000003"} {
		if status, answer := h.call("GET", "/api/voice/utterances", nil, principalID); status != http.StatusForbidden {
			t.Fatalf("%s: GET /api/voice/utterances = %d %v, want 403", principalID, status, answer)
		}
	}
	if status, answer := h.call("GET", "/api/voice/utterances", nil, testPrincipal); status != http.StatusOK {
		t.Fatalf("known principal: %d %v", status, answer)
	}
}

func TestAConfirmedPrincipalIsAskedAboutOnceAMinute(t *testing.T) {
	fake, address := newFakePrincipalStore(t, knownTestPrincipals())
	now := time.Unix(1_800_000_000, 0)
	verifier := NewPrincipalStoreVerifier(address, func() time.Time { return now })
	for range 5 {
		if err := verifier.VerifyPrincipal(context.Background(), testPrincipal); err != nil {
			t.Fatal(err)
		}
	}
	if asked := fake.asked.Load(); asked != 1 {
		t.Fatalf("asked principal-store %d times within a minute, want 1", asked)
	}
	now = now.Add(verifiedPrincipalLifetime + time.Second)
	if err := verifier.VerifyPrincipal(context.Background(), testPrincipal); err != nil {
		t.Fatal(err)
	}
	if asked := fake.asked.Load(); asked != 2 {
		t.Fatalf("asked principal-store %d times after the minute, want 2", asked)
	}
	for range 2 {
		if err := verifier.VerifyPrincipal(context.Background(), "nobody"); !errors.Is(err, ErrUnknownPrincipal) {
			t.Fatalf("unknown principal: %v", err)
		}
	}
	if asked := fake.asked.Load(); asked != 4 {
		t.Fatalf("a refusal was remembered: asked %d times, want 4", asked)
	}
}

func TestPrincipalStoreBeingDownIsAFailureNotARefusal(t *testing.T) {
	verifier := NewPrincipalStoreVerifier("http://127.0.0.1:1", time.Now)
	err := verifier.VerifyPrincipal(context.Background(), testPrincipal)
	if err == nil || errors.Is(err, ErrUnknownPrincipal) || errors.Is(err, ErrDisabledPrincipal) {
		t.Fatalf("err = %v, want a failure to ask", err)
	}
}

func TestThePoolFillsNoOneForAnIdPrincipalStoreDoesNotHave(t *testing.T) {
	h := newHarness(t, map[string]string{"VOICE_INTAKE_POOL_SIZE": "1"})
	// A row a test once left behind, from before ids were checked.
	if _, _, err := h.store.CreateUtterance(Utterance{ClientUtteranceID: "stray", PrincipalID: "confirmation-smoke-check",
		Mode: "quick", Strategy: "endpoint", Trigger: "widget", ReceivedAtMs: time.Now().UnixMilli()}); err != nil {
		t.Fatal(err)
	}
	// A known person with an utterance still gets a pool, so the test can
	// tell "skipped the unknown id" from "filled nobody".
	if _, _, err := h.store.CreateUtterance(Utterance{ClientUtteranceID: "known", PrincipalID: testPrincipal,
		Mode: "quick", Strategy: "endpoint", Trigger: "widget", ReceivedAtMs: time.Now().UnixMilli()}); err != nil {
		t.Fatal(err)
	}
	if err := h.pool.Restore(context.Background()); err != nil {
		t.Fatal(err)
	}
	h.pool.filling.Wait()
	created, _, _, _ := h.bridge.snapshot()
	filledKnown := false
	for _, creation := range created {
		if creation.PrincipalID == "confirmation-smoke-check" {
			t.Fatalf("a pooled session was started for an unknown id: %+v", creation)
		}
		filledKnown = filledKnown || creation.PrincipalID == testPrincipal
	}
	if !filledKnown {
		t.Fatalf("no pooled session for the known principal either: %+v", created)
	}
}
