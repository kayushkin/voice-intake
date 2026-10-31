package voiceintake

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// ErrUnknownPrincipal is a principal id principal-store does not have.
var ErrUnknownPrincipal = errors.New("principal-store has no such principal")

// ErrDisabledPrincipal is a principal principal-store has disabled.
var ErrDisabledPrincipal = errors.New("principal-store has disabled this principal")

// PrincipalVerifier says whether an id names an enabled principal in
// principal-store, the service that owns them. Every request and every pool
// refill checks through it, so an id that was never a principal (a test's
// made-up name, say) is refused at the door instead of being remembered and
// retried at every start.
type PrincipalVerifier interface {
	VerifyPrincipal(ctx context.Context, principalID string) error
}

// PrincipalStoreVerifier asks principal-store's GET /principals/{id}. A
// principal it confirmed is believed for verifiedPrincipalLifetime, so a
// phone streaming audio every 200 ms does not ask once per chunk; a refusal
// or a failure to ask is never kept.
type PrincipalStoreVerifier struct {
	baseURL    string
	httpClient *http.Client
	now        func() time.Time

	mutex      sync.Mutex
	verifiedAt map[string]time.Time
}

const verifiedPrincipalLifetime = time.Minute

// NewPrincipalStoreVerifier checks against the principal-store at baseURL.
func NewPrincipalStoreVerifier(baseURL string, now func() time.Time) *PrincipalStoreVerifier {
	return &PrincipalStoreVerifier{baseURL: strings.TrimRight(baseURL, "/"), httpClient: &http.Client{Timeout: 5 * time.Second},
		now: now, verifiedAt: map[string]time.Time{}}
}

// VerifyPrincipal answers nil for an enabled principal, ErrUnknownPrincipal
// or ErrDisabledPrincipal for one that may not act, and any other error when
// principal-store could not be asked.
func (verifier *PrincipalStoreVerifier) VerifyPrincipal(ctx context.Context, principalID string) error {
	verifier.mutex.Lock()
	verifiedAt, seen := verifier.verifiedAt[principalID]
	verifier.mutex.Unlock()
	if seen && verifier.now().Sub(verifiedAt) < verifiedPrincipalLifetime {
		return nil
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, verifier.baseURL+"/principals/"+url.PathEscape(principalID), nil)
	if err != nil {
		return err
	}
	response, err := verifier.httpClient.Do(request)
	if err != nil {
		return fmt.Errorf("asking principal-store about %q: %w", principalID, err)
	}
	defer response.Body.Close()
	switch {
	case response.StatusCode == http.StatusNotFound:
		return fmt.Errorf("%w: %q", ErrUnknownPrincipal, principalID)
	case response.StatusCode != http.StatusOK:
		return fmt.Errorf("asking principal-store about %q: it answered %s", principalID, response.Status)
	}
	var principal struct {
		ID         string `json:"id"`
		DisabledAt int64  `json:"disabled_at"`
	}
	if err := json.NewDecoder(response.Body).Decode(&principal); err != nil {
		return fmt.Errorf("reading principal-store's answer about %q: %w", principalID, err)
	}
	if principal.ID != principalID {
		return fmt.Errorf("principal-store answered for %q when asked about %q", principal.ID, principalID)
	}
	if principal.DisabledAt != 0 {
		return fmt.Errorf("%w: %q", ErrDisabledPrincipal, principalID)
	}
	verifier.mutex.Lock()
	verifier.verifiedAt[principalID] = verifier.now()
	verifier.mutex.Unlock()
	return nil
}
