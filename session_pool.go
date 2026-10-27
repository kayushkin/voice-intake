package voiceintake

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"
)

// pooledSessionDisplayName is the title a waiting pooled session carries in
// the person's chat until text is sent to it and it is renamed.
const pooledSessionDisplayName = "Voice: ready for speech"

// sessionStatesThatCannotTakeAMessage are the bridge states in which a
// pooled session is given up rather than used. A session the idle reaper
// stopped reads completed and still takes a message: the bridge resumes it.
var sessionStatesThatCannotTakeAMessage = map[string]bool{"aborted": true, "error": true, "disconnected": true}

// SessionPool keeps pool_size started sessions waiting per person.
type SessionPool struct {
	store    *Store
	bridge   SessionBridge
	settings func() DispatchSettings
	now      func() time.Time

	mutex    sync.Mutex
	waiting  map[string][]PooledSession
	creating map[string]int
	known    map[string]bool
	// filling is held while sessions are created, so tests can wait for it.
	filling sync.WaitGroup
}

// NewSessionPool builds an empty pool.
func NewSessionPool(store *Store, bridge SessionBridge, settings func() DispatchSettings, now func() time.Time) *SessionPool {
	return &SessionPool{store: store, bridge: bridge, settings: settings, now: now,
		waiting: map[string][]PooledSession{}, creating: map[string]int{}, known: map[string]bool{}}
}

func (pool *SessionPool) matches(session PooledSession, settings DispatchSettings) bool {
	return session.InstanceID == settings.InstanceID && session.Effort == settings.DefaultEffort &&
		session.Model == settings.DefaultModel && session.WorkingDirectory == settings.WorkingDirectory
}

// Restore loads the sessions a previous run left waiting, then brings every
// known person's pool to size.
func (pool *SessionPool) Restore(ctx context.Context) error {
	waiting, err := pool.store.WaitingPooledSessions()
	if err != nil {
		return err
	}
	principals, err := pool.store.PrincipalsWithUtterances()
	if err != nil {
		return err
	}
	pool.mutex.Lock()
	for _, session := range waiting {
		pool.waiting[session.PrincipalID] = append(pool.waiting[session.PrincipalID], session)
		pool.known[session.PrincipalID] = true
	}
	for _, principalID := range principals {
		pool.known[principalID] = true
	}
	pool.mutex.Unlock()
	pool.Resync(ctx)
	return nil
}

// Take hands out a waiting session that matches the settings in force, and
// the state the bridge reported for it. ok is false when none is ready.
func (pool *SessionPool) Take(ctx context.Context, principalID, utteranceID string, settings DispatchSettings) (sessionID, state string, ok bool) {
	for {
		pool.mutex.Lock()
		pool.known[principalID] = true
		var candidate *PooledSession
		queue := pool.waiting[principalID]
		for index, session := range queue {
			if pool.matches(session, settings) {
				taken := session
				candidate = &taken
				pool.waiting[principalID] = append(append([]PooledSession(nil), queue[:index]...), queue[index+1:]...)
				break
			}
		}
		pool.mutex.Unlock()
		if candidate == nil {
			return "", "", false
		}
		state, err := pool.bridge.SessionState(ctx, candidate.SessionID)
		if err != nil {
			log.Printf("[pool] ERROR could not read the state of pooled session %s, giving it up: %v", candidate.SessionID, err)
			pool.release(ctx, *candidate, "its state could not be read: "+err.Error())
			continue
		}
		if sessionStatesThatCannotTakeAMessage[state] {
			pool.release(ctx, *candidate, "it was "+state+" when an utterance came for it")
			continue
		}
		if err := pool.store.TakePooledSession(candidate.SessionID, utteranceID, pool.now().UnixMilli()); err != nil {
			log.Printf("[pool] ERROR could not record that %s took pooled session %s: %v", utteranceID, candidate.SessionID, err)
		}
		return candidate.SessionID, state, true
	}
}

// Fill starts sessions in the background until the person's pool is at size.
func (pool *SessionPool) Fill(principalID string) {
	settings := pool.settings()
	pool.mutex.Lock()
	pool.known[principalID] = true
	matching := 0
	for _, session := range pool.waiting[principalID] {
		if pool.matches(session, settings) {
			matching++
		}
	}
	need := settings.PoolSize - matching - pool.creating[principalID]
	if need <= 0 {
		pool.mutex.Unlock()
		return
	}
	pool.creating[principalID] += need
	pool.filling.Add(need)
	pool.mutex.Unlock()
	for range need {
		go func() {
			defer pool.filling.Done()
			defer func() {
				pool.mutex.Lock()
				pool.creating[principalID]--
				pool.mutex.Unlock()
			}()
			if err := pool.createOne(principalID, settings); err != nil {
				log.Printf("[pool] ERROR could not start a pooled session for %s: %v", principalID, err)
			}
		}()
	}
}

func (pool *SessionPool) createOne(principalID string, settings DispatchSettings) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	sessionID, err := pool.bridge.CreateSession(ctx, SessionCreation{
		PrincipalID: principalID, InstanceID: settings.InstanceID, Effort: settings.DefaultEffort, Model: settings.DefaultModel,
		WorkingDirectory: settings.WorkingDirectory, DisplayName: pooledSessionDisplayName,
	})
	if err != nil {
		return err
	}
	session := PooledSession{SessionID: sessionID, PrincipalID: principalID, InstanceID: settings.InstanceID, Effort: settings.DefaultEffort,
		Model: settings.DefaultModel, WorkingDirectory: settings.WorkingDirectory, CreatedAtMs: pool.now().UnixMilli()}
	if err := pool.store.InsertPooledSession(session); err != nil {
		pool.release(ctx, session, "it could not be recorded: "+err.Error())
		return fmt.Errorf("record pooled session %s: %w", sessionID, err)
	}
	pool.mutex.Lock()
	pool.waiting[principalID] = append(pool.waiting[principalID], session)
	pool.mutex.Unlock()
	log.Printf("[pool] started pooled session %s for %s", sessionID, principalID)
	return nil
}

// Resync gives up every waiting session that no longer matches the settings
// or is past pool_size, and fills every known person's pool.
func (pool *SessionPool) Resync(ctx context.Context) {
	settings := pool.settings()
	var released []PooledSession
	var principals []string
	pool.mutex.Lock()
	for principalID := range pool.known {
		principals = append(principals, principalID)
		var kept []PooledSession
		for _, session := range pool.waiting[principalID] {
			if pool.matches(session, settings) && len(kept) < settings.PoolSize {
				kept = append(kept, session)
			} else {
				released = append(released, session)
			}
		}
		pool.waiting[principalID] = kept
	}
	pool.mutex.Unlock()
	for _, session := range released {
		pool.release(ctx, session, "the settings changed or the pool is smaller")
	}
	for _, principalID := range principals {
		pool.Fill(principalID)
	}
}

// release stops and archives a pooled session nobody will use.
func (pool *SessionPool) release(ctx context.Context, session PooledSession, reason string) {
	if err := pool.bridge.StopSession(ctx, session.SessionID); err != nil {
		log.Printf("[pool] ERROR could not stop pooled session %s: %v", session.SessionID, err)
	}
	if err := pool.bridge.ArchiveSession(ctx, session.SessionID); err != nil {
		log.Printf("[pool] ERROR could not archive pooled session %s: %v", session.SessionID, err)
	}
	if err := pool.store.ReleasePooledSession(session.SessionID, reason, pool.now().UnixMilli()); err != nil {
		log.Printf("[pool] ERROR could not record the release of pooled session %s: %v", session.SessionID, err)
	}
	log.Printf("[pool] released pooled session %s: %s", session.SessionID, reason)
}

// WaitForFills blocks until no pooled session is being started.
func (pool *SessionPool) WaitForFills() { pool.filling.Wait() }
