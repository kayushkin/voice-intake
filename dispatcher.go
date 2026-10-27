package voiceintake

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/kayushkin/llm-bridge/msg"
)

// sessionTurnLimit is how long a dispatched session's event stream is read
// for its result before the turn is counted as finished without one.
const sessionTurnLimit = 5 * time.Minute

// streamOpenWait is how long a dispatch waits for the event stream to open
// before it sends anyway. The bridge replays the current turn to a late
// subscriber, so sending first loses no event; waiting only keeps it simple.
const streamOpenWait = 5 * time.Second

// sessionStatesThatEndATurn are bridge states after which no result will
// come for the message sent.
var sessionStatesThatEndATurn = map[msg.SessionState]bool{msg.SessionAborted: true, msg.SessionError: true, msg.SessionDisconnected: true}

// Dispatcher turns segments into sessions and decides which sessions to keep.
type Dispatcher struct {
	store          *Store
	bridge         SessionBridge
	judge          ContinuationJudge
	pool           *SessionPool
	settings       func() DispatchSettings
	now            func() time.Time
	audioDirectory string

	mutex sync.Mutex
	runs  map[string]*utteranceRun
}

// NewDispatcher builds a dispatcher.
func NewDispatcher(store *Store, bridge SessionBridge, judge ContinuationJudge, pool *SessionPool, settings func() DispatchSettings, now func() time.Time, audioDirectory string) *Dispatcher {
	return &Dispatcher{store: store, bridge: bridge, judge: judge, pool: pool, settings: settings, now: now, audioDirectory: audioDirectory,
		runs: map[string]*utteranceRun{}}
}

// requestGroup is one request the utterance holds: the segments read as one
// message, and the session answering it.
type requestGroup struct {
	segmentIndexes []int
	text           string
	sessionID      string
}

// runSession is a dispatched session as the run tracks it.
type runSession struct {
	sessionID      string
	segmentIndexes []int
	text           string
	role           string
	turnFinished   bool
	stopped        bool
	cancelStream   context.CancelFunc
}

type utteranceAudio struct {
	filename    string
	contentType string
	content     []byte
}

// utteranceRun is the live state of one utterance. Segment processing,
// finishing and window expiry run one at a time on its worker, in order.
type utteranceRun struct {
	dispatcher  *Dispatcher
	utteranceID string
	principalID string
	hub         *eventHub
	jobs        chan func()
	stopped     chan struct{}
	stopOnce    sync.Once

	mutex       sync.Mutex
	segments    map[int]Segment
	nextIndex   int
	groups      []*requestGroup
	sessions    map[string]*runSession
	order       []string
	final       bool
	cancelled   bool
	windowTimer *time.Timer
	audio       *utteranceAudio
}

// runFor returns the live run of an utterance, building it from the store
// when this process has none (the utterance was made before a restart).
func (d *Dispatcher) runFor(utterance Utterance) (*utteranceRun, error) {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	if run, ok := d.runs[utterance.UtteranceID]; ok {
		return run, nil
	}
	run := &utteranceRun{dispatcher: d, utteranceID: utterance.UtteranceID, principalID: utterance.PrincipalID, hub: newEventHub(),
		jobs: make(chan func(), 64), stopped: make(chan struct{}), segments: map[int]Segment{}, sessions: map[string]*runSession{}}
	if err := run.loadFromStore(utterance); err != nil {
		return nil, err
	}
	d.runs[utterance.UtteranceID] = run
	go run.work()
	run.checkDone()
	return run, nil
}

// loadFromStore brings back what an earlier process recorded. Segments it
// stored are counted as handled; sessions it started are not streamed again.
func (run *utteranceRun) loadFromStore(utterance Utterance) error {
	segments, err := run.dispatcher.store.Segments(utterance.UtteranceID)
	if err != nil {
		return err
	}
	for _, segment := range segments {
		run.segments[segment.Index] = segment
		if segment.Index >= run.nextIndex {
			run.nextIndex = segment.Index + 1
		}
	}
	sessions, err := run.dispatcher.store.UtteranceSessions(utterance.UtteranceID)
	if err != nil {
		return err
	}
	for _, session := range sessions {
		run.sessions[session.SessionID] = &runSession{sessionID: session.SessionID, segmentIndexes: session.SegmentIndexes, text: session.Text,
			role: session.Role, turnFinished: true, stopped: session.StoppedAtMs != nil}
		run.order = append(run.order, session.SessionID)
		run.hub.publish(eventDispatch, dispatchEventData(session.SessionID, session.SegmentIndexes, session.Text))
	}
	decisions, err := run.dispatcher.store.Decisions(utterance.UtteranceID)
	if err != nil {
		return err
	}
	for _, decision := range decisions {
		run.hub.publish(eventDecision, decisionEventData(decision.SessionID, decision.Role, decision.Reason))
	}
	run.final = utterance.FinalizedAtMs != nil
	run.cancelled = utterance.CancelledAtMs != nil
	return nil
}

func (run *utteranceRun) work() {
	for {
		select {
		case job := <-run.jobs:
			job()
		case <-run.stopped:
			return
		}
	}
}

func (run *utteranceRun) enqueue(job func()) {
	select {
	case run.jobs <- job:
	case <-run.stopped:
	}
}

func dispatchEventData(sessionID string, segmentIndexes []int, text string) map[string]any {
	return map[string]any{"session_id": sessionID, "segment_indexes": segmentIndexes, "text": text, "role": RoleSpeculative}
}

func decisionEventData(sessionID, role, reason string) map[string]any {
	return map[string]any{"session_id": sessionID, "role": role, "reason": reason}
}

// Start makes the run for a new utterance and fills the person's pool.
func (d *Dispatcher) Start(utterance Utterance) error {
	if _, err := d.runFor(utterance); err != nil {
		return err
	}
	d.pool.Fill(utterance.PrincipalID)
	return nil
}

// SegmentArrived hands a stored segment to the utterance's worker.
func (d *Dispatcher) SegmentArrived(utterance Utterance, segment Segment) error {
	run, err := d.runFor(utterance)
	if err != nil {
		return err
	}
	run.mutex.Lock()
	run.segments[segment.Index] = segment
	run.mutex.Unlock()
	run.enqueue(run.processReadySegments)
	return nil
}

// Finish makes the decisions final once every segment received is handled.
func (d *Dispatcher) Finish(utterance Utterance) error {
	run, err := d.runFor(utterance)
	if err != nil {
		return err
	}
	run.enqueue(func() {
		run.processReadySegments()
		run.finalize("the phone finished the utterance")
	})
	return nil
}

// Cancel stops every session the utterance started.
func (d *Dispatcher) Cancel(utterance Utterance) error {
	run, err := d.runFor(utterance)
	if err != nil {
		return err
	}
	run.cancel()
	return nil
}

// Follow streams the utterance's events.
func (d *Dispatcher) Follow(ctx context.Context, utterance Utterance, deliver func(UtteranceEvent) error) error {
	run, err := d.runFor(utterance)
	if err != nil {
		return err
	}
	return run.hub.follow(ctx, deliver)
}

// processReadySegments handles every received segment whose turn has come,
// in index order.
func (run *utteranceRun) processReadySegments() {
	for {
		run.mutex.Lock()
		segment, ready := run.segments[run.nextIndex]
		cancelled := run.cancelled
		run.mutex.Unlock()
		if !ready || cancelled {
			return
		}
		run.processSegment(segment)
		run.mutex.Lock()
		run.nextIndex = segment.Index + 1
		run.mutex.Unlock()
		run.scheduleWindowExpiry(segment)
	}
}

func (run *utteranceRun) processSegment(segment Segment) {
	d := run.dispatcher
	settings := d.settings()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	run.mutex.Lock()
	var last *requestGroup
	if len(run.groups) > 0 {
		last = run.groups[len(run.groups)-1]
	}
	previous, havePrevious := run.segments[segment.Index-1]
	final := run.final
	run.mutex.Unlock()

	if last == nil {
		session, err := run.dispatch(ctx, []int{segment.Index}, segment.Text)
		if err == nil {
			run.appendGroup(&requestGroup{segmentIndexes: []int{segment.Index}, text: segment.Text, sessionID: session.sessionID})
		}
		return
	}

	gapMs := int64(-1)
	if havePrevious {
		gapMs = segment.ReceivedAtMs - previous.SpeechEndedAtMs
	}
	windowMs := settings.ContinuationWindow.Milliseconds()
	if final || !havePrevious || gapMs > windowMs {
		why := "the utterance's decisions were already final"
		if !final && !havePrevious {
			why = fmt.Sprintf("segment %d was not handled by this process", segment.Index-1)
		} else if !final {
			why = fmt.Sprintf("segment %d arrived %d ms after segment %d's speech end, past the %d ms continuation window", segment.Index, gapMs, previous.Index, windowMs)
		}
		run.decide(last.sessionID, RoleKept, "no later segment continued it: "+why)
		session, err := run.dispatch(ctx, []int{segment.Index}, segment.Text)
		if err == nil {
			run.decide(session.sessionID, RoleSeparate, why)
			run.appendGroup(&requestGroup{segmentIndexes: []int{segment.Index}, text: segment.Text, sessionID: session.sessionID})
		}
		return
	}

	joinedIndexes := append(append([]int(nil), last.segmentIndexes...), segment.Index)
	joinedText := strings.TrimSpace(last.text + " " + segment.Text)
	var joined, alone *runSession
	var verdict ContinuationVerdict
	var judgeErr error
	var waitGroup sync.WaitGroup
	waitGroup.Add(3)
	go func() { defer waitGroup.Done(); joined, _ = run.dispatch(ctx, joinedIndexes, joinedText) }()
	go func() { defer waitGroup.Done(); alone, _ = run.dispatch(ctx, []int{segment.Index}, segment.Text) }()
	go func() {
		defer waitGroup.Done()
		verdict, judgeErr = run.askJudge(ctx, settings.JudgeModelRole, segment.Index, last.text, segment.Text)
	}()
	waitGroup.Wait()

	judgeSaid := ""
	switch {
	case judgeErr != nil:
		judgeSaid = "the continuation judge failed (" + judgeErr.Error() + ")"
	case verdict.Continues:
		judgeSaid = fmt.Sprintf("the judge (%s) said segment %d continues the earlier request: %s", verdict.Model, segment.Index, verdict.Reason)
	default:
		judgeSaid = fmt.Sprintf("the judge (%s) said segment %d is a separate request: %s", verdict.Model, segment.Index, verdict.Reason)
	}
	keepJoined := joined != nil && (judgeErr != nil || verdict.Continues || alone == nil)
	switch {
	case keepJoined:
		reason := judgeSaid
		if judgeErr != nil {
			reason += "; kept the joined text, which carries every word"
		} else if !verdict.Continues {
			reason += "; but the session for segment alone could not be started, so the joined text is kept"
		}
		run.decide(joined.sessionID, RoleKept, reason)
		run.decide(last.sessionID, RoleSuperseded, reason)
		if alone != nil {
			run.decide(alone.sessionID, RoleSuperseded, reason)
		}
		run.replaceLastGroup(&requestGroup{segmentIndexes: joinedIndexes, text: joinedText, sessionID: joined.sessionID})
	case alone != nil:
		reason := judgeSaid
		if joined == nil && verdict.Continues {
			reason += "; but the joined session could not be started, so the two are kept apart"
		}
		run.decide(last.sessionID, RoleKept, reason)
		run.decide(alone.sessionID, RoleSeparate, reason)
		if joined != nil {
			run.decide(joined.sessionID, RoleSuperseded, reason)
		}
		run.appendGroup(&requestGroup{segmentIndexes: []int{segment.Index}, text: segment.Text, sessionID: alone.sessionID})
	default:
		run.recordError(fmt.Sprintf("segment %d: neither the joined session nor the session for it alone could be started; the earlier request stands as it was", segment.Index))
	}
}

func (run *utteranceRun) appendGroup(group *requestGroup) {
	run.mutex.Lock()
	run.groups = append(run.groups, group)
	run.mutex.Unlock()
}

func (run *utteranceRun) replaceLastGroup(group *requestGroup) {
	run.mutex.Lock()
	run.groups[len(run.groups)-1] = group
	run.mutex.Unlock()
}

func (run *utteranceRun) askJudge(ctx context.Context, modelRole string, segmentIndex int, earlierText, newText string) (ContinuationVerdict, error) {
	d := run.dispatcher
	started := d.now()
	judgeContext, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	verdict, err := d.judge.Judge(judgeContext, modelRole, earlierText, newText)
	judgment := ContinuationJudgment{SegmentIndex: segmentIndex, EarlierText: earlierText, NewText: newText, ModelRole: modelRole,
		StartedAtMs: started.UnixMilli(), DurationMs: d.now().Sub(started).Milliseconds()}
	if err != nil {
		judgment.Error = err.Error()
		log.Printf("[dispatch] ERROR %s segment %d: continuation judge: %v", run.utteranceID, segmentIndex, err)
	} else {
		continues := verdict.Continues
		judgment.Continues = &continues
		judgment.Reason = verdict.Reason
		judgment.Model = verdict.Model
	}
	if storeErr := d.store.InsertContinuationJudgment(run.utteranceID, judgment); storeErr != nil {
		log.Printf("[dispatch] ERROR %s: record judgment: %v", run.utteranceID, storeErr)
	}
	return verdict, err
}

// effortFor is the effort a session with this text starts at.
func effortFor(text string, settings DispatchSettings) string {
	lowered := strings.ToLower(text)
	for _, phrase := range settings.EffortRaisingPhrases {
		if phrase != "" && strings.Contains(lowered, strings.ToLower(phrase)) {
			return settings.RaisedEffort
		}
	}
	return settings.DefaultEffort
}

// dispatch sends text to a session — a pooled one when one is ready and the
// text runs at the default effort, else a new one — and starts reading its
// events. The session's role is speculative until a decision says otherwise.
func (run *utteranceRun) dispatch(ctx context.Context, segmentIndexes []int, text string) (*runSession, error) {
	d := run.dispatcher
	settings := d.settings()
	effort := effortFor(text, settings)
	started := d.now()

	sessionID, pooledState, pooled := "", "", false
	if effort == settings.DefaultEffort {
		sessionID, pooledState, pooled = d.pool.Take(ctx, run.principalID, run.utteranceID, settings)
	}
	if !pooled {
		created, err := d.bridge.CreateSession(ctx, SessionCreation{PrincipalID: run.principalID, InstanceID: settings.InstanceID, Effort: effort,
			Model: settings.DefaultModel, WorkingDirectory: settings.WorkingDirectory, DisplayName: text})
		if err != nil {
			run.recordError(fmt.Sprintf("segments %v: start a session: %v", segmentIndexes, err))
			return nil, err
		}
		sessionID = created
	}
	defer d.pool.Fill(run.principalID)

	session := &runSession{sessionID: sessionID, segmentIndexes: segmentIndexes, text: text, role: RoleSpeculative}
	record := UtteranceSession{SessionID: sessionID, UtteranceID: run.utteranceID, SegmentIndexes: segmentIndexes, Text: text, Role: RoleSpeculative,
		Pooled: pooled, PooledSessionState: pooledState, Effort: effort, Model: settings.DefaultModel, InstanceID: settings.InstanceID,
		DispatchStartedAtMs: started.UnixMilli()}
	if err := d.store.InsertUtteranceSession(record); err != nil {
		log.Printf("[dispatch] ERROR %s: record session %s: %v", run.utteranceID, sessionID, err)
	}

	streamContext, cancelStream := context.WithTimeout(context.Background(), sessionTurnLimit)
	session.cancelStream = cancelStream
	run.mutex.Lock()
	cancelled := run.cancelled
	run.sessions[sessionID] = session
	run.order = append(run.order, sessionID)
	run.mutex.Unlock()
	if cancelled {
		run.stopSession(session, "the utterance was cancelled before its text was sent")
		return nil, errors.New("cancelled")
	}

	connected, streamEnded := make(chan struct{}), make(chan struct{})
	go run.streamSession(streamContext, session, connected, streamEnded)
	select {
	case <-connected:
	case <-streamEnded:
	case <-time.After(streamOpenWait):
		log.Printf("[dispatch] %s: the event stream of %s did not open in %s; sending anyway", run.utteranceID, sessionID, streamOpenWait)
	}

	if err := d.bridge.SendMessage(ctx, sessionID, text); err != nil {
		message := fmt.Sprintf("segments %v: send to %s: %v", segmentIndexes, sessionID, err)
		run.recordError(message)
		if storeErr := d.store.SetSessionError(sessionID, err.Error()); storeErr != nil {
			log.Printf("[dispatch] ERROR %s: %v", run.utteranceID, storeErr)
		}
		run.stopSession(session, "its text could not be sent")
		return nil, err
	}
	if err := d.store.SetSessionDispatched(sessionID, d.now().UnixMilli()); err != nil {
		log.Printf("[dispatch] ERROR %s: %v", run.utteranceID, err)
	}
	run.hub.publish(eventDispatch, dispatchEventData(sessionID, segmentIndexes, text))

	if pooled {
		go func() {
			renameContext, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if err := d.bridge.RenameSession(renameContext, sessionID, text); err != nil {
				log.Printf("[dispatch] ERROR %s: rename pooled session %s: %v", run.utteranceID, sessionID, err)
			}
		}()
	}
	run.mutex.Lock()
	audio := run.audio
	run.mutex.Unlock()
	if audio != nil {
		go run.shareAudio(session, audio)
	}
	return session, nil
}

// streamSession reads one session's events until its result: it records the
// first block and the result, and passes every event on unless the session
// was superseded.
func (run *utteranceRun) streamSession(ctx context.Context, session *runSession, connected, streamEnded chan struct{}) {
	d := run.dispatcher
	defer close(streamEnded)
	var connectOnce sync.Once
	sawResult := false
	err := d.bridge.StreamSessionEvents(ctx, session.sessionID, func() { connectOnce.Do(func() { close(connected) }) }, func(event BridgeEvent) bool {
		at := d.now().UnixMilli()
		switch msg.EventType(event.Type) {
		case msg.EventBlock:
			if err := d.store.SetSessionFirstBlock(session.sessionID, at); err != nil {
				log.Printf("[dispatch] ERROR %s: %v", run.utteranceID, err)
			}
		case msg.EventResult:
			if err := d.store.SetSessionResult(session.sessionID, at); err != nil {
				log.Printf("[dispatch] ERROR %s: %v", run.utteranceID, err)
			}
		}
		run.mutex.Lock()
		forward := session.role != RoleSuperseded && !session.stopped
		run.mutex.Unlock()
		if forward {
			run.hub.publish(eventSessionEvent, map[string]any{"session_id": session.sessionID, "event": event.Data})
		}
		if msg.EventType(event.Type) == msg.EventResult {
			sawResult = true
			return false
		}
		if msg.EventType(event.Type) == msg.EventSessionStatus && turnEndingStatus(event.Data) {
			return false
		}
		return true
	})
	run.mutex.Lock()
	stopped := session.stopped
	session.turnFinished = true
	run.mutex.Unlock()
	if !sawResult && !stopped {
		problem := "its event stream ended before a result"
		if err != nil {
			problem += ": " + err.Error()
		}
		log.Printf("[dispatch] ERROR %s: session %s: %s", run.utteranceID, session.sessionID, problem)
		if storeErr := d.store.SetSessionError(session.sessionID, problem); storeErr != nil {
			log.Printf("[dispatch] ERROR %s: %v", run.utteranceID, storeErr)
		}
	}
	run.checkDone()
}

func turnEndingStatus(data json.RawMessage) bool {
	var event struct {
		Status *msg.SessionStatus `json:"status"`
	}
	if err := json.Unmarshal(data, &event); err != nil || event.Status == nil {
		return false
	}
	return sessionStatesThatEndATurn[event.Status.State]
}

// decide gives a session a role, records it and tells the phone; a
// superseded session is stopped.
func (run *utteranceRun) decide(sessionID, role, reason string) {
	d := run.dispatcher
	run.mutex.Lock()
	session := run.sessions[sessionID]
	if session == nil || session.role == role {
		run.mutex.Unlock()
		return
	}
	session.role = role
	run.mutex.Unlock()
	if err := d.store.SetSessionRole(sessionID, role); err != nil {
		log.Printf("[dispatch] ERROR %s: %v", run.utteranceID, err)
	}
	if err := d.store.InsertDecision(run.utteranceID, Decision{SessionID: sessionID, Role: role, Reason: reason, DecidedAtMs: d.now().UnixMilli()}); err != nil {
		log.Printf("[dispatch] ERROR %s: %v", run.utteranceID, err)
	}
	run.hub.publish(eventDecision, decisionEventData(sessionID, role, reason))
	if role == RoleSuperseded {
		run.stopSession(session, "superseded")
	}
}

// stopSession stops and archives a session, once.
func (run *utteranceRun) stopSession(session *runSession, reason string) {
	d := run.dispatcher
	run.mutex.Lock()
	if session.stopped {
		run.mutex.Unlock()
		return
	}
	session.stopped = true
	cancelStream := session.cancelStream
	run.mutex.Unlock()
	if cancelStream != nil {
		cancelStream()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := d.bridge.StopSession(ctx, session.sessionID); err != nil {
		run.recordError(fmt.Sprintf("stop %s (%s): %v", session.sessionID, reason, err))
	}
	if err := d.bridge.ArchiveSession(ctx, session.sessionID); err != nil {
		log.Printf("[dispatch] ERROR %s: archive %s: %v", run.utteranceID, session.sessionID, err)
	}
	if err := d.store.SetSessionStopped(session.sessionID, d.now().UnixMilli(), reason); err != nil {
		log.Printf("[dispatch] ERROR %s: %v", run.utteranceID, err)
	}
	run.mutex.Lock()
	session.turnFinished = true
	run.mutex.Unlock()
	run.checkDone()
}

// scheduleWindowExpiry makes the decisions final when no segment follows
// this one within the continuation window.
func (run *utteranceRun) scheduleWindowExpiry(segment Segment) {
	d := run.dispatcher
	window := d.settings().ContinuationWindow
	wait := time.UnixMilli(segment.SpeechEndedAtMs).Add(window).Sub(d.now())
	if wait < 0 {
		wait = 0
	}
	run.mutex.Lock()
	if run.windowTimer != nil {
		run.windowTimer.Stop()
	}
	run.windowTimer = time.AfterFunc(wait, func() {
		run.enqueue(func() {
			run.mutex.Lock()
			_, newerArrived := run.segments[segment.Index+1]
			handledThrough := run.nextIndex - 1
			run.mutex.Unlock()
			if newerArrived || handledThrough != segment.Index {
				return
			}
			run.finalize(fmt.Sprintf("no segment followed segment %d within the %d ms continuation window", segment.Index, window.Milliseconds()))
		})
	})
	run.mutex.Unlock()
}

// finalize makes every decision final: a session still speculative is kept.
func (run *utteranceRun) finalize(why string) {
	d := run.dispatcher
	run.mutex.Lock()
	if run.final {
		run.mutex.Unlock()
		return
	}
	run.final = true
	if run.windowTimer != nil {
		run.windowTimer.Stop()
	}
	var speculative []string
	for _, sessionID := range run.order {
		if run.sessions[sessionID].role == RoleSpeculative {
			speculative = append(speculative, sessionID)
		}
	}
	run.mutex.Unlock()
	for _, sessionID := range speculative {
		run.decide(sessionID, RoleKept, why)
	}
	if err := d.store.MarkUtteranceFinalized(run.utteranceID, d.now().UnixMilli()); err != nil {
		log.Printf("[dispatch] ERROR %s: %v", run.utteranceID, err)
	}
	run.checkDone()
}

func (run *utteranceRun) cancel() {
	d := run.dispatcher
	run.mutex.Lock()
	if run.cancelled {
		run.mutex.Unlock()
		return
	}
	run.cancelled = true
	if run.windowTimer != nil {
		run.windowTimer.Stop()
	}
	var sessions []*runSession
	for _, sessionID := range run.order {
		sessions = append(sessions, run.sessions[sessionID])
	}
	run.mutex.Unlock()
	for _, session := range sessions {
		run.stopSession(session, "cancelled")
	}
	if err := d.store.MarkUtteranceCancelled(run.utteranceID, d.now().UnixMilli()); err != nil {
		log.Printf("[dispatch] ERROR %s: %v", run.utteranceID, err)
	}
	run.checkDone()
}

// checkDone sends done once the utterance is cancelled, or its decisions are
// final and every session not superseded has finished its turn.
func (run *utteranceRun) checkDone() {
	run.mutex.Lock()
	ready := run.cancelled
	if run.final && !ready {
		ready = true
		for _, session := range run.sessions {
			if session.role != RoleSuperseded && !session.turnFinished {
				ready = false
				break
			}
		}
	}
	run.mutex.Unlock()
	if !ready || run.hub.isClosed() {
		return
	}
	run.hub.publish(eventDone, map[string]any{})
	run.stopOnce.Do(func() { close(run.stopped) })
}

func (run *utteranceRun) recordError(message string) {
	log.Printf("[dispatch] ERROR %s: %s", run.utteranceID, message)
	if err := run.dispatcher.store.AppendUtteranceError(run.utteranceID, message); err != nil {
		log.Printf("[dispatch] ERROR %s: record error: %v", run.utteranceID, err)
	}
	run.hub.publish(eventError, map[string]any{"message": message})
}

// ReceiveAudio keeps the recording and shares it into every session the
// utterance still keeps, and into any session it starts later.
func (d *Dispatcher) ReceiveAudio(utterance Utterance, filename, contentType string, content []byte) ([]string, error) {
	directory := filepath.Join(d.audioDirectory, utterance.UtteranceID)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return nil, err
	}
	if err := os.WriteFile(filepath.Join(directory, filename), content, 0o600); err != nil {
		return nil, err
	}
	if err := d.store.RecordUtteranceAudio(utterance.UtteranceID, filename, contentType, int64(len(content)), d.now().UnixMilli()); err != nil {
		return nil, err
	}
	run, err := d.runFor(utterance)
	if err != nil {
		return nil, err
	}
	audio := &utteranceAudio{filename: filename, contentType: contentType, content: content}
	run.mutex.Lock()
	run.audio = audio
	var targets []*runSession
	for _, sessionID := range run.order {
		session := run.sessions[sessionID]
		if session.role != RoleSuperseded && !session.stopped {
			targets = append(targets, session)
		}
	}
	run.mutex.Unlock()
	shared := []string{}
	var failures []string
	for _, session := range targets {
		if err := run.shareAudio(session, audio); err != nil {
			failures = append(failures, err.Error())
			continue
		}
		shared = append(shared, session.sessionID)
	}
	if len(failures) > 0 {
		return shared, errors.New(strings.Join(failures, "; "))
	}
	return shared, nil
}

func (run *utteranceRun) shareAudio(session *runSession, audio *utteranceAudio) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := run.dispatcher.bridge.ShareFile(ctx, session.sessionID, audio.filename, audio.contentType, audio.content); err != nil {
		message := fmt.Sprintf("share %s into %s: %v", audio.filename, session.sessionID, err)
		run.recordError(message)
		return errors.New(message)
	}
	return nil
}
