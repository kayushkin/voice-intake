package voiceintake

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
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

// interruptedTurnEndWait is how long a follow-up waits, after interrupting
// the session's running turn, for that turn's own result before it is sent.
// The bridge ends an interrupted turn with a result of its own, 50 to 80 ms
// later when measured on 2026-11-25; sent before it, the new message's
// echo comes first and that result would read as its reply.
var interruptedTurnEndWait = 2 * time.Second

// confirmationLimit is how long held words wait for the person to confirm
// them before they expire, unsent.
var confirmationLimit = 2 * time.Minute

// Errors of ConfirmHeldWords.
var (
	ErrNothingAwaitingConfirmation = errors.New("no words are awaiting confirmation")
	ErrConfirmationTextDiffers     = errors.New("the text does not match the words held")
)

// sessionStatesThatEndATurn are bridge states after which no result will
// come for the message sent.
var sessionStatesThatEndATurn = map[msg.SessionState]bool{msg.SessionAborted: true, msg.SessionError: true, msg.SessionDisconnected: true}

// Dispatcher sends an utterance's segments to its one session and follows
// the replies.
type Dispatcher struct {
	store          *Store
	bridge         SessionBridge
	addresseeJudge AddresseeJudge
	pool           *SessionPool
	settings       func() DispatchSettings
	now            func() time.Time
	audioDirectory string
	// detector and transcribers serve streaming utterances; nil and empty
	// when none are set.
	detector     SpeechDetector
	transcribers Transcribers
	// replayRecordingDirectory holds the recordings replays read.
	replayRecordingDirectory string

	mutex sync.Mutex
	runs  map[string]*utteranceRun
}

// NewDispatcher builds a dispatcher.
func NewDispatcher(store *Store, bridge SessionBridge, addresseeJudge AddresseeJudge, pool *SessionPool, settings func() DispatchSettings, now func() time.Time, audioDirectory string) *Dispatcher {
	return &Dispatcher{store: store, bridge: bridge, addresseeJudge: addresseeJudge, pool: pool, settings: settings, now: now, audioDirectory: audioDirectory,
		runs: map[string]*utteranceRun{}}
}

// SetSpeechServices gives the dispatcher the detector and transcribers
// streaming utterances use.
func (d *Dispatcher) SetSpeechServices(detector SpeechDetector, transcribers Transcribers) {
	d.detector = detector
	d.transcribers = transcribers
}

// Dispatch modes of a replay configuration.
const (
	DispatchSession = "session"
	DispatchOneshot = "oneshot"
	DispatchNone    = "none"
)

// dispatchOverrides replace the settings for one utterance: a replay runs
// under its configuration, not under what live speech uses.
type dispatchOverrides struct {
	replayID         string
	mode             string
	instanceID       string
	model            string
	effort           string
	workingDirectory string
	maxBudgetUSD     float64
	// warmSessionID is a session started before the replay, taken by the
	// first dispatch, as a pooled one would be.
	warmSessionID string
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
	// ownedByVoiceIntake is false for a session the person already had (a
	// session or signal target): it is never stopped or archived from here.
	ownedByVoiceIntake bool
	// awaitingOwnMessage holds back the events of a session the person
	// already had until the message this utterance sent shows on its
	// stream: a new subscriber is first sent the session's last turn again,
	// whose result is not the reply to these words.
	awaitingOwnMessage bool
	// awaitedText, when set, is the message awaitingOwnMessage waits for:
	// only a user_message with this text ends the wait. The bridge can echo
	// one message twice, about a second apart, so the echo of an earlier
	// message must not end it.
	awaitedText string
	// discarding drops every event: the running turn was interrupted and
	// its last events are still arriving.
	discarding bool
	// resultsSeen counts the result events read, passed on or not.
	resultsSeen int
	// turnMessageIDs are the assistant message ids of the turn now running,
	// and interruptedMessageIDs those of turns that were interrupted: a
	// block or result that carries one of the latter is that turn's, however
	// late it comes, and is never passed on. The bridge's turn_id cannot
	// tell them apart: after an interrupt it stamps the old turn's last
	// events with the new turn's id.
	turnMessageIDs        map[string]bool
	interruptedMessageIDs map[string]bool
	// reading is true while a goroutine reads the session's events.
	reading bool
	// replyForwarded is true once an event of the turn now running has been
	// passed on to the phone.
	replyForwarded bool
	// heldEvents are events of the turn now running, kept back while the
	// person is still speaking: more words may change the request.
	heldEvents []json.RawMessage
	// withheld drops the session's events: the addressee check judged the
	// words were not meant for the assistant.
	withheld bool
	// followUpSequence is the session_follow_ups row of the turn now
	// running; zero while the first message's turn runs.
	followUpSequence int64
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

	mutex     sync.Mutex
	segments  map[int]Segment
	nextIndex int
	// conversation is the utterance's one session (targets pool and
	// new_session): the first segment with words starts it, and every later
	// one goes into it as its next message.
	conversation *runSession
	// sentSegments counts the segments with words sent to conversation.
	sentSegments int
	// segmentLimitReported is set once the limit of segments has been
	// reported, so it is reported once.
	segmentLimitReported bool
	sessions             map[string]*runSession
	order                []string
	final                bool
	canceled             bool
	windowTimer          *time.Timer
	audio                *utteranceAudio
	mode                 string
	trigger              string
	stream               *audioStream
	overrides            *dispatchOverrides
	// speechOpen is true while the stream's detector hears a segment of
	// speech not yet ended; segmentsCut counts the segments the stream has
	// cut. While speech is open, or a cut segment is not handled yet, the
	// session's events are held back (replyHeldBack).
	speechOpen  bool
	segmentsCut int
	// finishSeen is set once Finish has handled every segment: no more
	// words will come.
	finishSeen bool
	// target is where the words go. spokenTexts holds every segment with
	// words handled so far, in order: a session or signal target sends them
	// as one message when the utterance finishes, the addressee check reads
	// them, and they are the words held for confirmation.
	target          UtteranceTarget
	spokenTexts     []string
	targetDelivered bool
	// confirmBeforeSend holds the joined words at finish until the person
	// confirms them; confirmationTimer expires them.
	confirmBeforeSend bool
	held              bool
	confirmationTimer *time.Timer
	// confirmationOpen is true while held words wait for the person: done
	// is not sent until that ends.
	confirmationOpen bool
	// addresseeCheckRunning is true while the addressee check reads the
	// words; addresseeCheckStale is set when more words came meanwhile, so
	// it runs again on all of them. notForAssistant is set once it judged
	// the words were not meant for the assistant, with its reason and model.
	addresseeCheckRunning bool
	addresseeCheckStale   bool
	notForAssistant       bool
	notForAssistantReason string
	notForAssistantModel  string
}

// collectsOneMessage reports whether the segments are kept and sent joined
// once, rather than dispatched one by one: a session or signal target, or
// any target whose words wait for confirmation.
func (run *utteranceRun) collectsOneMessage() bool {
	return run.target.deliversOneMessage() || run.confirmBeforeSend
}

// runFor returns the live run of an utterance, building it from the store
// when this process has none (the utterance was made before a restart).
func (d *Dispatcher) runFor(utterance Utterance) (*utteranceRun, error) {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	if run, ok := d.runs[utterance.UtteranceID]; ok {
		return run, nil
	}
	target := utterance.Target
	if target.Kind == "" {
		target = PoolTarget
	}
	run := &utteranceRun{dispatcher: d, utteranceID: utterance.UtteranceID, principalID: utterance.PrincipalID, hub: newEventHub(), mode: utterance.Mode, trigger: utterance.Trigger,
		jobs: make(chan func(), 64), stopped: make(chan struct{}), segments: map[int]Segment{}, sessions: map[string]*runSession{}, target: target,
		confirmBeforeSend: utterance.ConfirmBeforeSend && target.heldForConfirmation()}
	if err := run.loadFromStore(utterance); err != nil {
		return nil, err
	}
	d.runs[utterance.UtteranceID] = run
	go run.work()
	if utterance.ConfirmationState == ConfirmationAwaiting && utterance.ConfirmationExpiresAtMs != nil {
		// Words held by an earlier process still wait, until their own
		// expiry.
		run.awaitConfirmation(utterance.ConfirmationText, *utterance.ConfirmationExpiresAtMs)
	}
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
			role: session.Role, turnFinished: true, stopped: session.StoppedAtMs != nil, ownedByVoiceIntake: run.target.Kind != TargetSession && run.target.Kind != TargetSignal}
		run.order = append(run.order, session.SessionID)
		run.hub.publish(eventDispatch, dispatchEventData(session.SessionID, session.SegmentIndexes, session.Text))
		if !run.target.deliversOneMessage() && session.Role != RoleSuperseded {
			run.conversation = run.sessions[session.SessionID]
			run.sentSegments = len(session.SegmentIndexes) + len(session.FollowUps)
		}
	}
	decisions, err := run.dispatcher.store.Decisions(utterance.UtteranceID)
	if err != nil {
		return err
	}
	for _, decision := range decisions {
		run.hub.publish(eventDecision, decisionEventData(decision.SessionID, decision.Role, decision.Reason))
	}
	run.final = utterance.FinalizedAtMs != nil
	run.canceled = utterance.CanceledAtMs != nil
	// A target utterance that reached a session has had its one message.
	run.targetDelivered = run.collectsOneMessage() && len(sessions) > 0
	run.held = utterance.ConfirmationState != ""
	if utterance.ConfirmationState == ConfirmationAwaiting {
		run.spokenTexts = []string{utterance.ConfirmationText}
	}
	if utterance.ConfirmationReason == ConfirmationReasonNotForAssistant {
		run.notForAssistant = true
		checks, err := run.dispatcher.store.AddresseeChecks(utterance.UtteranceID)
		if err != nil {
			return err
		}
		for _, check := range checks {
			if check.ForAssistant != nil && !*check.ForAssistant {
				run.notForAssistantReason, run.notForAssistantModel = check.Reason, check.Model
			}
		}
		if run.conversation != nil && utterance.ConfirmationState != ConfirmationConfirmed {
			run.conversation.withheld = true
		}
	}
	run.finishSeen = utterance.FinishedAtMs != nil || utterance.StoppedAtMs != nil
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

// Start makes the run for a new utterance and readies the person's pool:
// a waiting session the bridge has stopped is replaced now, while the
// person is still speaking.
func (d *Dispatcher) Start(utterance Utterance) error {
	if _, err := d.runFor(utterance); err != nil {
		return err
	}
	d.pool.Refresh(utterance.PrincipalID)
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
		run.mutex.Lock()
		run.finishSeen = true
		notForAssistant := run.notForAssistant
		run.mutex.Unlock()
		// Whatever happens next, a later checkDone must not wait on a
		// recording that has now stopped.
		defer run.checkDone()
		if run.confirmBeforeSend {
			// Nothing is sent, and nothing final, until the person confirms
			// the words, cancels them, or lets them expire.
			run.holdForConfirmation("")
			return
		}
		if notForAssistant {
			// The addressee check withheld the words while the recording
			// ran: now that every word is in, they wait for the person.
			run.holdForConfirmation(ConfirmationReasonNotForAssistant)
			return
		}
		if run.target.deliversOneMessage() {
			run.deliverToTarget()
		}
		run.finalize("the phone finished the utterance")
	})
	return nil
}

// holdForConfirmation keeps the joined words for the person to confirm and
// tells the phone. reason is empty for a hold the client asked for, and
// ConfirmationReasonNotForAssistant when the addressee check withheld the
// words. With no words there is nothing to confirm and the utterance ends.
// Run on the worker.
func (run *utteranceRun) holdForConfirmation(reason string) {
	d := run.dispatcher
	run.mutex.Lock()
	if run.held || run.canceled {
		run.mutex.Unlock()
		return
	}
	run.held = true
	text := strings.Join(run.spokenTexts, " ")
	target := run.target
	run.mutex.Unlock()
	if strings.TrimSpace(text) == "" {
		run.recordError(fmt.Sprintf("no words were heard, so nothing was held for confirmation or sent to the %s", target.Kind))
		run.endConfirmation()
		run.finalize("no words were heard")
		return
	}
	expiresAtMs := d.now().Add(confirmationLimit).UnixMilli()
	if err := d.store.HoldForConfirmation(run.utteranceID, text, expiresAtMs, reason); err != nil {
		run.recordError(fmt.Sprintf("hold the words for confirmation: %v; nothing was sent", err))
		run.endConfirmation()
		run.stopWithheldConversation("the words could not be held for confirmation")
		run.finalize("the words could not be held for confirmation")
		return
	}
	run.awaitConfirmation(text, expiresAtMs)
}

// awaitConfirmation tells the phone what is held and starts the expiry.
// Words the client asked to confirm go out as awaiting_confirmation; words
// the addressee check withheld go out as not_for_assistant, which a client
// must never send by itself.
func (run *utteranceRun) awaitConfirmation(text string, expiresAtMs int64) {
	d := run.dispatcher
	run.mutex.Lock()
	run.confirmationOpen = true
	notForAssistant, reason, model := run.notForAssistant, run.notForAssistantReason, run.notForAssistantModel
	run.mutex.Unlock()
	if notForAssistant {
		run.hub.publish(eventNotForAssistant, map[string]any{"text": text, "reason": reason, "model": model, "expires_at_ms": expiresAtMs})
	} else {
		run.hub.publish(eventAwaitingConfirmation, map[string]any{"text": text, "expires_at_ms": expiresAtMs})
	}
	wait := max(0, time.UnixMilli(expiresAtMs).Sub(d.now()))
	run.mutex.Lock()
	run.confirmationTimer = time.AfterFunc(wait, run.expireConfirmation)
	run.mutex.Unlock()
}

// endConfirmation records that held words stopped waiting, so done may be
// sent again.
func (run *utteranceRun) endConfirmation() {
	run.mutex.Lock()
	run.confirmationOpen = false
	if run.confirmationTimer != nil {
		run.confirmationTimer.Stop()
	}
	run.mutex.Unlock()
}

// stopWithheldConversation stops and archives the session of words the
// addressee check withheld and nobody sent anyway. Without such a session
// it does nothing.
func (run *utteranceRun) stopWithheldConversation(why string) {
	run.mutex.Lock()
	session := run.conversation
	withheld := run.notForAssistant && session != nil
	run.mutex.Unlock()
	if withheld {
		run.decide(session.sessionID, RoleSuperseded, why)
	}
}

// expireConfirmation ends held words nobody confirmed: recorded as
// expired, never sent.
func (run *utteranceRun) expireConfirmation() {
	d := run.dispatcher
	won, err := d.store.ResolveConfirmation(run.utteranceID, ConfirmationExpired, d.now().UnixMilli())
	if err != nil {
		run.recordError(fmt.Sprintf("expire the held words: %v", err))
		return
	}
	if !won {
		return
	}
	run.endConfirmation()
	run.hub.publish(eventConfirmationEnded, map[string]any{"state": ConfirmationExpired})
	run.enqueue(func() {
		why := fmt.Sprintf("nobody confirmed the words within %s, so nothing was sent", confirmationLimit)
		run.stopWithheldConversation("the addressee check judged the words were not meant for the assistant, and " + why)
		run.finalize(why)
	})
}

// ConfirmHeldWords sends the words the utterance holds. text, when given,
// must be the words held: the phone sends what it showed.
func (d *Dispatcher) ConfirmHeldWords(utterance Utterance, text *string) error {
	run, err := d.runFor(utterance)
	if err != nil {
		return err
	}
	stored, err := d.store.GetUtterance(utterance.UtteranceID)
	if err != nil {
		return err
	}
	if stored.ConfirmationState != ConfirmationAwaiting {
		return fmt.Errorf("%w (confirmation_state %q)", ErrNothingAwaitingConfirmation, stored.ConfirmationState)
	}
	if text != nil && strings.TrimSpace(*text) != strings.TrimSpace(stored.ConfirmationText) {
		return fmt.Errorf("%w: held %q, got %q", ErrConfirmationTextDiffers, stored.ConfirmationText, *text)
	}
	won, err := d.store.ResolveConfirmation(utterance.UtteranceID, ConfirmationConfirmed, d.now().UnixMilli())
	if err != nil {
		return err
	}
	if !won {
		return fmt.Errorf("%w (it ended as this confirm arrived)", ErrNothingAwaitingConfirmation)
	}
	run.endConfirmation()
	run.hub.publish(eventConfirmationEnded, map[string]any{"state": ConfirmationConfirmed})
	run.enqueue(func() {
		run.sendHeldWords()
		run.finalize("the person confirmed the words")
	})
	return nil
}

// sendHeldWords sends confirmed words: to the session or signal the phone
// named, to a new chat session, or, for words the addressee check withheld,
// into the utterance's one session. Run on the worker.
func (run *utteranceRun) sendHeldWords() {
	run.mutex.Lock()
	notForAssistant := run.notForAssistant
	run.mutex.Unlock()
	if notForAssistant {
		run.sendWithheldWords()
		return
	}
	if run.target.Kind != TargetNewSession {
		run.deliverToTarget()
		return
	}
	run.mutex.Lock()
	if run.targetDelivered || run.canceled {
		run.mutex.Unlock()
		return
	}
	run.targetDelivered = true
	text := strings.Join(run.spokenTexts, " ")
	indexes := run.spokenIndexes()
	run.mutex.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	// dispatch records its own failure; finalize then keeps nothing.
	_, _ = run.dispatch(ctx, indexes, text)
}

// spokenIndexes are the handled segments that had words, in order. The
// caller holds run.mutex.
func (run *utteranceRun) spokenIndexes() []int {
	var indexes []int
	for index, segment := range run.segments {
		if index < run.nextIndex && hasWords(segment) {
			indexes = append(indexes, index)
		}
	}
	sort.Ints(indexes)
	return indexes
}

// Cancel stops every session the utterance started, and discards words
// held for confirmation.
func (d *Dispatcher) Cancel(utterance Utterance) error {
	run, err := d.runFor(utterance)
	if err != nil {
		return err
	}
	won, err := d.store.ResolveConfirmation(utterance.UtteranceID, ConfirmationCanceled, d.now().UnixMilli())
	if err != nil {
		return err
	}
	if won {
		run.endConfirmation()
		run.hub.publish(eventConfirmationEnded, map[string]any{"state": ConfirmationCanceled})
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
		canceled := run.canceled
		run.mutex.Unlock()
		if !ready || canceled {
			return
		}
		// A segment with no words (an audio segment whose transcript came
		// back empty) holds its index so later ones are not kept waiting,
		// and changes nothing else: no dispatch, and the continuation
		// window still runs from the last segment that had words.
		spoken := hasWords(segment)
		if spoken {
			run.mutex.Lock()
			run.spokenTexts = append(run.spokenTexts, strings.TrimSpace(segment.Text))
			run.mutex.Unlock()
		}
		if spoken && !run.collectsOneMessage() {
			// A session or signal target, and any utterance whose words wait
			// for confirmation, sends nothing here: spokenTexts goes as one
			// message at finish, or at confirmation.
			run.mutex.Lock()
			overrides := run.overrides
			run.mutex.Unlock()
			switch {
			case overrides != nil && overrides.mode == DispatchNone:
			case overrides != nil && overrides.mode == DispatchOneshot:
				run.oneshot(overrides, segment)
			default:
				run.processSegment(segment)
			}
		}
		run.mutex.Lock()
		run.nextIndex = segment.Index + 1
		run.mutex.Unlock()
		// This segment is handled: a reply kept back while it was spoken
		// and transcribed may go out, unless more speech is on its way.
		run.releaseHeldReplies(false)
		if spoken && !run.collectsOneMessage() {
			run.scheduleWindowExpiry(segment)
		}
	}
}

func hasWords(segment Segment) bool { return strings.TrimSpace(segment.Text) != "" }

// previousSpokenSegment is the nearest segment before index that had words.
// The caller holds run.mutex.
func (run *utteranceRun) previousSpokenSegment(index int) (Segment, bool) {
	for earlier := index - 1; earlier >= 0; earlier-- {
		segment, known := run.segments[earlier]
		if !known {
			return Segment{}, false
		}
		if hasWords(segment) {
			return segment, true
		}
	}
	return Segment{}, false
}

// processSegment sends a segment with words to the utterance's one session:
// the first starts it, and each later one goes into it as its next message.
// The session has every earlier word, so it is the one to judge whether the
// new ones continue the request, change it or ask something new. Run on the
// worker.
func (run *utteranceRun) processSegment(segment Segment) {
	d := run.dispatcher
	settings := d.settings()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	run.mutex.Lock()
	conversation := run.conversation
	withheld := run.notForAssistant
	// A streamed recording counts its segments itself and is told to stop at
	// the limit; segments the phone cuts are counted here.
	limit := 0
	if run.mode != "long" && run.stream == nil && run.overrides == nil {
		limit = settings.MaximumQuickSegments
	}
	overLimit := limit > 0 && run.sentSegments >= limit
	alreadyReported := run.segmentLimitReported
	if overLimit {
		run.segmentLimitReported = true
	}
	run.mutex.Unlock()
	if withheld {
		// The addressee check withheld the words: these are kept in
		// spokenTexts with the rest, for the person to send anyway.
		return
	}
	if overLimit {
		if !alreadyReported {
			run.recordError(fmt.Sprintf("segment %d and any after it were not sent: one quick recording sends at most %d segments (%s)",
				segment.Index, limit, SettingMaximumQuickSegments))
		}
		return
	}

	if conversation == nil {
		session, err := run.dispatch(ctx, []int{segment.Index}, segment.Text)
		if err != nil {
			return
		}
		run.mutex.Lock()
		run.conversation = session
		run.sentSegments++
		run.mutex.Unlock()
		run.decide(session.sessionID, RoleKept, "one press, one session: every later segment goes into it as its next message")
	} else {
		if !run.sendIntoConversation(ctx, conversation, []int{segment.Index}, segment.Text) {
			return
		}
		run.mutex.Lock()
		run.sentSegments++
		run.mutex.Unlock()
	}
	run.checkAddressee()
}

// sendIntoConversation sends text into the utterance's one session as its
// next message. A turn still running is interrupted first and whatever of
// its reply was kept back is dropped: the new words may change the request,
// so the session answers all of it afresh. A turn that had finished keeps
// its reply. It reports whether the text went.
func (run *utteranceRun) sendIntoConversation(ctx context.Context, session *runSession, segmentIndexes []int, text string) bool {
	d := run.dispatcher
	run.mutex.Lock()
	if session.stopped || run.canceled || session.withheld {
		stopped := session.stopped
		run.mutex.Unlock()
		if stopped {
			run.recordError(fmt.Sprintf("segments %v were not sent: session %s has been stopped", segmentIndexes, session.sessionID))
		}
		// Withheld: the addressee check's verdict came as this segment was
		// on its way; its words are kept with the rest.
		return false
	}
	interruptedReply := !session.turnFinished
	resultsBefore := session.resultsSeen
	if interruptedReply {
		// Until the interrupted turn has ended, nothing of it is passed on.
		session.discarding = true
		if session.interruptedMessageIDs == nil {
			session.interruptedMessageIDs = map[string]bool{}
		}
		for messageID := range session.turnMessageIDs {
			session.interruptedMessageIDs[messageID] = true
		}
	} else if !session.withheld {
		// The earlier turn finished while the person was still speaking and
		// its reply was kept back. The session answers the new words as one
		// who has said it, so it goes to the phone first.
		for _, event := range session.heldEvents {
			run.hub.publish(eventSessionEvent, map[string]any{"session_id": session.sessionID, "event": event})
		}
	}
	// What is left is the draft of a turn about to be interrupted, or of
	// words withheld: it is dropped.
	session.heldEvents = nil
	session.turnMessageIDs = nil
	session.replyForwarded = false
	run.mutex.Unlock()

	if interruptedReply {
		if err := d.bridge.InterruptSession(ctx, session.sessionID); err != nil {
			// The turn ending by itself as the interrupt was sent is the
			// usual cause, and then there is nothing to interrupt.
			log.Printf("[dispatch] %s: interrupt %s before segments %v: %v", run.utteranceID, session.sessionID, segmentIndexes, err)
		} else {
			run.awaitInterruptedTurnEnd(session, resultsBefore)
		}
	}
	run.mutex.Lock()
	session.discarding = false
	// From here nothing is passed on until the message sent below shows on
	// the stream.
	session.awaitingOwnMessage = true
	session.awaitedText = strings.TrimSpace(text)
	session.turnFinished = false
	run.mutex.Unlock()
	sequence, err := d.store.InsertSessionFollowUp(run.utteranceID, SessionFollowUp{SessionID: session.sessionID, SegmentIndexes: segmentIndexes,
		Text: text, InterruptedReply: interruptedReply, SentAtMs: d.now().UnixMilli()})
	if err != nil {
		log.Printf("[dispatch] ERROR %s: record follow-up to %s: %v", run.utteranceID, session.sessionID, err)
	}
	run.mutex.Lock()
	session.followUpSequence = sequence
	run.mutex.Unlock()
	run.readSession(session)

	if err := d.bridge.SendMessage(ctx, session.sessionID, text); err != nil {
		run.recordError(fmt.Sprintf("segments %v: send to %s: %v", segmentIndexes, session.sessionID, err))
		if sequence != 0 {
			if storeErr := d.store.SetFollowUpError(sequence, err.Error()); storeErr != nil {
				log.Printf("[dispatch] ERROR %s: %v", run.utteranceID, storeErr)
			}
		}
		run.mutex.Lock()
		session.awaitingOwnMessage = false
		session.turnFinished = true
		run.mutex.Unlock()
		run.checkDone()
		return false
	}
	run.hub.publish(eventFollowUp, map[string]any{"session_id": session.sessionID, "segment_indexes": segmentIndexes, "text": text,
		"interrupted_reply": interruptedReply})
	run.mutex.Lock()
	withheldMeanwhile := session.withheld
	run.mutex.Unlock()
	if withheldMeanwhile {
		// The verdict's own interrupt may have reached the bridge before
		// this message did, and then the message started a turn nobody
		// stopped.
		if err := d.bridge.InterruptSession(ctx, session.sessionID); err != nil {
			log.Printf("[dispatch] %s: interrupt %s, whose words were withheld as a segment was sent: %v", run.utteranceID, session.sessionID, err)
		}
	}
	return true
}

// awaitInterruptedTurnEnd waits for the result that ends a turn just
// interrupted, so the next message is sent after it. resultsBefore is the
// count of results read when the turn was found running.
func (run *utteranceRun) awaitInterruptedTurnEnd(session *runSession, resultsBefore int) {
	deadline := time.Now().Add(interruptedTurnEndWait)
	for {
		run.mutex.Lock()
		ended := session.resultsSeen > resultsBefore || session.stopped || !session.reading
		run.mutex.Unlock()
		if ended {
			return
		}
		if time.Now().After(deadline) {
			log.Printf("[dispatch] %s: the interrupted turn of %s sent no result within %s; sending the next message anyway", run.utteranceID, session.sessionID, interruptedTurnEndWait)
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// readSession starts reading a session's events, unless a reader already
// runs, and waits a moment for the stream to open.
func (run *utteranceRun) readSession(session *runSession) {
	run.mutex.Lock()
	if session.reading {
		run.mutex.Unlock()
		return
	}
	session.reading = true
	streamContext, cancelStream := context.WithTimeout(context.Background(), sessionTurnLimit)
	session.cancelStream = cancelStream
	run.mutex.Unlock()
	connected, streamEnded := make(chan struct{}), make(chan struct{})
	go run.streamSession(streamContext, session, connected, streamEnded)
	select {
	case <-connected:
	case <-streamEnded:
	case <-time.After(streamOpenWait):
		log.Printf("[dispatch] %s: the event stream of %s did not open in %s; sending anyway", run.utteranceID, session.sessionID, streamOpenWait)
	}
}

// replyHeldBack reports whether session events wait instead of going to the
// phone: the person is speaking, or a segment already cut has not been
// handled, so words that may change the request are on their way. The
// caller holds run.mutex.
func (run *utteranceRun) replyHeldBack() bool {
	return run.speechOpen || run.segmentsCut > run.nextIndex
}

// releaseHeldReplies passes on the session events kept back while the
// person spoke, once nothing more is on its way; force passes them on
// whatever the state, for the end of the utterance.
func (run *utteranceRun) releaseHeldReplies(force bool) {
	run.mutex.Lock()
	defer run.mutex.Unlock()
	if !force && run.replyHeldBack() {
		return
	}
	for _, sessionID := range run.order {
		session := run.sessions[sessionID]
		held := session.heldEvents
		session.heldEvents = nil
		if len(held) == 0 || session.role == RoleSuperseded || session.stopped || session.withheld {
			continue
		}
		session.replyForwarded = true
		// Published under run.mutex, as streamSession does, so a held event
		// never goes out after a later one.
		for _, event := range held {
			run.hub.publish(eventSessionEvent, map[string]any{"session_id": session.sessionID, "event": event})
		}
	}
}

// speechHeard records whether the stream's detector has a segment of speech
// open, and lets held replies out when it closes with nothing cut.
func (run *utteranceRun) speechHeard(open bool) {
	run.mutex.Lock()
	changed := run.speechOpen != open
	run.speechOpen = open
	run.mutex.Unlock()
	if changed && !open {
		run.releaseHeldReplies(false)
	}
}

// segmentCut records that the stream has cut segment index, which will reach
// the worker once it is transcribed.
func (run *utteranceRun) segmentCut(index int) {
	run.mutex.Lock()
	if index+1 > run.segmentsCut {
		run.segmentsCut = index + 1
	}
	run.mutex.Unlock()
}

// checkAddressee asks, beside the session, whether the words so far were
// meant for the assistant. One check runs at a time; words that arrive
// while it runs make it run again on all of them. Typed words, replays and
// words the person has already confirmed are not checked.
func (run *utteranceRun) checkAddressee() {
	d := run.dispatcher
	run.mutex.Lock()
	if d.addresseeJudge == nil || run.overrides != nil || run.trigger == TriggerKeyboard || run.notForAssistant || run.held || run.canceled {
		run.mutex.Unlock()
		return
	}
	if run.addresseeCheckRunning {
		run.addresseeCheckStale = true
		run.mutex.Unlock()
		return
	}
	run.addresseeCheckRunning = true
	run.addresseeCheckStale = false
	text := strings.Join(run.spokenTexts, " ")
	run.mutex.Unlock()

	modelRole := d.settings().AddresseeCheckModelRole
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), addresseeCheckLimit)
		defer cancel()
		started := d.now()
		verdict, err := d.addresseeJudge.Judge(ctx, modelRole, text)
		check := AddresseeCheck{Text: text, ModelRole: modelRole, StartedAtMs: started.UnixMilli(), DurationMs: d.now().Sub(started).Milliseconds()}
		if err != nil {
			check.Error = err.Error()
		} else {
			forAssistant := verdict.ForAssistant
			check.ForAssistant, check.Reason, check.Model = &forAssistant, verdict.Reason, verdict.Model
		}
		if storeErr := d.store.InsertAddresseeCheck(run.utteranceID, check); storeErr != nil {
			log.Printf("[dispatch] ERROR %s: record addressee check: %v", run.utteranceID, storeErr)
		}
		run.mutex.Lock()
		run.addresseeCheckRunning = false
		stale := run.addresseeCheckStale
		run.mutex.Unlock()
		switch {
		case err != nil:
			// The words were sent as they were; the person is told the
			// check did not run, so its silence is not read as approval.
			run.recordError(fmt.Sprintf("the check of whether the words were meant for the assistant did not run, so they were sent unchecked: %v", err))
			if stale {
				run.checkAddressee()
			}
		case !verdict.ForAssistant:
			// Acted on here and now, not in the worker's queue: the worker
			// may be busy sending the next segments, which is what the
			// verdict is there to stop.
			run.withholdAsNotForAssistant(verdict)
		case stale:
			run.checkAddressee()
		}
	}()
}

// withholdAsNotForAssistant acts on the addressee check judging the words
// were not meant for the assistant: the session's turn is interrupted and
// its events dropped, no later segment is sent, the recording is told to
// stop, and once every word is in they wait for the person to send them
// anyway. It runs off the worker, the moment the verdict comes; only the
// hold itself is the worker's.
func (run *utteranceRun) withholdAsNotForAssistant(verdict AddresseeVerdict) {
	d := run.dispatcher
	run.mutex.Lock()
	if run.canceled || run.notForAssistant || run.held || run.hub.isClosed() {
		run.mutex.Unlock()
		return
	}
	run.notForAssistant = true
	run.notForAssistantReason, run.notForAssistantModel = verdict.Reason, verdict.Model
	// The words are about to be held: done must not go out in between, as
	// it would when the interrupted turn ends.
	run.confirmationOpen = true
	session := run.conversation
	interrupt := false
	if session != nil {
		session.withheld = true
		session.heldEvents = nil
		interrupt = !session.turnFinished && !session.stopped
	}
	stream := run.stream
	finishSeen := run.finishSeen
	run.mutex.Unlock()
	log.Printf("[dispatch] %s: the addressee check (%s) judged the words were not meant for the assistant: %s", run.utteranceID, verdict.Model, verdict.Reason)

	if interrupt {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		if err := d.bridge.InterruptSession(ctx, session.sessionID); err != nil {
			log.Printf("[dispatch] %s: interrupt %s, whose words were not meant for the assistant: %v", run.utteranceID, session.sessionID, err)
		}
		cancel()
	}
	if stream != nil && !finishSeen {
		// The recording, or its last transcriptions, still run: stop the
		// microphone, and Finish holds the words once every one is in.
		stream.requestStop(StopReasonNotForAssistant)
		return
	}
	run.enqueue(func() { run.holdForConfirmation(ConfirmationReasonNotForAssistant) })
}

// sendWithheldWords sends, into the utterance's one session, the words the
// addressee check withheld and the person chose to send anyway. Run on the
// worker.
func (run *utteranceRun) sendWithheldWords() {
	run.mutex.Lock()
	if run.targetDelivered || run.canceled {
		run.mutex.Unlock()
		return
	}
	run.targetDelivered = true
	text := strings.Join(run.spokenTexts, " ")
	indexes := run.spokenIndexes()
	session := run.conversation
	audio := run.audio
	run.mutex.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if session == nil {
		// The first segment's session could not be started; start it now.
		created, err := run.dispatch(ctx, indexes, text)
		if err != nil {
			return
		}
		run.mutex.Lock()
		run.conversation = created
		run.mutex.Unlock()
		run.decide(created.sessionID, RoleKept, "the person sent the words anyway")
		return
	}
	// The session was interrupted, or its reply dropped: the words go again,
	// whole, as its next message.
	run.mutex.Lock()
	session.withheld = false
	run.mutex.Unlock()
	if run.sendIntoConversation(ctx, session, indexes, text) && audio != nil {
		go run.shareAudio(session, audio)
	}
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
// events. The session's role is speculative until a decision says otherwise,
// which for the utterance's one session is at once.
func (run *utteranceRun) dispatch(ctx context.Context, segmentIndexes []int, text string) (*runSession, error) {
	d := run.dispatcher
	settings := d.settings()
	started := d.now()
	choice := run.sessionChoice(text, settings)

	sessionID, pooledState, pooled := "", "", false
	switch {
	case choice.warmSessionID != "":
		sessionID, pooled = choice.warmSessionID, true
		state, err := d.bridge.SessionState(ctx, sessionID)
		if err != nil {
			state = "unknown: " + err.Error()
		}
		pooledState = state
	case choice.usePool:
		sessionID, pooledState, pooled = d.pool.Take(ctx, run.principalID, run.utteranceID, settings)
	}
	effort := choice.effort
	switch {
	case choice.chatSession:
		created, instanceID, err := d.bridge.CreateChatSession(ctx, run.principalID, text)
		if err != nil {
			run.recordError(fmt.Sprintf("segments %v: start a chat session: %v", segmentIndexes, err))
			return nil, err
		}
		sessionID, choice.instanceID = created, instanceID
	case !pooled:
		created, err := d.bridge.CreateSession(ctx, SessionCreation{PrincipalID: run.principalID, InstanceID: choice.instanceID, Effort: effort,
			Model: choice.model, WorkingDirectory: choice.workingDirectory, DisplayName: text, MaxBudgetUSD: choice.maxBudgetUSD})
		if err != nil {
			run.recordError(fmt.Sprintf("segments %v: start a session: %v", segmentIndexes, err))
			return nil, err
		}
		sessionID = created
	}
	if choice.usePool {
		defer d.pool.Fill(run.principalID)
	}

	session := &runSession{sessionID: sessionID, segmentIndexes: segmentIndexes, text: text, role: RoleSpeculative, ownedByVoiceIntake: true}
	record := UtteranceSession{SessionID: sessionID, UtteranceID: run.utteranceID, SegmentIndexes: segmentIndexes, Text: text, Role: RoleSpeculative,
		Pooled: pooled, PooledSessionState: pooledState, Effort: effort, Model: choice.model, InstanceID: choice.instanceID,
		WorkingDirectory: choice.workingDirectory, DispatchStartedAtMs: started.UnixMilli()}
	if err := d.store.InsertUtteranceSession(record); err != nil {
		log.Printf("[dispatch] ERROR %s: record session %s: %v", run.utteranceID, sessionID, err)
	}

	run.mutex.Lock()
	canceled := run.canceled
	run.sessions[sessionID] = session
	run.order = append(run.order, sessionID)
	run.mutex.Unlock()
	if canceled {
		run.stopSession(session, "the utterance was canceled before its text was sent")
		return nil, errors.New("canceled")
	}
	run.readSession(session)

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

// deliverToTarget sends the utterance's words, joined, to the session or
// question the phone named, once, and follows that session's reply. Run on
// the worker after every segment received is handled.
func (run *utteranceRun) deliverToTarget() {
	d := run.dispatcher
	run.mutex.Lock()
	if run.targetDelivered || run.canceled {
		run.mutex.Unlock()
		return
	}
	run.targetDelivered = true
	text := strings.Join(run.spokenTexts, " ")
	indexes := run.spokenIndexes()
	target := run.target
	run.mutex.Unlock()
	if strings.TrimSpace(text) == "" {
		run.recordError(fmt.Sprintf("no words were heard, so nothing was sent to the %s the phone named", target.Kind))
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	started := d.now()

	sessionID := target.SessionID
	var question OpenQuestion
	if target.Kind == TargetSignal {
		var err error
		question, err = d.bridge.OpenQuestionOf(ctx, run.principalID, target.SignalID)
		if err != nil {
			run.recordError(fmt.Sprintf("answer signal %s: %v", target.SignalID, err))
			return
		}
		sessionID = question.SessionID
	}

	session := &runSession{sessionID: sessionID, segmentIndexes: indexes, text: text, role: RoleSpeculative, awaitingOwnMessage: true}
	record := UtteranceSession{SessionID: sessionID, UtteranceID: run.utteranceID, SegmentIndexes: indexes, Text: text, Role: RoleSpeculative,
		DispatchStartedAtMs: started.UnixMilli()}
	if err := d.store.InsertUtteranceSession(record); err != nil {
		log.Printf("[dispatch] ERROR %s: record session %s: %v", run.utteranceID, sessionID, err)
	}
	run.mutex.Lock()
	run.sessions[sessionID] = session
	run.order = append(run.order, sessionID)
	run.mutex.Unlock()
	run.readSession(session)

	var reason string
	var err error
	switch target.Kind {
	case TargetSession:
		err = d.bridge.SendMessageAsPerson(ctx, run.principalID, sessionID, text)
		reason = "sent as the next message of the session the phone named"
	case TargetSignal:
		answers := map[string]string{}
		for _, signalID := range question.GroupSignalIDs {
			answers[signalID] = text
		}
		var deliveredVia string
		deliveredVia, err = d.bridge.AnswerSignal(ctx, run.principalID, question.SignalID, answers)
		if err == nil && deliveredVia == "parked_hook" {
			// The answer went to a tool call still waiting in the turn now
			// running: no new message starts a turn, and that turn's
			// replayed events hold no result, so pass everything on.
			run.mutex.Lock()
			session.awaitingOwnMessage = false
			run.mutex.Unlock()
		}
		reason = fmt.Sprintf("the words answered signal %s (%q, %d question(s) in its group; delivered as %s)",
			question.SignalID, question.Title, len(question.GroupSignalIDs), deliveredVia)
	}
	if err != nil {
		run.recordError(fmt.Sprintf("send to %s %s: %v", target.Kind, sessionID, err))
		if storeErr := d.store.SetSessionError(sessionID, err.Error()); storeErr != nil {
			log.Printf("[dispatch] ERROR %s: %v", run.utteranceID, storeErr)
		}
		run.stopSession(session, "its text could not be sent")
		return
	}
	if err := d.store.SetSessionDispatched(sessionID, d.now().UnixMilli()); err != nil {
		log.Printf("[dispatch] ERROR %s: %v", run.utteranceID, err)
	}
	run.hub.publish(eventDispatch, dispatchEventData(sessionID, indexes, text))
	run.decide(sessionID, RoleKept, reason)
}

// sessionChoice is where and how one text's session runs.
type sessionChoice struct {
	instanceID       string
	model            string
	effort           string
	workingDirectory string
	maxBudgetUSD     float64
	usePool          bool
	warmSessionID    string
	// chatSession starts an ordinary chat session, with the chat's own
	// defaults, instead of a voice session (target new_session).
	chatSession bool
}

// sessionChoice reads it: a replay's configuration; else the raised
// instance, model and effort for long mode or an effort-raising phrase; else
// the defaults, from the pool when one is ready.
func (run *utteranceRun) sessionChoice(text string, settings DispatchSettings) sessionChoice {
	run.mutex.Lock()
	defer run.mutex.Unlock()
	if o := run.overrides; o != nil {
		choice := sessionChoice{instanceID: o.instanceID, model: o.model, effort: o.effort, workingDirectory: o.workingDirectory,
			maxBudgetUSD: o.maxBudgetUSD, warmSessionID: o.warmSessionID}
		o.warmSessionID = ""
		return choice
	}
	if run.target.Kind == TargetNewSession {
		// The chat's defaults decide model, effort and instance; the voice
		// settings do not apply to an ordinary chat.
		return sessionChoice{chatSession: true}
	}
	choice := sessionChoice{instanceID: settings.InstanceID, model: settings.DefaultModel, effort: effortFor(text, settings),
		workingDirectory: settings.WorkingDirectory}
	if run.mode == "long" || choice.effort != settings.DefaultEffort {
		choice.effort = settings.RaisedEffort
		if settings.RaisedInstanceID != "" {
			choice.instanceID = settings.RaisedInstanceID
		}
		if settings.RaisedModel != "" {
			choice.model = settings.RaisedModel
		}
		return choice
	}
	choice.usePool = true
	return choice
}

// streamSession reads one session's events for as long as the utterance
// lives: it records each turn's first block and result, and passes every
// event on unless the session was superseded, stopped or withheld. While
// the person is still speaking the events are kept back instead
// (replyHeldBack). It ends when the utterance is done, the session's
// stream ends or sessionTurnLimit passes.
func (run *utteranceRun) streamSession(ctx context.Context, session *runSession, connected, streamEnded chan struct{}) {
	d := run.dispatcher
	defer close(streamEnded)
	var connectOnce sync.Once
	err := d.bridge.StreamSessionEvents(ctx, session.sessionID, func() { connectOnce.Do(func() { close(connected) }) }, func(event BridgeEvent) bool {
		eventType := msg.EventType(event.Type)
		messageID, messageText := eventMessageIDAndText(event.Data)
		run.mutex.Lock()
		if eventType == msg.EventResult {
			session.resultsSeen++
		}
		// An interrupted turn's events: all of them while it ends, and any
		// that carry its message ids afterwards.
		ofInterruptedTurn := session.discarding ||
			((eventType == msg.EventBlock || eventType == msg.EventResult) && messageID != "" && session.interruptedMessageIDs[messageID])
		awaiting := session.awaitingOwnMessage
		if !ofInterruptedTurn && awaiting && eventType == msg.EventUserMessage &&
			(session.awaitedText == "" || strings.TrimSpace(messageText) == session.awaitedText) {
			session.awaitingOwnMessage = false
		}
		if !ofInterruptedTurn && !awaiting && eventType == msg.EventBlock && messageID != "" {
			if session.turnMessageIDs == nil {
				session.turnMessageIDs = map[string]bool{}
			}
			session.turnMessageIDs[messageID] = true
		}
		followUpSequence := session.followUpSequence
		run.mutex.Unlock()
		if ofInterruptedTurn || awaiting {
			// An interrupted turn still ending, the session's earlier turn
			// sent again to a new subscriber, or the message itself: none
			// is the reply.
			return true
		}
		at := d.now().UnixMilli()
		switch eventType {
		case msg.EventBlock:
			var err error
			if followUpSequence == 0 {
				err = d.store.SetSessionFirstBlock(session.sessionID, at)
			} else {
				err = d.store.SetFollowUpFirstBlock(followUpSequence, at)
			}
			if err != nil {
				log.Printf("[dispatch] ERROR %s: %v", run.utteranceID, err)
			}
		case msg.EventResult:
			text, cost := resultReply(event.Data)
			var err error
			if followUpSequence == 0 {
				if err = d.store.SetSessionResult(session.sessionID, at); err == nil {
					err = d.store.SetSessionReply(session.sessionID, text, cost)
				}
			} else {
				err = d.store.SetFollowUpResult(followUpSequence, at, text, cost)
			}
			if err != nil {
				log.Printf("[dispatch] ERROR %s: %v", run.utteranceID, err)
			}
		}
		run.mutex.Lock()
		if session.role != RoleSuperseded && !session.stopped && !session.withheld {
			if run.replyHeldBack() {
				session.heldEvents = append(session.heldEvents, event.Data)
			} else {
				session.replyForwarded = true
				// Published under run.mutex, as releaseHeldReplies does, so
				// a held event never goes out after a later one.
				run.hub.publish(eventSessionEvent, map[string]any{"session_id": session.sessionID, "event": event.Data})
			}
		}
		if eventType == msg.EventResult {
			session.turnFinished = true
		}
		run.mutex.Unlock()
		if eventType == msg.EventResult {
			// The session may get another message, so the stream stays
			// open; done closes it.
			run.checkDone()
			return true
		}
		if eventType == msg.EventSessionStatus && turnEndingStatus(event.Data) {
			return false
		}
		return true
	})
	run.mutex.Lock()
	stopped := session.stopped
	unfinished := !session.turnFinished
	session.turnFinished = true
	session.reading = false
	replay := run.overrides != nil
	run.mutex.Unlock()
	if replay && !stopped {
		// A replay's sessions are measurements: each is stopped and archived
		// once the utterance is over.
		defer run.stopSession(session, "the replay's turn finished")
	}
	if unfinished && !stopped {
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

// eventMessageIDAndText reads, from a llm-bridge-server event, the id of the
// message it belongs to and, for a user_message, the text sent.
func eventMessageIDAndText(data json.RawMessage) (messageID, text string) {
	var event struct {
		MessageID string `json:"message_id"`
		Result    *struct {
			Text string `json:"text"`
		} `json:"result"`
	}
	if err := json.Unmarshal(data, &event); err != nil {
		return "", ""
	}
	if event.Result != nil {
		text = event.Result.Text
	}
	return event.MessageID, text
}

// resultReply reads the reply text and the turn's cost from a result event
// as llm-bridge-server streams it: the event, with the result under "result".
func resultReply(data json.RawMessage) (string, *float64) {
	var event struct {
		Result *struct {
			Text string `json:"text"`
			Cost *struct {
				TotalUSD float64 `json:"total_usd"`
			} `json:"cost"`
		} `json:"result"`
	}
	if err := json.Unmarshal(data, &event); err != nil || event.Result == nil {
		return "", nil
	}
	if event.Result.Cost == nil {
		return event.Result.Text, nil
	}
	cost := event.Result.Cost.TotalUSD
	return event.Result.Text, &cost
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
	owned := session.ownedByVoiceIntake
	run.mutex.Unlock()
	if cancelStream != nil {
		cancelStream()
	}
	if !owned {
		// The person's own session: stop following it, and leave it running.
		if err := d.store.SetSessionStopped(session.sessionID, d.now().UnixMilli(), reason+"; stopped following it, and left the session as it was"); err != nil {
			log.Printf("[dispatch] ERROR %s: %v", run.utteranceID, err)
		}
		run.mutex.Lock()
		session.turnFinished = true
		run.mutex.Unlock()
		run.checkDone()
		return
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
// this one within the continuation window, for segments the phone cuts
// itself.
func (run *utteranceRun) scheduleWindowExpiry(segment Segment) {
	d := run.dispatcher
	run.mutex.Lock()
	streamed := run.stream != nil
	run.mutex.Unlock()
	if streamed {
		// A streamed recording ends by its own clock, in audio time: the
		// stream tells the phone to stop when the window closes, and the
		// phone's stop finishes the utterance. Finalizing here as well,
		// while the microphone was still open, sent done first on
		// 2026-11-17 and the stop never reached the phone.
		return
	}
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
			// Segments after this one that had no words do not count as
			// following it.
			run.mutex.Lock()
			followed := false
			for index, later := range run.segments {
				if index > segment.Index && (index >= run.nextIndex || hasWords(later)) {
					followed = true
					break
				}
			}
			run.mutex.Unlock()
			if followed {
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
	if run.canceled {
		run.mutex.Unlock()
		return
	}
	run.canceled = true
	if run.windowTimer != nil {
		run.windowTimer.Stop()
	}
	if run.stream != nil {
		go run.stream.cancel()
	}
	var sessions []*runSession
	for _, sessionID := range run.order {
		sessions = append(sessions, run.sessions[sessionID])
	}
	run.mutex.Unlock()
	for _, session := range sessions {
		run.stopSession(session, "canceled")
	}
	if err := d.store.MarkUtteranceCanceled(run.utteranceID, d.now().UnixMilli()); err != nil {
		log.Printf("[dispatch] ERROR %s: %v", run.utteranceID, err)
	}
	run.checkDone()
}

// checkDone sends done once the utterance is canceled, or its decisions are
// final, no held words wait, and every session not superseded has finished
// its turn. It never sends done while a streamed recording still runs: done
// closes the event stream, and the stop_recording that ends the recording
// would not reach the phone.
func (run *utteranceRun) checkDone() {
	run.mutex.Lock()
	canceled := run.canceled
	ready := canceled
	if run.final && !ready && !run.confirmationOpen {
		ready = true
		for _, session := range run.sessions {
			if session.role != RoleSuperseded && !session.turnFinished {
				ready = false
				break
			}
		}
	}
	stream := run.stream
	run.mutex.Unlock()
	if !ready || run.hub.isClosed() {
		return
	}
	if !canceled && stream != nil && !stream.isStopped() {
		return
	}
	// Nothing more will be said: whatever reply was kept back goes out.
	run.releaseHeldReplies(true)
	run.hub.publish(eventDone, map[string]any{})
	run.stopOnce.Do(func() { close(run.stopped) })
	run.mutex.Lock()
	var cancelStreams []context.CancelFunc
	for _, session := range run.sessions {
		if session.cancelStream != nil {
			cancelStreams = append(cancelStreams, session.cancelStream)
		}
	}
	run.mutex.Unlock()
	for _, cancelStream := range cancelStreams {
		cancelStream()
	}
}

// done is closed once the utterance has sent done.
func (run *utteranceRun) done() <-chan struct{} { return run.stopped }

// oneshotSystemPrompt is what a oneshot replay sends: the floor of what a
// voice answer costs, with no tools and no session.
const oneshotSystemPrompt = "You answer a spoken request; your reply is read aloud. Answer in one or two short plain sentences."

// oneshot answers a segment with one model call and records it on the
// replay.
func (run *utteranceRun) oneshot(overrides *dispatchOverrides, segment Segment) {
	d := run.dispatcher
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	started := d.now().UnixMilli()
	answer, err := d.bridge.InstanceOneshot(ctx, overrides.instanceID, overrides.model, oneshotSystemPrompt, segment.Text)
	answered := d.now().UnixMilli()
	if err != nil {
		run.recordError(fmt.Sprintf("segment %d: oneshot on %s: %v", segment.Index, overrides.instanceID, err))
		return
	}
	if err := d.store.SetReplayOneshot(overrides.replayID, started, answered, answer.Model, answer.Text); err != nil {
		log.Printf("[dispatch] ERROR %s: %v", run.utteranceID, err)
	}
}

func (run *utteranceRun) recordError(message string) {
	log.Printf("[dispatch] ERROR %s: %s", run.utteranceID, message)
	if err := run.dispatcher.store.AppendUtteranceError(run.utteranceID, message); err != nil {
		log.Printf("[dispatch] ERROR %s: record error: %v", run.utteranceID, err)
	}
	run.hub.publish(eventError, map[string]any{"message": message})
}

// RecordError writes a failure to the utterance's errors and sends it on the
// event stream, so the phone hears of it.
func (d *Dispatcher) RecordError(utterance Utterance, message string) error {
	run, err := d.runFor(utterance)
	if err != nil {
		return err
	}
	run.recordError(message)
	return nil
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
		if session.role != RoleSuperseded && !session.stopped && !session.withheld && session.ownedByVoiceIntake {
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
