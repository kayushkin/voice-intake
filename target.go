package voiceintake

import (
	"errors"
	"fmt"
	"strings"
)

// Where an utterance's words go: the target on POST /api/voice/utterances.
const (
	// TargetPool is today's behaviour and the default: a pooled voice
	// session, or a fresh one like it, with segments split and judged.
	TargetPool = "pool"
	// TargetNewSession starts an ordinary chat session, as the chat's New
	// button would, instead of a voice session.
	TargetNewSession = "new_session"
	// TargetSession sends the words as the next message of a session the
	// person already has.
	TargetSession = "session"
	// TargetSignal answers an open question signal with the words.
	TargetSignal = "signal"
)

var utteranceTargetKinds = map[string]bool{TargetPool: true, TargetNewSession: true, TargetSession: true, TargetSignal: true}

// UtteranceTarget says where an utterance's words go.
type UtteranceTarget struct {
	Kind      string `json:"kind"`
	SessionID string `json:"session_id,omitempty"`
	SignalID  string `json:"signal_id,omitempty"`
}

// PoolTarget is the target of an utterance that names none.
var PoolTarget = UtteranceTarget{Kind: TargetPool}

// validate says what is wrong with a target the phone sent, if anything.
func (target UtteranceTarget) validate() error {
	if !utteranceTargetKinds[target.Kind] {
		return fmt.Errorf("target.kind %q is not one of %s", target.Kind, sortedKeys(utteranceTargetKinds))
	}
	var problems []string
	if target.Kind == TargetSession && strings.TrimSpace(target.SessionID) == "" {
		problems = append(problems, "target.session_id is required for kind session")
	}
	if target.Kind != TargetSession && target.SessionID != "" {
		problems = append(problems, "target.session_id is only for kind session")
	}
	if target.Kind == TargetSignal && strings.TrimSpace(target.SignalID) == "" {
		problems = append(problems, "target.signal_id is required for kind signal")
	}
	if target.Kind != TargetSignal && target.SignalID != "" {
		problems = append(problems, "target.signal_id is only for kind signal")
	}
	if len(problems) > 0 {
		return errors.New(strings.Join(problems, "; "))
	}
	return nil
}

// deliversOneMessage reports whether the utterance's words go, joined, as
// one message once the phone has stopped, rather than segment by segment.
// A session or a question the person already has gets one message: there is
// nothing to split speculatively, and two half-sentences would be two turns.
func (target UtteranceTarget) deliversOneMessage() bool {
	return target.Kind == TargetSession || target.Kind == TargetSignal
}

// heldForConfirmation reports whether the target can hold its words for the
// person to confirm. The pool cannot: quick questions stay instant.
func (target UtteranceTarget) heldForConfirmation() bool {
	return target.Kind == TargetSession || target.Kind == TargetSignal || target.Kind == TargetNewSession
}

// Errors a bridge returns about a target.
var (
	// ErrTargetNotFound is a session or signal the bridge does not have, or
	// one that belongs to someone else: the bridge answers both alike.
	ErrTargetNotFound = errors.New("not found")
	// ErrSignalNotAnswerable is a signal that is not an open question.
	ErrSignalNotAnswerable = errors.New("not an open question")
)

// OpenQuestion is a question signal still waiting for an answer, with every
// open question that resolves together with it.
type OpenQuestion struct {
	SignalID  string
	SessionID string
	Title     string
	// GroupSignalIDs holds SignalID and its open siblings: the questions of
	// one AskUserQuestion call, or of one turn, which the bridge resolves
	// only all at once.
	GroupSignalIDs []string
}
