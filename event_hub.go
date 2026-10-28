package voiceintake

import (
	"context"
	"sync"
)

// UtteranceEvent is one Server-Sent Event of GET /api/voice/utterances/{id}/events.
type UtteranceEvent struct {
	Name string
	Data any
}

// Event names, as the contract spells them. eventError is the one addition:
// a failure the phone would otherwise only see as silence.
const (
	eventDispatch     = "dispatch"
	eventDecision     = "decision"
	eventSessionEvent = "session_event"
	eventDone         = "done"
	eventError        = "error"
	// Contract v2, for streaming utterances.
	eventHearing       = "hearing"
	eventTranscript    = "transcript"
	eventStopRecording = "stop_recording"
	// Held words: the person confirms them, or they are never sent.
	eventAwaitingConfirmation = "awaiting_confirmation"
	eventConfirmationEnded    = "confirmation_ended"
)

// eventHub keeps every event of one utterance, so a subscriber that connects
// late reads the whole history before the live events, and none is dropped.
type eventHub struct {
	mutex   sync.Mutex
	history []UtteranceEvent
	changed chan struct{}
	closed  bool
}

func newEventHub() *eventHub {
	return &eventHub{changed: make(chan struct{})}
}

// publish appends an event; after close it does nothing.
func (hub *eventHub) publish(name string, data any) {
	hub.mutex.Lock()
	defer hub.mutex.Unlock()
	if hub.closed {
		return
	}
	hub.history = append(hub.history, UtteranceEvent{Name: name, Data: data})
	close(hub.changed)
	hub.changed = make(chan struct{})
	if name == eventDone {
		hub.closed = true
	}
}

func (hub *eventHub) isClosed() bool {
	hub.mutex.Lock()
	defer hub.mutex.Unlock()
	return hub.closed
}

// follow calls deliver for every event from the first, waiting for new ones,
// until done is delivered or ctx ends.
func (hub *eventHub) follow(ctx context.Context, deliver func(UtteranceEvent) error) error {
	next := 0
	for {
		hub.mutex.Lock()
		pending := append([]UtteranceEvent(nil), hub.history[next:]...)
		changed := hub.changed
		hub.mutex.Unlock()
		for _, event := range pending {
			if err := deliver(event); err != nil {
				return err
			}
			next++
			if event.Name == eventDone {
				return nil
			}
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
