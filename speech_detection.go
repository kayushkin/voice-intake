package voiceintake

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// SpeechDetector scores streamed audio for speech, window by window. A
// stream keeps its state between calls, so each call carries only the bytes
// after the last.
type SpeechDetector interface {
	// Score takes the next 16 kHz mono 16-bit PCM bytes of a stream and
	// returns the speech probability of each window they complete.
	Score(ctx context.Context, streamID string, pcm []byte) (DetectorScores, error)
	// Close ends a stream.
	Close(ctx context.Context, streamID string) error
}

// DetectorScores is one Score answer.
type DetectorScores struct {
	WindowSamples int       `json:"window_samples"`
	FirstWindow   int       `json:"first_window"`
	Probabilities []float64 `json:"probabilities"`
}

// SpeechServerDetector calls speech-server's Silero detector.
type SpeechServerDetector struct {
	baseURL string
	client  *http.Client
}

// NewSpeechServerDetector builds a detector for speech-server at baseURL.
func NewSpeechServerDetector(baseURL string) *SpeechServerDetector {
	return &SpeechServerDetector{baseURL: strings.TrimRight(baseURL, "/"), client: &http.Client{Timeout: 10 * time.Second}}
}

// Score posts the bytes to POST /vad/streams/{id}.
func (d *SpeechServerDetector) Score(ctx context.Context, streamID string, pcm []byte) (DetectorScores, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, d.baseURL+"/vad/streams/"+url.PathEscape(streamID), bytes.NewReader(pcm))
	if err != nil {
		return DetectorScores{}, err
	}
	request.Header.Set("Content-Type", "application/octet-stream")
	response, err := d.client.Do(request)
	if err != nil {
		return DetectorScores{}, fmt.Errorf("speech detector: %w", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return DetectorScores{}, fmt.Errorf("speech detector: read answer: %w", err)
	}
	if response.StatusCode != http.StatusOK {
		return DetectorScores{}, fmt.Errorf("speech detector answered %d: %s", response.StatusCode, strings.TrimSpace(string(body)))
	}
	var scores DetectorScores
	if err := json.Unmarshal(body, &scores); err != nil {
		return DetectorScores{}, fmt.Errorf("speech detector: decode answer: %w", err)
	}
	if scores.WindowSamples <= 0 {
		return DetectorScores{}, fmt.Errorf("speech detector answered window_samples %d", scores.WindowSamples)
	}
	return scores, nil
}

// Close deletes the stream.
func (d *SpeechServerDetector) Close(ctx context.Context, streamID string) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodDelete, d.baseURL+"/vad/streams/"+url.PathEscape(streamID), nil)
	if err != nil {
		return err
	}
	response, err := d.client.Do(request)
	if err != nil {
		return fmt.Errorf("speech detector: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusNotFound {
		body, _ := io.ReadAll(response.Body)
		return fmt.Errorf("speech detector answered %d to close: %s", response.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}

// Thresholds of the endpointer, Silero's own: speech begins when a window
// scores at least speechStartProbability, and a window below
// speechContinueProbability counts as silence.
const (
	speechStartProbability    = 0.5
	speechContinueProbability = 0.35
	// minimumSpeechMs is the shortest speech kept as a segment; anything
	// shorter (a click, a tick, a cough) is dropped.
	minimumSpeechMs = 200
)

// endpointerEvent is what one window changed.
type endpointerEvent struct {
	// HearingChanged is set when the detector starts or stops hearing
	// speech; Hearing is the new state.
	HearingChanged bool
	Hearing        bool
	// SegmentEnded is set when silence has lasted long enough to end a
	// segment of speech; the segment is samples [StartSample, EndSample).
	SegmentEnded bool
	StartSample  int
	EndSample    int
}

// endpointer turns window probabilities into speech segments. With
// silenceWindows zero it never ends a segment on a pause: the caller ends
// it (long mode, manual strategy).
type endpointer struct {
	windowSamples   int
	silenceWindows  int
	minimumWindows  int
	hearing         bool
	open            bool
	startWindow     int
	lastSpeechIndex int
	everHeard       bool
}

func newEndpointer(windowSamples int, silence time.Duration) *endpointer {
	windowMs := float64(windowSamples) * 1000 / segmentAudioSampleRate
	silenceWindows := 0
	if silence > 0 {
		silenceWindows = int(float64(silence.Milliseconds())/windowMs + 0.999)
	}
	return &endpointer{windowSamples: windowSamples, silenceWindows: silenceWindows, minimumWindows: int(minimumSpeechMs/windowMs + 0.999)}
}

// feed scores one window, numbered from 0 at the start of the stream.
func (e *endpointer) feed(window int, probability float64) endpointerEvent {
	var event endpointerEvent
	switch {
	case probability >= speechStartProbability || (e.hearing && probability >= speechContinueProbability):
		if !e.open {
			e.open = true
			e.startWindow = window
		}
		e.lastSpeechIndex = window
		if !e.hearing {
			e.hearing = true
			event.HearingChanged, event.Hearing = true, true
		}
	default:
		if e.hearing {
			e.hearing = false
			event.HearingChanged, event.Hearing = true, false
		}
		if e.open && e.silenceWindows > 0 && window-e.lastSpeechIndex >= e.silenceWindows {
			event = e.close(event)
		}
	}
	return event
}

// close ends the open segment at the end of its last speech window. A
// segment shorter than minimumSpeechMs is dropped and reports no end.
func (e *endpointer) close(event endpointerEvent) endpointerEvent {
	if !e.open {
		return event
	}
	e.open = false
	if e.lastSpeechIndex+1-e.startWindow < e.minimumWindows {
		return event
	}
	e.everHeard = true
	event.SegmentEnded = true
	event.StartSample = e.startWindow * e.windowSamples
	event.EndSample = (e.lastSpeechIndex + 1) * e.windowSamples
	return event
}

// finish ends any open segment, as when the microphone stops. A segment
// still hearing speech ends at endSample, the end of the audio received.
func (e *endpointer) finish(endSample int) endpointerEvent {
	if !e.open {
		return endpointerEvent{}
	}
	hearing := e.hearing
	event := e.close(endpointerEvent{})
	if event.SegmentEnded && hearing && endSample > event.EndSample {
		event.EndSample = endSample
	}
	e.hearing = false
	return event
}

// speechInProgress reports whether a segment is open: speech heard and not
// yet ended by silence.
func (e *endpointer) speechInProgress() bool { return e.open }
