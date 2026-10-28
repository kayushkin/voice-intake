package voiceintake

import (
	"encoding/binary"
	"math"
	"net/http"
	"strings"
	"testing"
)

func sinePCM(frequencyHertz float64, milliseconds int, amplitude float64) []byte {
	var pcm []byte
	for sample := range milliseconds * 16 {
		value := amplitude * math.Sin(2*math.Pi*frequencyHertz*float64(sample)/segmentAudioSampleRate)
		pcm = binary.LittleEndian.AppendUint16(pcm, uint16(int16(value)))
	}
	return pcm
}

// rootMeanSquare skips the first 100 ms, while the filter settles.
func rootMeanSquare(pcm []byte) float64 {
	sum, count := 0.0, 0
	for offset := 1600 * 2; offset+1 < len(pcm); offset += 2 {
		value := float64(int16(binary.LittleEndian.Uint16(pcm[offset:])))
		sum += value * value
		count++
	}
	return math.Sqrt(sum / float64(count))
}

func TestTheHighPassFilterCutsRumbleAndKeepsSpeechFrequencies(t *testing.T) {
	for _, check := range []struct {
		frequencyHertz           float64
		minimumKept, maximumKept float64
	}{
		{20, 0, 0.1},  // wind and handling rumble
		{40, 0, 0.25}, // the -12 dB/octave slope below an 80 Hz cutoff
		{300, 0.95, 1.01},
		{1000, 0.99, 1.01},
	} {
		input := sinePCM(check.frequencyHertz, 1000, 10000)
		kept := rootMeanSquare(newHighPassFilter(80).filter(input)) / rootMeanSquare(input)
		if kept < check.minimumKept || kept > check.maximumKept {
			t.Errorf("%v Hz: kept %.3f of the level, want %.2f..%.2f", check.frequencyHertz, kept, check.minimumKept, check.maximumKept)
		}
	}
}

func TestTheHighPassFilterGivesTheSameSamplesChunkByChunkAsAllAtOnce(t *testing.T) {
	input := append(sinePCM(30, 300, 9000), sinePCM(500, 300, 4000)...)
	whole := newHighPassFilter(80).filter(input)
	chunked := newHighPassFilter(80)
	var pieces []byte
	// Odd chunk sizes split samples across calls.
	for offset := 0; offset < len(input); offset += 333 {
		pieces = append(pieces, chunked.filter(input[offset:min(len(input), offset+333)])...)
	}
	if string(pieces) != string(whole) {
		t.Fatalf("chunked output differs from whole output (%d and %d bytes)", len(pieces), len(whole))
	}
	if newHighPassFilter(0) != nil {
		t.Fatal("a zero cutoff must mean no filter")
	}
}

func TestAWeaklyHeardSegmentIsKeptWithNoWordsAndItsReasonAndNotTranscribed(t *testing.T) {
	// The fake detector scores speech 0.9: below a 0.95 minimum.
	h, queue := newStreamingHarness(t, map[string]string{"VOICE_INTAKE_ENDPOINT_SILENCE_MS": "300",
		"VOICE_INTAKE_MINIMUM_SEGMENT_MEAN_SPEECH_PROBABILITY": "0.95"}, "mm-hmm")
	utteranceID := h.createStreamingUtterance("weak-1", "quick", "endpoint")
	h.streamAll(utteranceID, pcmOf(audioPart{false, 300}, audioPart{true, 600}, audioPart{false, 500}))
	h.stop(utteranceID, StopReasonUser)
	events := h.followUntilDone(utteranceID)
	if queue.calls != 0 {
		t.Fatalf("transcriber calls = %d, want 0", queue.calls)
	}
	transcripts := eventsNamed(events, eventTranscript)
	if len(transcripts) != 1 || transcripts[0].data["text"] != "" || !strings.HasPrefix(transcripts[0].data["dropped_reason"].(string), "weak speech") {
		t.Fatalf("transcript events = %v", transcripts)
	}
	if _, ids, _, _ := h.bridge.snapshot(); len(ids) != 0 {
		t.Fatalf("sessions = %v, want none", ids)
	}
	_, record := h.call("GET", "/api/voice/utterances/"+utteranceID, nil, testPrincipal)
	segment := record["segments"].([]any)[0].(map[string]any)
	if segment["text"] != "" || segment["mean_speech_probability"].(float64) < 0.89 || segment["voiced_ms"].(float64) < 500 ||
		!strings.Contains(segment["dropped_reason"].(string), SettingMinimumSegmentMeanSpeechProbability) {
		t.Fatalf("segment = %v", segment)
	}
}

func TestASurelyHeardSegmentPassesTheWeakSpeechMinimum(t *testing.T) {
	h, queue := newStreamingHarness(t, map[string]string{"VOICE_INTAKE_ENDPOINT_SILENCE_MS": "300",
		"VOICE_INTAKE_MINIMUM_SEGMENT_MEAN_SPEECH_PROBABILITY": "0.85"}, "what's the weather like")
	utteranceID := h.createStreamingUtterance("strong-1", "quick", "endpoint")
	h.streamAll(utteranceID, pcmOf(audioPart{false, 300}, audioPart{true, 600}, audioPart{false, 500}))
	h.stop(utteranceID, StopReasonUser)
	h.followUntilDone(utteranceID)
	if _, ids, sent, _ := h.bridge.snapshot(); queue.calls != 1 || len(ids) != 1 || sent[ids[0]][0] != "what's the weather like" {
		t.Fatalf("transcriber calls %d, sessions %v, sent %v", queue.calls, ids, sent)
	}
}

func TestTheNoiseGuardSettingsAreReadAndPatched(t *testing.T) {
	h := newHarness(t, nil)
	status, answer := h.call("PATCH", "/api/voice/settings", map[string]any{"high_pass_cutoff_hz": 100, "minimum_segment_mean_speech_probability": 0.7}, testPrincipal)
	if status != http.StatusOK {
		t.Fatalf("patch = %d %v", status, answer)
	}
	_, settings := h.call("GET", "/api/voice/settings", nil, testPrincipal)
	if settings["high_pass_cutoff_hz"] != float64(100) || settings["minimum_segment_mean_speech_probability"] != 0.7 {
		t.Fatalf("settings = %v", settings)
	}
	if status, _ := h.call("PATCH", "/api/voice/settings", map[string]any{"minimum_segment_mean_speech_probability": 1.5}, testPrincipal); status != http.StatusBadRequest {
		t.Fatalf("a probability above 1 = %d, want 400", status)
	}
}
