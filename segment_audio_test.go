package voiceintake

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"
)

// testWAV is a 16 kHz mono 16-bit WAV whose sample bytes are the transcript
// the fake whisper-server will read back out of it.
func testWAV(transcript string) []byte {
	data := []byte(transcript)
	if len(data)%2 == 1 {
		data = append(data, ' ')
	}
	var wav bytes.Buffer
	write := func(value any) { _ = binary.Write(&wav, binary.LittleEndian, value) }
	wav.WriteString("RIFF")
	write(uint32(36 + len(data)))
	wav.WriteString("WAVEfmt ")
	write(uint32(16))
	write(uint16(1))
	write(uint16(1))
	write(uint32(16000))
	write(uint32(32000))
	write(uint16(2))
	write(uint16(16))
	wav.WriteString("data")
	write(uint32(len(data)))
	wav.Write(data)
	return wav.Bytes()
}

// fakeWhisperServer answers like whisper-server's inference route, with the
// WAV's sample bytes as the text, laid out on two lines as whisper does. The
// transcript "FAIL" is a 500.
func fakeWhisperServer(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/inference" || r.FormValue("response_format") != "json" || r.FormValue("temperature") != "0" {
		http.Error(w, "unexpected request", http.StatusBadRequest)
		return
	}
	file, _, err := r.FormFile("file")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	wav, _ := io.ReadAll(file)
	text := strings.TrimSpace(string(wav[44:]))
	if text == "FAIL" {
		http.Error(w, `{"error":"the model fell over"}`, http.StatusInternalServerError)
		return
	}
	words := strings.Fields(text)
	if len(words) > 2 {
		text = " " + strings.Join(words[:2], " ") + "\n " + strings.Join(words[2:], " ") + "\n"
	}
	_ = json.NewEncoder(w).Encode(map[string]string{"text": text})
}

func (h *harness) postSegmentAudio(utteranceID string, index int, wav []byte, speechEndedAtMs int64) (int, map[string]any) {
	h.t.Helper()
	path := h.server.URL + "/api/voice/utterances/" + utteranceID + "/segments/" + strconv.Itoa(index) + "/audio"
	if speechEndedAtMs != 0 {
		path += "?speech_ended_at_ms=" + strconv.FormatInt(speechEndedAtMs, 10)
	}
	request, _ := http.NewRequest("POST", path, bytes.NewReader(wav))
	request.Header.Set("Content-Type", "audio/wav")
	request.Header.Set("X-Client-Now-Ms", strconv.FormatInt(time.Now().UnixMilli(), 10))
	request.Header.Set("X-Principal-Id", testPrincipal)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		h.t.Fatal(err)
	}
	defer response.Body.Close()
	answer := map[string]any{}
	raw, _ := io.ReadAll(response.Body)
	_ = json.Unmarshal(raw, &answer)
	return response.StatusCode, answer
}

func TestSegmentAudioIsTranscribedAndDispatchedLikeText(t *testing.T) {
	h := newHarness(t, nil)
	utteranceID := h.createUtterance("audio-weather")
	now := time.Now().UnixMilli()
	status, answer := h.postSegmentAudio(utteranceID, 0, testWAV("what's the weather like"), now-200)
	if status != http.StatusAccepted || answer["text"] != "what's the weather like" || answer["status"] != "accepted" {
		t.Fatalf("audio segment 0 = %d %v", status, answer)
	}
	if _, ok := answer["transcription_ms"].(float64); !ok {
		t.Fatalf("no transcription_ms in %v", answer)
	}
	// The same index again is not transcribed or dispatched again.
	if status, answer := h.postSegmentAudio(utteranceID, 0, testWAV("something else entirely"), now); status != http.StatusOK || answer["status"] != "already received" {
		t.Fatalf("repeated audio segment 0 = %d %v", status, answer)
	}
	// A text segment with the same index is refused the same way.
	if status := h.postSegment(utteranceID, 0, "other words", now); status != http.StatusOK {
		t.Fatalf("text segment over an audio one = %d", status)
	}
	h.call("POST", "/api/voice/utterances/"+utteranceID+"/finish", map[string]any{"ended_at_ms": now}, testPrincipal)
	events := h.followUntilDone(utteranceID)
	var dispatched []string
	for _, event := range events {
		if event.name == eventDispatch {
			dispatched = append(dispatched, event.data["text"].(string))
		}
	}
	if len(dispatched) != 1 || dispatched[0] != "what's the weather like" {
		t.Fatalf("dispatched %v", dispatched)
	}

	status, record := h.call("GET", "/api/voice/utterances/"+utteranceID, nil, testPrincipal)
	if status != 200 {
		t.Fatalf("get = %d", status)
	}
	segment := record["segments"].([]any)[0].(map[string]any)
	if segment["source"] != "audio" || segment["transcript_raw"] != " what's the\n weather like\n" || segment["transcription_ms"] == nil || segment["audio_duration_ms"] == nil {
		t.Fatalf("segment record %v", segment)
	}
	status, latency := h.call("GET", "/api/voice/latency?group_by=source", nil, testPrincipal)
	groups := latency["groups"].([]any)
	if status != 200 || len(groups) != 1 || groups[0].(map[string]any)["group"] != "audio" ||
		groups[0].(map[string]any)["samples"].(map[string]any)["transcription"].(float64) != 1 {
		t.Fatalf("latency = %d %v", status, latency)
	}
}

func TestAnAudioSegmentWithNoWordsHoldsItsIndexAndDispatchesNothing(t *testing.T) {
	h := newHarness(t, nil)
	utteranceID := h.createUtterance("audio-silence")
	now := time.Now().UnixMilli()
	status, answer := h.postSegmentAudio(utteranceID, 0, testWAV("[BLANK_AUDIO]"), now-500)
	if status != http.StatusOK || answer["status"] != "no words" || answer["text"] != "" {
		t.Fatalf("silent segment = %d %v", status, answer)
	}
	// Segment 1 is not kept waiting for segment 0, and goes out as the
	// utterance's first request.
	status, answer = h.postSegmentAudio(utteranceID, 1, testWAV("remind me to call the plumber"), now)
	if status != http.StatusAccepted {
		t.Fatalf("segment 1 = %d %v", status, answer)
	}
	h.call("POST", "/api/voice/utterances/"+utteranceID+"/finish", map[string]any{"ended_at_ms": now}, testPrincipal)
	events := h.followUntilDone(utteranceID)
	var dispatched []any
	for _, event := range events {
		if event.name == eventDispatch {
			dispatched = append(dispatched, event.data["segment_indexes"], event.data["text"])
		}
	}
	if len(dispatched) != 2 || dispatched[1] != "remind me to call the plumber" || len(dispatched[0].([]any)) != 1 || dispatched[0].([]any)[0].(float64) != 1 {
		t.Fatalf("dispatched %v", dispatched)
	}
}

func TestAnUtteranceWhoseOnlySegmentHadNoWordsFinishes(t *testing.T) {
	h := newHarness(t, nil)
	utteranceID := h.createUtterance("audio-only-silence")
	now := time.Now().UnixMilli()
	if status, answer := h.postSegmentAudio(utteranceID, 0, testWAV(""), 0); status != http.StatusOK || answer["status"] != "no words" {
		t.Fatalf("empty segment = %d %v", status, answer)
	}
	h.call("POST", "/api/voice/utterances/"+utteranceID+"/finish", map[string]any{"ended_at_ms": now}, testPrincipal)
	for _, event := range h.followUntilDone(utteranceID) {
		if event.name == eventDispatch {
			t.Fatalf("dispatched %v", event.data)
		}
	}
}

func TestAFailedTranscriptionIsReportedAndLeavesTheIndexFree(t *testing.T) {
	h := newHarness(t, nil)
	utteranceID := h.createUtterance("audio-failure")
	now := time.Now().UnixMilli()
	status, answer := h.postSegmentAudio(utteranceID, 0, testWAV("FAIL"), now)
	if status != http.StatusBadGateway || !strings.Contains(answer["error"].(string), "500") {
		t.Fatalf("failed transcription = %d %v", status, answer)
	}
	_, record := h.call("GET", "/api/voice/utterances/"+utteranceID, nil, testPrincipal)
	if errors := record["errors"].([]any); len(errors) != 1 || !strings.Contains(errors[0].(string), "transcription failed") {
		t.Fatalf("errors %v", record["errors"])
	}
	// The phone may send segment 0 again.
	if status, answer := h.postSegmentAudio(utteranceID, 0, testWAV("call the plumber"), now); status != http.StatusAccepted {
		t.Fatalf("retry = %d %v", status, answer)
	}
}

func TestSegmentAudioThatIsNotSixteenKilohertzMonoPCMIsRefused(t *testing.T) {
	h := newHarness(t, nil)
	utteranceID := h.createUtterance("audio-bad")
	stereo := testWAV("hello there")
	binary.LittleEndian.PutUint16(stereo[22:], 2)
	if status, answer := h.postSegmentAudio(utteranceID, 0, stereo, 0); status != http.StatusBadRequest || !strings.Contains(answer["error"].(string), "2 channels") {
		t.Fatalf("stereo = %d %v", status, answer)
	}
	if status, _ := h.postSegmentAudio(utteranceID, 0, []byte("not audio at all"), 0); status != http.StatusBadRequest {
		t.Fatalf("not a WAV = %d", status)
	}
}

func TestSpokenTextJoinsWhisperLinesAndDropsNonSpeechMarkers(t *testing.T) {
	for raw, want := range map[string]string{
		" And so, my fellow\n Americans!\n": "And so, my fellow Americans!",
		" [BLANK_AUDIO]\n":                  "",
		"(silence)":                         "",
		"":                                  "",
		"[laughs] okay then":                "[laughs] okay then",
	} {
		if got := spokenText(raw); got != want {
			t.Errorf("spokenText(%q) = %q, want %q", raw, got, want)
		}
	}
}

func TestASegmentWithNoWordsDoesNotKeepTheWindowFromMakingDecisionsFinal(t *testing.T) {
	h := newHarness(t, map[string]string{"VOICE_INTAKE_CONTINUATION_WINDOW_MS": "300"})
	utteranceID := h.createUtterance("audio-trailing-silence")
	now := time.Now().UnixMilli()
	if status, answer := h.postSegmentAudio(utteranceID, 0, testWAV("what's the weather like"), now); status != http.StatusAccepted {
		t.Fatalf("segment 0 = %d %v", status, answer)
	}
	if status, answer := h.postSegmentAudio(utteranceID, 1, testWAV(""), now+50); status != http.StatusOK {
		t.Fatalf("segment 1 = %d %v", status, answer)
	}
	// No finish: the window passing alone must end the utterance.
	roles := decisionsOf(h.followUntilDone(utteranceID))
	if len(roles) != 1 {
		t.Fatalf("decisions %v", roles)
	}
	for _, role := range roles {
		if role != RoleKept {
			t.Fatalf("decisions %v", roles)
		}
	}
}
