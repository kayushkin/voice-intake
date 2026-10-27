package voiceintake

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"regexp"
	"strings"
)

// SpeechTranscriber turns a segment's audio into text.
type SpeechTranscriber interface {
	// Transcribe returns the transcript exactly as the transcription service
	// gave it. An empty transcript is not an error.
	Transcribe(ctx context.Context, wav []byte) (string, error)
}

// WhisperServerTranscriber calls a whisper.cpp whisper-server's inference
// route: multipart field "file", JSON answer {"text": …}.
type WhisperServerTranscriber struct {
	inferenceURL string
	client       *http.Client
}

// NewWhisperServerTranscriber builds a transcriber for the inference route at
// inferenceURL, for example http://127.0.0.1:<port>/inference.
func NewWhisperServerTranscriber(inferenceURL string) *WhisperServerTranscriber {
	return &WhisperServerTranscriber{inferenceURL: inferenceURL, client: &http.Client{}}
}

// Transcribe sends the WAV at temperature 0. The audio context is left at
// whisper's full 30 seconds: a context sized to a short clip was three times
// faster when measured but repeated sentences and turned silence into words.
func (t *WhisperServerTranscriber) Transcribe(ctx context.Context, wav []byte) (string, error) {
	var body bytes.Buffer
	form := multipart.NewWriter(&body)
	file, err := form.CreateFormFile("file", "segment.wav")
	if err != nil {
		return "", err
	}
	if _, err := file.Write(wav); err != nil {
		return "", err
	}
	for field, value := range map[string]string{"response_format": "json", "temperature": "0"} {
		if err := form.WriteField(field, value); err != nil {
			return "", err
		}
	}
	if err := form.Close(); err != nil {
		return "", err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, t.inferenceURL, &body)
	if err != nil {
		return "", err
	}
	request.Header.Set("Content-Type", form.FormDataContentType())
	response, err := t.client.Do(request)
	if err != nil {
		return "", fmt.Errorf("transcription service: %w", err)
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return "", fmt.Errorf("transcription service: read answer: %w", err)
	}
	if response.StatusCode != http.StatusOK {
		return "", fmt.Errorf("transcription service answered %d: %s", response.StatusCode, strings.TrimSpace(string(raw)))
	}
	var answer struct {
		Text  *string `json:"text"`
		Error string  `json:"error"`
	}
	if err := json.Unmarshal(raw, &answer); err != nil {
		return "", fmt.Errorf("transcription service: answer is not JSON: %v", err)
	}
	if answer.Error != "" {
		return "", fmt.Errorf("transcription service: %s", answer.Error)
	}
	if answer.Text == nil {
		return "", errors.New("transcription service: answer has no text field")
	}
	return *answer.Text, nil
}

// nonSpeechMarker matches a transcript that is only a marker whisper writes
// for audio without words, such as "[BLANK_AUDIO]" or "(silence)".
var nonSpeechMarker = regexp.MustCompile(`^(\[[^\]]*\]|\([^)]*\))$`)

// spokenText is the words of a raw transcript: whitespace, including the line
// breaks whisper puts between its own segments, becomes single spaces, and a
// transcript that is only a non-speech marker is empty.
func spokenText(rawTranscript string) string {
	text := strings.Join(strings.Fields(rawTranscript), " ")
	if nonSpeechMarker.MatchString(text) {
		return ""
	}
	return text
}

// segmentAudioSampleRate etc. are the one format the segment audio route takes:
// what the phone records, and what whisper-server reads without converting.
const (
	segmentAudioSampleRate    = 16000
	segmentAudioChannels      = 1
	segmentAudioBitsPerSample = 16
)

// wavDurationMs checks that wav is a 16 kHz mono 16-bit PCM WAV file and
// returns the length of its audio.
func wavDurationMs(wav []byte) (int64, error) {
	if len(wav) < 12 || string(wav[0:4]) != "RIFF" || string(wav[8:12]) != "WAVE" {
		return 0, errors.New("the body is not a WAV file: it does not start with RIFF…WAVE")
	}
	var haveFormat bool
	offset := 12
	for offset+8 <= len(wav) {
		chunkID := string(wav[offset : offset+4])
		chunkSize := int(binary.LittleEndian.Uint32(wav[offset+4 : offset+8]))
		chunkStart := offset + 8
		switch chunkID {
		case "fmt ":
			if chunkSize < 16 || chunkStart+16 > len(wav) {
				return 0, errors.New("the WAV fmt chunk is too short")
			}
			audioFormat := binary.LittleEndian.Uint16(wav[chunkStart:])
			channels := binary.LittleEndian.Uint16(wav[chunkStart+2:])
			sampleRate := binary.LittleEndian.Uint32(wav[chunkStart+4:])
			bitsPerSample := binary.LittleEndian.Uint16(wav[chunkStart+14:])
			if audioFormat != 1 || channels != segmentAudioChannels || sampleRate != segmentAudioSampleRate || bitsPerSample != segmentAudioBitsPerSample {
				return 0, fmt.Errorf("the WAV is format %d, %d channels, %d Hz, %d bits; this route takes PCM (1), %d channel, %d Hz, %d bits",
					audioFormat, channels, sampleRate, bitsPerSample, segmentAudioChannels, segmentAudioSampleRate, segmentAudioBitsPerSample)
			}
			haveFormat = true
		case "data":
			if !haveFormat {
				return 0, errors.New("the WAV data chunk comes before its fmt chunk")
			}
			// A recorder that streams may leave the size unset or too large;
			// count what is actually there.
			dataBytes := len(wav) - chunkStart
			if chunkSize < dataBytes {
				dataBytes = chunkSize
			}
			bytesPerSecond := segmentAudioSampleRate * segmentAudioChannels * segmentAudioBitsPerSample / 8
			return int64(dataBytes) * 1000 / int64(bytesPerSecond), nil
		}
		offset = chunkStart + chunkSize + chunkSize%2
	}
	return 0, errors.New("the WAV has no data chunk")
}
