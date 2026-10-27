// Command speech-server keeps speech models in memory for voice-intake: a
// Parakeet TDT model answering whisper-server's POST /inference shape, with
// word times added, and a Silero voice activity detector that scores a live
// audio stream window by window.
package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/kayushkin/llm-bridge/msg"
	"github.com/kayushkin/llm-bridge/servicesettings"
)

const (
	settingListenAddress  = "listen_address"
	settingParakeetModel  = "parakeet_model"
	settingVADModel       = "voice_activity_model"
	settingThreads        = "threads"
	maximumAudioBytes     = 64 << 20
	streamIdleLimit       = 10 * time.Minute
	pcmBytesPerSample     = 2
	expectedSampleRate    = 16000
	expectedBitsPerSample = 16
)

func settingDefinitions() []servicesettings.Definition {
	wiring, path, behaviour := msg.ServiceSettingKindWiring, msg.ServiceSettingKindPath, msg.ServiceSettingKindBehaviour
	text, integer := msg.ServiceSettingValueTypeString, msg.ServiceSettingValueTypeInteger
	return []servicesettings.Definition{
		{Key: settingListenAddress, EnvironmentVariable: "SPEECH_SERVER_ADDR", Kind: wiring, ValueType: text, Default: "127.0.0.1:8335",
			Description: "The address the HTTP server listens on. Keep it on loopback."},
		{Key: settingParakeetModel, EnvironmentVariable: "SPEECH_SERVER_PARAKEET_MODEL", Kind: path, ValueType: text, Required: true,
			Description: "A ggml Parakeet TDT model file, kept in memory and used by POST /inference."},
		{Key: settingVADModel, EnvironmentVariable: "SPEECH_SERVER_VOICE_ACTIVITY_MODEL", Kind: path, ValueType: text, Required: true,
			Description: "A ggml Silero VAD model file; each audio stream loads its own copy."},
		{Key: settingThreads, EnvironmentVariable: "SPEECH_SERVER_THREADS", Kind: behaviour, ValueType: integer, Default: "4",
			Description: "CPU threads one transcription uses."},
	}
}

type server struct {
	transcriber *ParakeetTranscriber
	vadModel    string
	mutex       sync.Mutex
	streams     map[string]*VoiceActivityStream
}

func main() {
	registry, err := servicesettings.New("speech-server", []string{"SPEECH_SERVER_"}, settingDefinitions(), servicesettings.ProcessEnvironment())
	if err != nil {
		log.Fatalf("speech-server: %v", err)
	}
	if err := registry.CheckRequired(); err != nil {
		log.Fatalf("speech-server: %v", err)
	}
	started := time.Now()
	transcriber, err := LoadParakeetTranscriber(registry.String(settingParakeetModel), registry.Integer(settingThreads))
	if err != nil {
		log.Fatalf("speech-server: %v", err)
	}
	vadModel := registry.String(settingVADModel)
	probe, err := OpenVoiceActivityStream(vadModel)
	if err != nil {
		log.Fatalf("speech-server: %v", err)
	}
	probe.Close()
	log.Printf("speech-server: models loaded in %s", time.Since(started).Round(time.Millisecond))

	s := &server{transcriber: transcriber, vadModel: vadModel, streams: map[string]*VoiceActivityStream{}}
	go s.closeIdleStreams()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.Handle("GET /settings", servicesettings.Handler(registry, "/settings"))
	mux.HandleFunc("POST /inference", s.inference)
	mux.HandleFunc("POST /vad/streams/{stream_id}", s.scoreStream)
	mux.HandleFunc("DELETE /vad/streams/{stream_id}", s.closeStream)
	address := registry.String(settingListenAddress)
	log.Printf("speech-server listening on %s", address)
	httpServer := &http.Server{Addr: address, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	log.Fatal(httpServer.ListenAndServe())
}

// inference takes the form whisper-server takes (multipart field "file", a
// 16 kHz mono 16-bit WAV) and answers {"text", "words", "transcription_ms"}.
func (s *server) inference(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maximumAudioBytes)
	file, _, err := r.FormFile("file")
	if err != nil {
		writeError(w, http.StatusBadRequest, "multipart field \"file\" is required: "+err.Error())
		return
	}
	defer file.Close()
	wav, err := io.ReadAll(file)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	samples, err := wavSamples(wav)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	started := time.Now()
	text, words, err := s.transcriber.Transcribe(samples)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"text": text, "words": words, "transcription_ms": time.Since(started).Milliseconds()})
}

// scoreStream takes raw 16 kHz mono 16-bit little-endian PCM, the next bytes
// of the stream, and answers the speech probability of every window they
// complete: {"window_samples", "first_window", "probabilities"}.
func (s *server) scoreStream(w http.ResponseWriter, r *http.Request) {
	streamID := r.PathValue("stream_id")
	pcm, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maximumAudioBytes))
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if len(pcm)%pcmBytesPerSample != 0 {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("the body is %d bytes, not a whole number of 16-bit samples", len(pcm)))
		return
	}
	stream, err := s.stream(streamID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	firstWindow, probabilities, err := stream.Score(pcmToFloat(pcm))
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"window_samples": sileroWindowSamples, "first_window": firstWindow, "probabilities": probabilities})
}

func (s *server) stream(streamID string) (*VoiceActivityStream, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	stream, ok := s.streams[streamID]
	if !ok {
		opened, err := OpenVoiceActivityStream(s.vadModel)
		if err != nil {
			return nil, err
		}
		stream = opened
		s.streams[streamID] = stream
	}
	stream.lastUsedAtUnix = time.Now().Unix()
	return stream, nil
}

func (s *server) closeStream(w http.ResponseWriter, r *http.Request) {
	s.mutex.Lock()
	stream, ok := s.streams[r.PathValue("stream_id")]
	delete(s.streams, r.PathValue("stream_id"))
	s.mutex.Unlock()
	if !ok {
		writeError(w, http.StatusNotFound, "no stream "+r.PathValue("stream_id"))
		return
	}
	stream.Close()
	writeJSON(w, http.StatusOK, map[string]string{"status": "closed"})
}

func (s *server) closeIdleStreams() {
	for range time.Tick(time.Minute) {
		cutoff := time.Now().Add(-streamIdleLimit).Unix()
		s.mutex.Lock()
		for streamID, stream := range s.streams {
			if stream.lastUsedAtUnix < cutoff {
				stream.Close()
				delete(s.streams, streamID)
				log.Printf("speech-server: closed stream %s, idle for %s", streamID, streamIdleLimit)
			}
		}
		s.mutex.Unlock()
	}
}

func pcmToFloat(pcm []byte) []float32 {
	samples := make([]float32, len(pcm)/pcmBytesPerSample)
	for index := range samples {
		samples[index] = float32(int16(binary.LittleEndian.Uint16(pcm[index*2:]))) / 32768
	}
	return samples
}

// wavSamples reads a 16 kHz mono 16-bit PCM WAV.
func wavSamples(wav []byte) ([]float32, error) {
	if len(wav) < 12 || !bytes.Equal(wav[0:4], []byte("RIFF")) || !bytes.Equal(wav[8:12], []byte("WAVE")) {
		return nil, errors.New("the file is not a WAV: it does not start with RIFF…WAVE")
	}
	haveFormat := false
	for offset := 12; offset+8 <= len(wav); {
		chunkID := string(wav[offset : offset+4])
		chunkSize := int(binary.LittleEndian.Uint32(wav[offset+4:]))
		start := offset + 8
		switch chunkID {
		case "fmt ":
			if start+16 > len(wav) {
				return nil, errors.New("the WAV fmt chunk is too short")
			}
			format, channels := binary.LittleEndian.Uint16(wav[start:]), binary.LittleEndian.Uint16(wav[start+2:])
			rate, bits := binary.LittleEndian.Uint32(wav[start+4:]), binary.LittleEndian.Uint16(wav[start+14:])
			if format != 1 || channels != 1 || rate != expectedSampleRate || bits != expectedBitsPerSample {
				return nil, fmt.Errorf("the WAV is format %d, %d channels, %d Hz, %d bits; this server takes PCM, 1 channel, 16000 Hz, 16 bits", format, channels, rate, bits)
			}
			haveFormat = true
		case "data":
			if !haveFormat {
				return nil, errors.New("the WAV data chunk comes before its fmt chunk")
			}
			end := len(wav)
			if start+chunkSize < end {
				end = start + chunkSize
			}
			data := wav[start:end]
			return pcmToFloat(data[:len(data)/2*2]), nil
		}
		offset = start + chunkSize + chunkSize%2
	}
	return nil, errors.New("the WAV has no data chunk")
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		log.Printf("speech-server: encode answer: %v", err)
	}
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}
