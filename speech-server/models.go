package main

/*
#cgo LDFLAGS: -lparakeet -lwhisper -lggml -lggml-base
#include <stdlib.h>
#include <stdio.h>
#include "parakeet.h"
#include "whisper.h"

// quietLog passes on warnings and errors only: both libraries log every call
// at INFO, and the VAD is called several times a second per stream.
static void quietLog(enum ggml_log_level level, const char * text, void * user_data) {
	(void) user_data;
	if (level == GGML_LOG_LEVEL_WARN || level == GGML_LOG_LEVEL_ERROR) {
		fputs(text, stderr);
	}
}

static void quietLibraries(void) {
	parakeet_log_set(quietLog, NULL);
	whisper_log_set(quietLog, NULL);
}

static struct parakeet_context * loadParakeet(const char * path) {
	struct parakeet_context_params params = parakeet_context_default_params();
	params.use_gpu = false;
	return parakeet_init_from_file_with_params(path, params);
}

static int runParakeet(struct parakeet_context * context, const float * samples, int sampleCount, int threads) {
	struct parakeet_full_params params = parakeet_full_default_params(PARAKEET_SAMPLING_GREEDY);
	params.n_threads = threads;
	return parakeet_full(context, params, samples, sampleCount);
}

static struct whisper_vad_context * loadVoiceActivityDetector(const char * path) {
	struct whisper_vad_context_params params = whisper_vad_default_context_params();
	params.n_threads = 1;
	params.use_gpu = false;
	return whisper_vad_init_from_file_with_params(path, params);
}
*/
import "C"

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"unsafe"
)

func init() { C.quietLibraries() }

// Word is one recognised word and where it lies in the audio.
type Word struct {
	Text    string `json:"word"`
	StartMs int64  `json:"start_ms"`
	EndMs   int64  `json:"end_ms"`
}

// parakeetTimeUnitMs is the length of one unit of Parakeet's t0 and t1.
const parakeetTimeUnitMs = 10

// ParakeetTranscriber holds one Parakeet model in memory. A context is not
// safe for two calls at once, so calls take turns.
type ParakeetTranscriber struct {
	mutex   sync.Mutex
	context *C.struct_parakeet_context
	threads int
}

// LoadParakeetTranscriber loads a ggml Parakeet TDT model.
func LoadParakeetTranscriber(modelPath string, threads int) (*ParakeetTranscriber, error) {
	path := C.CString(modelPath)
	defer C.free(unsafe.Pointer(path))
	context := C.loadParakeet(path)
	if context == nil {
		return nil, fmt.Errorf("parakeet could not load the model %s", modelPath)
	}
	return &ParakeetTranscriber{context: context, threads: threads}, nil
}

// Transcribe returns the text and its words with times.
func (t *ParakeetTranscriber) Transcribe(samples []float32) (string, []Word, error) {
	if len(samples) == 0 {
		return "", []Word{}, nil
	}
	t.mutex.Lock()
	defer t.mutex.Unlock()
	if status := C.runParakeet(t.context, (*C.float)(unsafe.Pointer(&samples[0])), C.int(len(samples)), C.int(t.threads)); status != 0 {
		return "", nil, fmt.Errorf("parakeet_full returned %d", int(status))
	}
	var texts []string
	words := []Word{}
	segmentCount := int(C.parakeet_full_n_segments(t.context))
	for segment := 0; segment < segmentCount; segment++ {
		texts = append(texts, strings.TrimSpace(C.GoString(C.parakeet_full_get_segment_text(t.context, C.int(segment)))))
		tokenCount := int(C.parakeet_full_n_tokens(t.context, C.int(segment)))
		for token := 0; token < tokenCount; token++ {
			data := C.parakeet_full_get_token_data(t.context, C.int(segment), C.int(token))
			var buffer [256]C.char
			C.parakeet_token_to_text(C.parakeet_token_to_str(t.context, data.id), C.bool(len(words) == 0), &buffer[0], C.int(len(buffer)))
			piece := C.GoString(&buffer[0])
			startMs, endMs := int64(data.t0)*parakeetTimeUnitMs, int64(data.t1)*parakeetTimeUnitMs
			if bool(data.is_word_start) || len(words) == 0 {
				words = append(words, Word{Text: strings.TrimSpace(piece), StartMs: startMs, EndMs: endMs})
				continue
			}
			last := &words[len(words)-1]
			last.Text += piece
			if endMs > last.EndMs {
				last.EndMs = endMs
			}
		}
	}
	return strings.TrimSpace(strings.Join(texts, " ")), words, nil
}

// VoiceActivityStream is one Silero VAD model with the state of one audio
// stream: the LSTM state carries from one call to the next, and samples
// short of a whole window wait for the next call.
type VoiceActivityStream struct {
	mutex          sync.Mutex
	context        *C.struct_whisper_vad_context
	windowSamples  int
	pending        []float32
	windowsScored  int
	lastUsedAtUnix int64
}

// sileroWindowSamples is the window Silero scores at 16 kHz.
const sileroWindowSamples = 512

// OpenVoiceActivityStream loads the VAD model for a new stream.
func OpenVoiceActivityStream(modelPath string) (*VoiceActivityStream, error) {
	path := C.CString(modelPath)
	defer C.free(unsafe.Pointer(path))
	context := C.loadVoiceActivityDetector(path)
	if context == nil {
		return nil, fmt.Errorf("whisper.cpp could not load the VAD model %s", modelPath)
	}
	// A new context's LSTM state is whatever the allocation held: two fresh
	// streams scored the same audio up to 0.98 apart until it was cleared.
	C.whisper_vad_reset_state(context)
	return &VoiceActivityStream{context: context, windowSamples: sileroWindowSamples}, nil
}

// Score adds samples to the stream and returns the speech probability of
// each whole window they complete, and the index of the first of them.
func (s *VoiceActivityStream) Score(samples []float32) (firstWindow int, probabilities []float32, err error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if s.context == nil {
		return 0, nil, errors.New("the stream is closed")
	}
	s.pending = append(s.pending, samples...)
	whole := len(s.pending) / s.windowSamples * s.windowSamples
	firstWindow = s.windowsScored
	probabilities = []float32{}
	if whole == 0 {
		return firstWindow, probabilities, nil
	}
	if !bool(C.whisper_vad_detect_speech_no_reset(s.context, (*C.float)(unsafe.Pointer(&s.pending[0])), C.int(whole))) {
		return 0, nil, errors.New("whisper_vad_detect_speech_no_reset failed")
	}
	count := int(C.whisper_vad_n_probs(s.context))
	scored := unsafe.Slice((*float32)(unsafe.Pointer(C.whisper_vad_probs(s.context))), count)
	probabilities = append(probabilities, scored...)
	s.windowsScored += count
	s.pending = append(s.pending[:0], s.pending[whole:]...)
	return firstWindow, probabilities, nil
}

// Close frees the model.
func (s *VoiceActivityStream) Close() {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if s.context != nil {
		C.whisper_vad_free(s.context)
		s.context = nil
	}
}
