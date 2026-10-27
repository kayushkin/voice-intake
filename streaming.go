package voiceintake

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"
)

// StreamAudioFormat is the one audio_format a streaming utterance may name.
const StreamAudioFormat = "pcm_s16le_16000_mono"

// streamBytesPerMillisecond is 16 kHz × 2 bytes.
const streamBytesPerMillisecond = segmentAudioSampleRate * segmentAudioBitsPerSample / 8 / 1000

// segmentPadding is the audio kept on each side of the detected speech when a
// segment is cut for transcription: the detector's edges are a window or two
// inside the first and last sounds.
const segmentPadding = 200 * time.Millisecond

// Stop reasons. The phone stops for its own reasons (user, reply_playing,
// max_duration) or because this service told it to with stop_recording,
// whose reasons it sends back.
const (
	StopReasonUser                     = "user"
	StopReasonReplyPlaying             = "reply_playing"
	StopReasonMaximumDuration          = "max_duration"
	StopReasonContinuationWindowClosed = "continuation_window_closed"
	StopReasonNoSpeech                 = "no_speech"
)

// stopReasons are every reason POST …/stop takes.
var stopReasons = map[string]bool{StopReasonUser: true, StopReasonReplyPlaying: true, StopReasonMaximumDuration: true,
	StopReasonContinuationWindowClosed: true, StopReasonNoSpeech: true}

// Errors of AppendStreamAudio.
var (
	ErrNotStreaming  = errors.New("the utterance does not stream its audio")
	ErrStreamStopped = errors.New("the utterance's microphone has stopped")
)

// StreamOffsetAheadError is an audio chunk whose offset is past the bytes
// received: the phone resends from ExpectedOffsetBytes.
type StreamOffsetAheadError struct{ ExpectedOffsetBytes int64 }

func (e StreamOffsetAheadError) Error() string {
	return fmt.Sprintf("offset is past the %d bytes received", e.ExpectedOffsetBytes)
}

// StreamOptions is how one streamed recording is listened to.
type StreamOptions struct {
	// Silence ends a segment; zero ends segments only at stop (long mode,
	// manual strategy).
	Silence            time.Duration
	NoSpeechTimeout    time.Duration
	MaximumRecording   time.Duration
	ContinuationWindow time.Duration
	TranscriberName    string
	// Replay streams do not share the assembled recording into sessions.
	Replay bool
}

// StreamOptionsFor picks the silence for the utterance's mode and strategy.
func StreamOptionsFor(utterance Utterance, settings StreamSettings) StreamOptions {
	options := StreamOptions{NoSpeechTimeout: settings.NoSpeechTimeout, MaximumRecording: settings.MaximumRecording,
		ContinuationWindow: settings.ContinuationWindow, TranscriberName: settings.Transcriber}
	switch {
	case utterance.Mode == "long" || utterance.Strategy == "manual":
	case utterance.Strategy == "eager":
		options.Silence = settings.EagerSilence
	default:
		options.Silence = settings.EndpointSilence
	}
	return options
}

// audioStream is the live state of one streamed recording.
type audioStream struct {
	run         *utteranceRun
	utterance   Utterance
	options     StreamOptions
	transcriber SpeechTranscriber
	detector    SpeechDetector

	mutex               sync.Mutex
	pcm                 []byte
	endpointer          *endpointer
	windowSamples       int
	nextSegmentIndex    int
	heardSegment        bool
	lastSpeechEndSample int
	stopRecordingSent   bool
	stopRecording       chan struct{}
	stopped             bool
	pending             sync.WaitGroup
}

// streamingAvailable reports why a streaming utterance cannot be taken, if
// it cannot.
func (d *Dispatcher) streamingAvailable(transcriberName string) error {
	if d.detector == nil {
		return errors.New("no speech detector is set: " + SettingSpeechDetectionURL + " (VOICE_INTAKE_SPEECH_DETECTION_URL) is empty")
	}
	_, err := d.transcribers.Named(transcriberName)
	return err
}

// StartStream makes a streaming utterance's stream. The detector and the
// named transcriber must exist.
func (d *Dispatcher) StartStream(utterance Utterance, options StreamOptions) error {
	if d.detector == nil {
		return errors.New("no speech detector is set: " + SettingSpeechDetectionURL + " (VOICE_INTAKE_SPEECH_DETECTION_URL) is empty")
	}
	transcriber, err := d.transcribers.Named(options.TranscriberName)
	if err != nil {
		return err
	}
	run, err := d.runFor(utterance)
	if err != nil {
		return err
	}
	run.mutex.Lock()
	defer run.mutex.Unlock()
	if run.stream == nil {
		run.stream = &audioStream{run: run, utterance: utterance, options: options, transcriber: transcriber, detector: d.detector,
			stopRecording: make(chan struct{})}
	}
	return nil
}

func (d *Dispatcher) streamOf(utterance Utterance) (*audioStream, error) {
	run, err := d.runFor(utterance)
	if err != nil {
		return nil, err
	}
	run.mutex.Lock()
	stream := run.stream
	run.mutex.Unlock()
	if stream == nil {
		if utterance.AudioStreaming {
			return nil, errors.New("this process has no stream for the utterance; it was started before a restart")
		}
		return nil, ErrNotStreaming
	}
	return stream, nil
}

// AppendStreamAudio adds the bytes at offset to the stream, ignores any part
// already received, scores the new audio and acts on what it hears. It
// answers the total bytes received.
func (d *Dispatcher) AppendStreamAudio(ctx context.Context, utterance Utterance, offset int64, chunk []byte) (int64, error) {
	stream, err := d.streamOf(utterance)
	if err != nil {
		return 0, err
	}
	return stream.append(ctx, offset, chunk)
}

func (stream *audioStream) append(ctx context.Context, offset int64, chunk []byte) (int64, error) {
	stream.mutex.Lock()
	defer stream.mutex.Unlock()
	total := int64(len(stream.pcm))
	if stream.stopped {
		return total, ErrStreamStopped
	}
	if offset > total {
		return total, StreamOffsetAheadError{ExpectedOffsetBytes: total}
	}
	overlap := total - offset
	if overlap >= int64(len(chunk)) {
		return total, nil
	}
	fresh := chunk[overlap:]
	stream.pcm = append(stream.pcm, fresh...)
	total = int64(len(stream.pcm))
	d := stream.run.dispatcher
	if err := d.store.SetStreamReceivedBytes(stream.utterance.UtteranceID, total); err != nil {
		log.Printf("[stream] ERROR %s: %v", stream.utterance.UtteranceID, err)
	}
	scores, err := stream.detector.Score(ctx, stream.utterance.UtteranceID, fresh)
	if err != nil {
		stream.run.recordError(fmt.Sprintf("speech detector: %v", err))
		return total, err
	}
	if stream.endpointer == nil {
		stream.windowSamples = scores.WindowSamples
		stream.endpointer = newEndpointer(scores.WindowSamples, stream.options.Silence)
	}
	for index, probability := range scores.Probabilities {
		event := stream.endpointer.feed(scores.FirstWindow+index, probability)
		stream.act(event)
	}
	stream.checkStopConditions()
	return total, nil
}

// act passes a detector change on: hearing to the phone, a segment's end to
// transcription.
func (stream *audioStream) act(event endpointerEvent) {
	if event.HearingChanged {
		stream.run.hub.publish(eventHearing, map[string]any{"speech": event.Hearing})
	}
	if event.SegmentEnded {
		stream.endSegment(event.StartSample, event.EndSample)
	}
}

// checkStopConditions tells the phone to stop, once, when the recording has
// gone on too long, held no speech, or passed the continuation window after
// the last segment. Every measure is audio time, so a replay decides as the
// live stream did.
func (stream *audioStream) checkStopConditions() {
	if stream.stopRecordingSent {
		return
	}
	audioMs := int64(len(stream.pcm) / streamBytesPerMillisecond)
	speaking := stream.endpointer != nil && stream.endpointer.speechInProgress()
	lastSpeechEndMs := int64(stream.lastSpeechEndSample) * 1000 / segmentAudioSampleRate
	switch {
	case audioMs >= stream.options.MaximumRecording.Milliseconds():
		stream.sendStopRecording(StopReasonMaximumDuration)
	case !stream.heardSegment && !speaking && audioMs >= stream.options.NoSpeechTimeout.Milliseconds():
		stream.sendStopRecording(StopReasonNoSpeech)
	case stream.options.Silence > 0 && stream.heardSegment && !speaking && audioMs-lastSpeechEndMs >= stream.options.ContinuationWindow.Milliseconds():
		stream.sendStopRecording(StopReasonContinuationWindowClosed)
	}
}

func (stream *audioStream) sendStopRecording(reason string) {
	stream.stopRecordingSent = true
	close(stream.stopRecording)
	d := stream.run.dispatcher
	if err := d.store.MarkStopRecordingSent(stream.utterance.UtteranceID, reason, d.now().UnixMilli()); err != nil {
		log.Printf("[stream] ERROR %s: %v", stream.utterance.UtteranceID, err)
	}
	stream.run.hub.publish(eventStopRecording, map[string]any{"reason": reason})
}

// endSegment cuts the speech [startSample, endSample) with padding and has
// it transcribed. The caller holds stream.mutex.
func (stream *audioStream) endSegment(startSample, endSample int) {
	d := stream.run.dispatcher
	endpointedAtMs := d.now().UnixMilli()
	index := stream.nextSegmentIndex
	stream.nextSegmentIndex++
	stream.heardSegment = true
	stream.lastSpeechEndSample = endSample
	paddingSamples := int(segmentPadding.Milliseconds()) * segmentAudioSampleRate / 1000
	fromByte := max(0, (startSample-paddingSamples)*2)
	toByte := min(len(stream.pcm), (endSample+paddingSamples)*2)
	wav := pcmToWAV(stream.pcm[fromByte:toByte])
	startOffsetMs := int64(startSample) * 1000 / segmentAudioSampleRate
	endOffsetMs := int64(endSample) * 1000 / segmentAudioSampleRate
	stream.pending.Add(1)
	go func() {
		defer stream.pending.Done()
		stream.transcribeSegment(index, wav, startOffsetMs, endOffsetMs, endpointedAtMs)
	}()
}

// transcribeSegment transcribes a cut segment, stores it and hands it to
// dispatch. A failed transcription is stored with no words, so the indexes
// after it are not held up: in a stream nobody sends the segment again.
func (stream *audioStream) transcribeSegment(index int, wav []byte, startOffsetMs, endOffsetMs, endpointedAtMs int64) {
	d := stream.run.dispatcher
	utterance := stream.utterance
	ctx, cancel := context.WithTimeout(context.Background(), segmentTranscriptionLimit)
	defer cancel()
	startedAtMs := d.now().UnixMilli()
	transcript, err := stream.transcriber.Transcribe(ctx, wav)
	transcribedAtMs := d.now().UnixMilli()
	if err != nil {
		stream.run.recordError(fmt.Sprintf("segment %d: transcription failed after %d ms, so it is kept with no words: %v", index, transcribedAtMs-startedAtMs, err))
		transcript = Transcript{}
	}
	sizeBytes := int64(len(wav))
	durationMs, err := wavDurationMs(wav)
	if err != nil {
		stream.run.recordError(fmt.Sprintf("segment %d: %v", index, err))
	}
	speechEndedAtMs := utterance.SpeechStartedAtMs + endOffsetMs
	segment := Segment{UtteranceID: utterance.UtteranceID, Index: index, Text: spokenText(transcript.Text),
		SpeechEndedAtClientMs: speechEndedAtMs - utterance.ClientClockSkewMs, SpeechEndedAtMs: speechEndedAtMs,
		ClientClockSkewMs: utterance.ClientClockSkewMs, ReceivedAtMs: endpointedAtMs, Source: SegmentSourceStream,
		AudioSizeBytes: &sizeBytes, AudioDurationMs: &durationMs, TranscriptionStartedAtMs: &startedAtMs, TranscribedAtMs: &transcribedAtMs,
		TranscriptRaw: transcript.Text, DetectedSpeechStartOffsetMs: &startOffsetMs, DetectedSpeechEndOffsetMs: &endOffsetMs,
		EndpointedAtMs: &endpointedAtMs, Transcriber: stream.options.TranscriberName}
	if _, err := d.store.InsertSegment(segment); err != nil {
		stream.run.recordError(fmt.Sprintf("segment %d: store: %v", index, err))
		return
	}
	stream.run.hub.publish(eventTranscript, map[string]any{"index": index, "text": segment.Text})
	if err := d.SegmentArrived(utterance, segment); err != nil {
		stream.run.recordError(fmt.Sprintf("segment %d: %v", index, err))
	}
}

// StopStream records that the microphone stopped: any open segment ends at
// the end of the audio received, and once every segment is transcribed the
// recording is shared into the kept sessions and the decisions become final.
func (d *Dispatcher) StopStream(utterance Utterance, stoppedAtClientMs, stoppedAtMs int64, reason string) error {
	stream, err := d.streamOf(utterance)
	if err != nil {
		return err
	}
	stream.mutex.Lock()
	if stream.stopped {
		stream.mutex.Unlock()
		return nil
	}
	stream.stopped = true
	if stream.endpointer != nil {
		stream.act(stream.endpointer.finish(len(stream.pcm) / 2))
	}
	pcm := stream.pcm
	stream.mutex.Unlock()
	if err := d.store.MarkUtteranceStopped(utterance.UtteranceID, stoppedAtClientMs, stoppedAtMs, reason); err != nil {
		return err
	}
	go func() {
		stream.pending.Wait()
		closeContext, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		if err := stream.detector.Close(closeContext, utterance.UtteranceID); err != nil {
			log.Printf("[stream] ERROR %s: %v", utterance.UtteranceID, err)
		}
		cancel()
		if !stream.options.Replay && len(pcm) > 0 {
			if _, err := d.ReceiveAudio(utterance, utterance.UtteranceID+".wav", "audio/wav", pcmToWAV(pcm)); err != nil {
				log.Printf("[stream] ERROR %s: share the recording: %v", utterance.UtteranceID, err)
			}
		}
		if err := d.Finish(utterance); err != nil {
			log.Printf("[stream] ERROR %s: finish: %v", utterance.UtteranceID, err)
		}
	}()
	return nil
}

// cancelStream stops listening without finishing anything.
func (stream *audioStream) cancel() {
	stream.mutex.Lock()
	already := stream.stopped
	stream.stopped = true
	stream.mutex.Unlock()
	if already {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := stream.detector.Close(ctx, stream.utterance.UtteranceID); err != nil {
		log.Printf("[stream] ERROR %s: %v", stream.utterance.UtteranceID, err)
	}
}

// pcmToWAV wraps 16 kHz mono 16-bit PCM in a WAV header.
func pcmToWAV(pcm []byte) []byte {
	var buffer bytes.Buffer
	buffer.Grow(44 + len(pcm))
	write := func(value any) { _ = binary.Write(&buffer, binary.LittleEndian, value) }
	buffer.WriteString("RIFF")
	write(uint32(36 + len(pcm)))
	buffer.WriteString("WAVEfmt ")
	write(uint32(16))
	write(uint16(1))
	write(uint16(segmentAudioChannels))
	write(uint32(segmentAudioSampleRate))
	write(uint32(segmentAudioSampleRate * segmentAudioChannels * segmentAudioBitsPerSample / 8))
	write(uint16(segmentAudioChannels * segmentAudioBitsPerSample / 8))
	write(uint16(segmentAudioBitsPerSample))
	buffer.WriteString("data")
	write(uint32(len(pcm)))
	buffer.Write(pcm)
	return buffer.Bytes()
}

// wavPCM returns the sample bytes of a 16 kHz mono 16-bit WAV.
func wavPCM(wav []byte) ([]byte, error) {
	if _, err := wavDurationMs(wav); err != nil {
		return nil, err
	}
	for offset := 12; offset+8 <= len(wav); {
		chunkID := string(wav[offset : offset+4])
		chunkSize := int(binary.LittleEndian.Uint32(wav[offset+4 : offset+8]))
		start := offset + 8
		if chunkID == "data" {
			end := min(len(wav), start+chunkSize)
			return wav[start : start+(end-start)/2*2], nil
		}
		offset = start + chunkSize + chunkSize%2
	}
	return nil, errors.New("the WAV has no data chunk")
}
