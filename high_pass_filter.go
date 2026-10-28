package voiceintake

import (
	"encoding/binary"
	"math"
)

// highPassFilter is a second-order Butterworth high-pass filter over 16 kHz
// mono 16-bit PCM. It keeps its state between calls, so a stream filtered
// chunk by chunk comes out the same as the whole recording filtered at once.
type highPassFilter struct {
	b0, b1, b2, a1, a2 float64
	x1, x2, y1, y2     float64
	// pendingByte holds the first byte of a sample split across two calls.
	pendingByte []byte
}

// newHighPassFilter builds a filter with the cutoff in hertz, or nil for a
// cutoff of zero (no filter). The coefficients are the bilinear transform of
// the analogue Butterworth section, Q = 1/√2.
func newHighPassFilter(cutoffHertz int) *highPassFilter {
	if cutoffHertz <= 0 {
		return nil
	}
	omega := 2 * math.Pi * float64(cutoffHertz) / segmentAudioSampleRate
	alpha := math.Sin(omega) / (2 * math.Sqrt2 / 2)
	cosine := math.Cos(omega)
	a0 := 1 + alpha
	return &highPassFilter{
		b0: (1 + cosine) / 2 / a0,
		b1: -(1 + cosine) / a0,
		b2: (1 + cosine) / 2 / a0,
		a1: -2 * cosine / a0,
		a2: (1 - alpha) / a0,
	}
}

// filter returns the filtered samples of pcm, little-endian 16-bit, the same
// length as the whole samples it was given. A trailing odd byte is kept for
// the next call.
func (f *highPassFilter) filter(pcm []byte) []byte {
	if len(f.pendingByte) > 0 {
		pcm = append(append([]byte(nil), f.pendingByte...), pcm...)
		f.pendingByte = nil
	}
	whole := len(pcm) / 2 * 2
	if whole < len(pcm) {
		f.pendingByte = []byte{pcm[whole]}
	}
	out := make([]byte, whole)
	for offset := 0; offset < whole; offset += 2 {
		x := float64(int16(binary.LittleEndian.Uint16(pcm[offset:])))
		y := f.b0*x + f.b1*f.x1 + f.b2*f.x2 - f.a1*f.y1 - f.a2*f.y2
		f.x2, f.x1 = f.x1, x
		f.y2, f.y1 = f.y1, y
		binary.LittleEndian.PutUint16(out[offset:], uint16(clampToInt16(y)))
	}
	return out
}

func clampToInt16(value float64) int16 {
	switch {
	case value > math.MaxInt16:
		return math.MaxInt16
	case value < math.MinInt16:
		return math.MinInt16
	default:
		return int16(math.Round(value))
	}
}

// highPassFilterWAV filters the samples of a 16 kHz mono 16-bit WAV and
// returns a new WAV holding them. A cutoff of zero returns the WAV as given.
func highPassFilterWAV(wav []byte, cutoffHertz int) ([]byte, error) {
	filter := newHighPassFilter(cutoffHertz)
	if filter == nil {
		return wav, nil
	}
	pcm, err := wavPCM(wav)
	if err != nil {
		return nil, err
	}
	return pcmToWAV(filter.filter(pcm)), nil
}
