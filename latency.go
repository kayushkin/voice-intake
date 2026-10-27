package voiceintake

import (
	"sort"
	"strconv"
)

// latencyGroupings are the group_by values GET /api/voice/latency takes.
var latencyGroupings = map[string]bool{"strategy": true, "pooled": true, "trigger": true, "source": true}

// LatencyGroup is one group's sample count and medians, in milliseconds.
// A median is null when no sample in the group has both ends of it.
type LatencyGroup struct {
	Group    string            `json:"group"`
	Sessions int               `json:"sessions"`
	MedianMs map[string]*int64 `json:"median_ms"`
	Samples  map[string]int    `json:"samples"`
}

// latencyMeasures names each interval and how to read it from a sample.
// Every interval is server time: phone timestamps were corrected by the clock
// skew measured on the request that carried them.
var latencyMeasures = []struct {
	name string
	read func(LatencySample) *int64
}{
	{"speech_end_to_received", func(s LatencySample) *int64 { return difference(&s.SpeechEndedAtMs, &s.SegmentReceivedAtMs) }},
	{"transcription", func(s LatencySample) *int64 { return difference(s.TranscriptionStartedAtMs, s.TranscribedAtMs) }},
	{"speech_end_to_transcribed", func(s LatencySample) *int64 { return difference(&s.SpeechEndedAtMs, s.TranscribedAtMs) }},
	{"received_to_dispatched", func(s LatencySample) *int64 { return difference(&s.SegmentReceivedAtMs, s.DispatchedAtMs) }},
	{"speech_end_to_dispatched", func(s LatencySample) *int64 { return difference(&s.SpeechEndedAtMs, s.DispatchedAtMs) }},
	{"dispatched_to_first_block", func(s LatencySample) *int64 { return difference(s.DispatchedAtMs, s.FirstBlockAtMs) }},
	{"dispatched_to_result", func(s LatencySample) *int64 { return difference(s.DispatchedAtMs, s.ResultAtMs) }},
	{"speech_end_to_first_block", func(s LatencySample) *int64 { return difference(&s.SpeechEndedAtMs, s.FirstBlockAtMs) }},
	{"speech_end_to_result", func(s LatencySample) *int64 { return difference(&s.SpeechEndedAtMs, s.ResultAtMs) }},
}

func difference(from, to *int64) *int64 {
	if from == nil || to == nil {
		return nil
	}
	value := *to - *from
	return &value
}

// LatencyMedians groups the samples and takes each interval's median.
func LatencyMedians(samples []LatencySample, groupBy string) []LatencyGroup {
	byGroup := map[string][]LatencySample{}
	for _, sample := range samples {
		var key string
		switch groupBy {
		case "strategy":
			key = sample.Strategy
		case "trigger":
			key = sample.Trigger
		case "pooled":
			key = strconv.FormatBool(sample.Pooled)
		case "source":
			key = sample.Source
		}
		byGroup[key] = append(byGroup[key], sample)
	}
	groups := make([]LatencyGroup, 0, len(byGroup))
	for key, members := range byGroup {
		group := LatencyGroup{Group: key, Sessions: len(members), MedianMs: map[string]*int64{}, Samples: map[string]int{}}
		for _, measure := range latencyMeasures {
			var values []int64
			for _, member := range members {
				if value := measure.read(member); value != nil {
					values = append(values, *value)
				}
			}
			group.Samples[measure.name] = len(values)
			group.MedianMs[measure.name] = median(values)
		}
		groups = append(groups, group)
	}
	sort.Slice(groups, func(i, j int) bool { return groups[i].Group < groups[j].Group })
	return groups
}

func median(values []int64) *int64 {
	if len(values) == 0 {
		return nil
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	middle := len(values) / 2
	result := values[middle]
	if len(values)%2 == 0 {
		result = (values[middle-1] + values[middle]) / 2
	}
	return &result
}
