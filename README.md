# voice-intake

A Go service that takes speech transcripts from a phone and puts them into
agent sessions on [llm-bridge-server](https://github.com/kayushkin/llm-bridge-server),
as fast as it can, and streams the sessions' replies back.

A person presses a button on the phone and speaks. The phone cuts the speech
into segments at each pause and posts each segment as soon as it has it. The
service sends segment 0 to a session that is already running, and for each
later segment decides — with a fast model's help — whether it continues the
earlier request or is a request of its own. It stops the sessions it does not
keep. Every session is created as the person, so each request is a real
conversation in that person's chat.

Go, SQLite, `llm-bridge/servicesettings` for configuration. MIT licence.

## Routes

The phone reaches the service through a front door that checks the person's
sign-in and adds `X-Principal-Id`. The service takes requests only from
loopback, and every `/api/voice/` route needs `X-Principal-Id` (401 without
it). An utterance belonging to another principal is a 404.

Every phone request carries `X-Client-Now-Ms` (the phone's clock in epoch
milliseconds). The service takes server time minus that as the clock skew of
the request and turns each phone timestamp in the same request into server
time with it. Both values are stored.

| Route | Does |
|-------|------|
| `POST /api/voice/utterances` | `{"client_utterance_id","mode","strategy","trigger","speech_started_at_ms"}` → 201 `{"utterance_id":"utterance_000001"}`; the same `client_utterance_id` again → 200, same row |
| `POST /api/voice/utterances/{id}/segments` | `{"index","text","speech_ended_at_ms"}` → 202 and dispatch; an index already received → 200, ignored; after cancel → 409 |
| `POST /api/voice/utterances/{id}/segments/{index}/audio?speech_ended_at_ms=…` | body the segment's sound, `audio/wav`, 16 kHz mono 16-bit PCM; transcribed, then handled as a text segment with that index → 202 `{"text","transcription_ms","audio_duration_ms","status":"accepted"}`; no words → 200 `"status":"no words"`, stored, not dispatched; an index already received → 200, not transcribed; after cancel → 409; transcription failed → 502, nothing stored; no transcription service set → 503 (an addition: see *Contract changes*) |
| `POST /api/voice/utterances/{id}/finish` | `{"ended_at_ms"}` → 200; decisions become final once every received segment is handled |
| `POST /api/voice/utterances/{id}/cancel` | → 200; stops every session the utterance started |
| `POST /api/voice/utterances/{id}/audio?filename=…` | body the bytes, `audio/wav`, `audio/ogg` or `audio/flac` → 200 `{"shared_into":[…]}`; kept in the data directory and shared into every session the utterance still keeps, and into any it starts later |
| `GET /api/voice/utterances/{id}` | the row: segments, sessions with roles and timings, decisions, continuation judgments |
| `GET /api/voice/utterances?limit=50` | the caller's utterances, newest first |
| `GET /api/voice/utterances/{id}/events` | Server-Sent Events, below |
| `GET /api/voice/settings`, `PATCH /api/voice/settings` | the settings below; an unknown field or a value of the wrong type is 400 and nothing is written |
| `POST /api/voice/utterances/{id}/audio-stream?offset_bytes=N` | contract v2: body raw 16 kHz mono 16-bit PCM → 200 `{"received_bytes"}`; an offset below the total is a repeat, its overlap ignored; above it → 409 `{"expected_offset_bytes"}`; after stop → 409 |
| `POST /api/voice/utterances/{id}/stop` | contract v2: `{"ended_at_ms","reason"}` → 200; reason is `user`, `reply_playing`, `max_duration`, `continuation_window_closed` or `no_speech` |
| `GET /api/voice/latency?group_by=strategy\|pooled\|trigger\|source\|configuration` | median intervals per group, below |
| `POST /api/voice/replay-recordings?expected_text=…&ground_truth_transcriber=…` | body a 16 kHz mono WAV → 201 the recording, with the true speech end from the transcriber's word times (or `true_speech_end_ms` with `true_speech_end_source`) |
| `GET /api/voice/replay-recordings` | every recording kept for replay |
| `POST /api/voice/replay-configurations`, `GET …` | a named way to run a replay, below |
| `POST /api/voice/replays` | `{"configuration_id", "recording_id"}` or `{"configuration_id","text"}` → 202 the replay; it runs in the background |
| `GET /api/voice/replays?configuration_id=…`, `GET /api/voice/replays/{replay_id}` | replays with their utterance, transcript, word accuracy, replies and timings |
| `GET /health` | `{"status":"ok"}` |
| `GET /settings` | every setting, where its value came from; secrets only as set or not |

### Events

`GET …/events` sends the utterance's whole history first and then live events,
until every session the utterance keeps has finished its turn, or five
minutes pass. A comment line every 15 seconds keeps proxies from closing a
quiet stream; the answer carries `X-Accel-Buffering: no`.

- `event: dispatch` `{"session_id","segment_indexes","text","role":"speculative"}`
- `event: decision` `{"session_id","role":"kept|superseded|separate","reason"}`
- `event: session_event` `{"session_id","event":<the llm-bridge-server event, unchanged>}`, for sessions not superseded. The reply is the `result` event's text.
- `event: error` `{"message"}`, when a session could not be started, sent to or stopped (an addition: see *Contract changes*)
- `event: done` `{}`
- Streaming utterances also get `event: hearing` `{"speech"}` when the detector starts or stops hearing speech, `event: transcript` `{"index","text"}` once a segment is transcribed, and `event: stop_recording` `{"reason":"continuation_window_closed|max_duration|no_speech"}`, sent once.

## Streaming: the server ends the segments

An utterance made with `"audio_streaming":true,"audio_format":"pcm_s16le_16000_mono"` sends its sound as it is recorded, and this service cuts it into segments. Byte 0 of the stream is `speech_started_at_ms`. Each chunk is scored by speech-server's Silero voice activity detector (`VOICE_INTAKE_SPEECH_DETECTION_URL`), 32 ms windows with the LSTM state kept across chunks; the endpointer in `speech_detection.go` hears speech from a window scoring 0.5, keeps hearing it down to 0.35, and ends a segment when silence has lasted `endpoint_silence_ms` (`eager_silence_ms` under the eager strategy). Speech shorter than 200 ms — a click, the phone's start tick — is dropped. A segment is cut with 200 ms either side, transcribed by the `transcriber` setting's service, stored with source `stream`, and dispatched like any other. Long mode and the manual strategy end no segment on a pause: the speech is one segment, cut at `stop`. `stop_recording` is sent when `maximum_recording_ms` of audio has arrived, when `no_speech_timeout_ms` has passed with no speech, or (quick mode) when `continuation_window_ms` has passed since the last segment's speech ended. At `stop` an open segment ends at the end of the audio received; once every segment is transcribed the recording is shared into the kept sessions as `utterance_NNNNNN.wav` and the decisions become final. A failed transcription in a stream stores the segment with no words, so later segments are not held up.

## Replays

A replay runs a stored recording through the streaming path at the pace it was spoken, in 200 ms chunks, or sends a stored text, under a named **configuration**: detector and `endpoint_silence_ms`, `transcriber`, `dispatch` (`session`, `oneshot` — one `POST /instances/{id}/oneshot` call with no tools — or `none`), `instance_id`, `model`, `effort`, `working_directory` and the `prompt_variant` it stands for, `warm_session` (start the session first and give it 15 s, as the pool would) and `max_budget_usd` (required for sessions). The replay stops as the phone would: on `stop_recording`, or with reason `user` where the recording ends. Its sessions are stopped and archived once their turn is over. A recording's true speech end, from word times, is what `endpoint_delay` is measured against. `GET /api/voice/latency?group_by=configuration` reports per configuration the medians of `endpoint_delay`, `detected_speech_end_error`, `transcription`, `speech_end_to_transcribed`, `dispatched_to_first_block`, `dispatched_to_result`, `speech_end_to_first_block` and `speech_end_to_result` (speech end is the true end for a recording and the send for a text), the mean word accuracy against the expected text (spoken numbers read as digits), and the sum of the turns' reported cost.

## How segments are dispatched

A segment posted as audio is transcribed first (below) and from then on is
the same as one posted as text. A segment whose transcript has no words
holds its index, so later segments do not wait for it, and is otherwise
skipped: it starts no session, and the continuation window runs from the
last segment that had words.

- **Segment 0** goes at once to a waiting pooled session when one is ready and its text runs at the default effort; otherwise to a new session. Its role is `speculative`.
- **Segment k within the continuation window** — received no later than `continuation_window_ms` after segment k−1's speech end — starts three things at the same moment: a session for the latest request's segments joined with segment k, a session for segment k alone, and a question to the continuation judge. When the judge says *continues*, the joined session is kept and the earlier one and the lone one are superseded. When it says *separate*, the earlier session is kept, the lone one is `separate`, and the joined one is superseded. A superseded session is stopped at once and moved to the chat's Archive.
- **"The latest request"** is the last request the utterance holds, so after a separate request a further continuation joins onto that one, not onto segment 0. For an utterance whose segments all continue each other, this is segments 0..k.
- **Segment k past the window** is a separate request without asking the judge, and the earlier session is kept.
- **Final decisions**: after `finish`, or when the window passes with no new segment, every session still speculative is kept and nothing more is superseded.
- **Segments are handled one at a time per utterance, in index order.** A segment that arrives while the previous one is still being judged waits for that decision, so a request is never joined onto a session that is about to be superseded. A segment that arrives out of order waits for the missing index.
- **When the judge fails**, the joined session is kept, since its text holds every word, and the failure is in the decision's reason and the judgment row. When only one of the two new sessions could be started, that one decides.
- **Effort** is `default_effort` unless the text contains one of `effort_raising_phrases` (any case) or the utterance is in long mode, and then `raised_effort`, on `raised_instance_id` and `raised_model` when they are set. A raised text never goes to a pooled session, which runs at the default.

## The pool

`pool_size` sessions per person are started ahead of speech, with the default
effort, model, instance and working directory, and titled "Voice: ready for
speech" until text arrives and the session is renamed to it. A person's pool
starts filling at their first utterance, and at start for everyone who has
made one. A waiting session the bridge reports `aborted`, `error` or
`disconnected` is given up; one the bridge's idle reaper stopped reads
`completed` and is still used, since the bridge resumes it. Each session row
records whether it was pooled and the state the pooled session was in, so
pooled and fresh sessions can be compared. Changing a setting the pool
depends on gives up the waiting sessions that no longer match.

## Transcription

`POST …/segments/{index}/audio` and a streamed segment send the WAV to the
transcription service the `transcriber` setting names in
`VOICE_INTAKE_TRANSCRIBERS`: a whisper.cpp `whisper-server`, or speech-server
(`speech-server/`), which keeps a Parakeet TDT model in memory, answers the
same inference form and adds word times. Measured on four phone recordings
cut to their speech: Parakeet 0.6b q8_0 0.2–0.45 s, whisper base.en about
1.0 s, the same words. Whisper is called at temperature 0 and whisper's full 30-second audio context: a context sized
to the clip was about three times faster when measured, but repeated
sentences and turned silence into words. The transcript is kept as the
server gave it (`transcript_raw`); the segment's text is that with
whitespace joined into single spaces, and empty when it is only a marker
such as `[BLANK_AUDIO]`. A failed transcription is written to the
utterance's errors and sent as `event: error`, and stores no segment, so the
phone may send the same index again.

## Timings and latency

All times are milliseconds since the epoch, server clock. Per utterance:
speech start, received, finished, finalized, cancelled. Per segment: speech
end, received, and for audio segments transcription started and ended
(`transcription_ms` is the difference). Per session: dispatch started, dispatched (the send returned),
first `block` event, `result` event, stopped. `GET /api/voice/latency?group_by=strategy|pooled|trigger|source` groups
every dispatched session and reports, per group, the median of
`speech_end_to_received`, `transcription`, `speech_end_to_transcribed`, `received_to_dispatched`, `speech_end_to_dispatched`,
`dispatched_to_first_block`, `dispatched_to_result`, `speech_end_to_first_block`
and `speech_end_to_result`, each measured from the speech end of the last
segment the session's text holds, with the number of samples behind each.

## Settings

| Key | Default | Variable |
|-----|---------|----------|
| `pool_size` | 1 | `VOICE_INTAKE_POOL_SIZE` |
| `continuation_window_ms` | 8000 | `VOICE_INTAKE_CONTINUATION_WINDOW_MS` |
| `default_effort` | low | `VOICE_INTAKE_DEFAULT_EFFORT` |
| `default_model` | empty: the harness default | `VOICE_INTAKE_DEFAULT_MODEL` |
| `effort_raising_phrases` | think hard, think carefully, take your time | `VOICE_INTAKE_EFFORT_RAISING_PHRASES` |
| `continuation_judge_model_role` | efficient | `VOICE_INTAKE_CONTINUATION_JUDGE_MODEL_ROLE` |
| `instance_id` | empty | `VOICE_INTAKE_INSTANCE_ID` |
| `endpoint_silence_ms` | 900 | `VOICE_INTAKE_ENDPOINT_SILENCE_MS` |
| `eager_silence_ms` | 350 | `VOICE_INTAKE_EAGER_SILENCE_MS` |
| `transcriber` | empty | `VOICE_INTAKE_TRANSCRIBER` |
| `no_speech_timeout_ms` | 6000 | `VOICE_INTAKE_NO_SPEECH_TIMEOUT_MS` |
| `maximum_recording_ms` | 300000 | `VOICE_INTAKE_MAXIMUM_RECORDING_MS` |
| `raised_instance_id` | empty: `instance_id` | `VOICE_INTAKE_RAISED_INSTANCE_ID` |
| `raised_model` | empty: `default_model` | `VOICE_INTAKE_RAISED_MODEL` |

These are stored in the database: the environment seeds each once, and
after that `PATCH /api/voice/settings` changes them. A new
`continuation_judge_model_role` is checked against model-store's roles, a new
`default_model` against its models, and a new `instance_id` against
llm-bridge-server's instances; when the owner cannot be asked the answer is
502; a new `transcriber` must be a name in `VOICE_INTAKE_TRANSCRIBERS`, and `raised_model` and `raised_instance_id` are checked like `default_model` and `instance_id`. The phone reads `endpoint_silence_ms` and `eager_silence_ms` when it cuts segments itself; for a streaming utterance this service uses them.

Read only from the environment: `VOICE_INTAKE_ADDR` (127.0.0.1:8332),
`VOICE_INTAKE_DATA_DIR`, `VOICE_INTAKE_LLM_BRIDGE_URL`,
`VOICE_INTAKE_MODEL_STORE_URL`, `VOICE_INTAKE_TRANSCRIBERS` (name:url pairs,
each url an inference route such as `http://127.0.0.1:<port>/inference`),
`VOICE_INTAKE_SPEECH_DETECTION_URL` (speech-server's address; empty: a
streaming utterance is refused with 503), `VOICE_INTAKE_SESSION_WORKING_DIR` (empty: the
instance's directory), `VOICE_INTAKE_RAISED_EFFORT` (high), and
`LLMBRIDGE_SERVICE_TOKEN` (required). A set `VOICE_INTAKE_` variable that is
not declared stops the start.

## Host setup

The unit file sets only the listen address. Put the rest in a host-local
drop-in, `~/.config/systemd/user/voice-intake.service.d/host.conf`, mode 600:
an `EnvironmentFile=` holding `LLMBRIDGE_SERVICE_TOKEN`, the instance and
working directory, `VOICE_INTAKE_TRANSCRIBERS`, `VOICE_INTAKE_TRANSCRIBER` and
`VOICE_INTAKE_SPEECH_DETECTION_URL`. A whisper-server is run by the host, not
by this repo.

speech-server (`speech-server/`, its own Go module) links whisper.cpp's
`libparakeet` and `libwhisper` through cgo. `deploy.sh` builds it with
`CGO_CFLAGS` and `CGO_LDFLAGS` from `~/.config/speech-server/build.env`
(headers and libraries of a whisper.cpp build that has Parakeet), and installs
`speech-server.service`, which sets only `SPEECH_SERVER_ADDR`
(127.0.0.1:8335). A drop-in beside it sets `SPEECH_SERVER_PARAKEET_MODEL` (a
ggml Parakeet TDT model), `SPEECH_SERVER_VOICE_ACTIVITY_MODEL` (a ggml Silero
VAD model, ggml-org/whisper-vad), `SPEECH_SERVER_THREADS` and the library path.
Its routes: `POST /inference` (whisper-server's form; answers `text`, `words`
with `start_ms`/`end_ms`, `transcription_ms`), `POST /vad/streams/{id}` (raw
PCM in, `{"window_samples","first_window","probabilities"}` out, state kept per
stream; a new stream's state is cleared, or two streams score the same audio
differently), `DELETE /vad/streams/{id}`, `GET /health`, `GET /settings`. The prompt every voice session reads is the project prompt
collection rooted at `VOICE_INTAKE_SESSION_WORKING_DIR`, kept in agent-store
like any other prompt; Claude Code reads its rendered `CLAUDE.md` because the
session starts there.

Sessions are created with type `interactive`, purpose `voice` and origin
`voice-intake`. llm-bridge's purpose registry does not list `voice` yet, so
the bridge stores it and logs a taxonomy notice on each create.

## Build, test, deploy

```bash
go test -race ./...
./deploy.sh
```

`deploy.sh` runs the shared deploy gate, the tests, checks the running
service's environment against the declared settings, installs the binary and
unit, restarts the user unit, smoke-checks the routes and the loopback bind,
and records the deploy.

## Contract changes

The phone-facing contract is unchanged except for these additions, none of
which a client that ignores unknown fields and events has to handle:

- `event: error` `{"message"}` on the event stream, when a session could not be started, sent to or stopped, so such a failure does not reach the phone as silence.
- The 202 and 200 answers to segments, finish and cancel carry a small JSON body (`utterance_id`, `status`); the audio answer carries `shared_into`.
- `POST /api/voice/utterances/{id}/segments/{index}/audio`: the phone may send a segment's sound instead of its text, and the service transcribes it. Answers as in *Routes*. A transcript with no words uses up the index; a failed transcription does not.
- Each segment in `GET /api/voice/utterances/{id}` carries `source` (`text` or `audio`), and an audio one also `audio_size_bytes`, `audio_duration_ms`, `transcription_started_at_ms`, `transcribed_at_ms`, `transcription_ms` and `transcript_raw`. An audio segment with no words appears with empty `text`.
- `GET /api/voice/latency` takes `group_by=source` and reports two more medians, `transcription` and `speech_end_to_transcribed`.
- Contract v2 (streaming) is implemented as written, with these additions: `POST …/stop` takes the `stop_recording` reasons (`continuation_window_closed`, `no_speech`, `max_duration`) as well as `user` and `reply_playing`, since the phone sends the server's reason back; `audio-stream` answers 409 after `stop`, and 400 for an odd byte count or offset; a streamed segment has source `stream` and carries `detected_speech_start_offset_ms`, `detected_speech_end_offset_ms`, `endpointed_at_ms` and `transcriber`; an utterance carries `audio_streaming`, `audio_format`, `stream_received_bytes`, `stop_recording_reason`, `stop_recording_sent_at_ms`, `stopped_at_ms`, `stopped_at_client_ms`, `stop_reason` and `replay_id`.
- `GET /api/voice/settings` carries five more fields: `transcriber`, `no_speech_timeout_ms`, `maximum_recording_ms`, `raised_instance_id`, `raised_model`.
- A session in `GET /api/voice/utterances/{id}` carries `working_directory`, `reply_text` and `turn_cost_usd`.
- Replay routes and `GET /api/voice/latency?group_by=configuration`, above. They are for measuring settings, not for the phone.
- `GET /api/voice/utterances/{id}` and the list carry more fields than the contract names: the phone's own timestamps beside the server-time ones, the clock skew, audio details, errors and the continuation judgments.
