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
| `POST /api/voice/utterances/{id}/finish` | `{"ended_at_ms"}` → 200; decisions become final once every received segment is handled |
| `POST /api/voice/utterances/{id}/cancel` | → 200; stops every session the utterance started |
| `POST /api/voice/utterances/{id}/audio?filename=…` | body the bytes, `audio/wav`, `audio/ogg` or `audio/flac` → 200 `{"shared_into":[…]}`; kept in the data directory and shared into every session the utterance still keeps, and into any it starts later |
| `GET /api/voice/utterances/{id}` | the row: segments, sessions with roles and timings, decisions, continuation judgments |
| `GET /api/voice/utterances?limit=50` | the caller's utterances, newest first |
| `GET /api/voice/utterances/{id}/events` | Server-Sent Events, below |
| `GET /api/voice/settings`, `PATCH /api/voice/settings` | the settings below; an unknown field or a value of the wrong type is 400 and nothing is written |
| `GET /api/voice/latency?group_by=strategy\|pooled\|trigger` | median intervals per group, below |
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

## How segments are dispatched

- **Segment 0** goes at once to a waiting pooled session when one is ready and its text runs at the default effort; otherwise to a new session. Its role is `speculative`.
- **Segment k within the continuation window** — received no later than `continuation_window_ms` after segment k−1's speech end — starts three things at the same moment: a session for the latest request's segments joined with segment k, a session for segment k alone, and a question to the continuation judge. When the judge says *continues*, the joined session is kept and the earlier one and the lone one are superseded. When it says *separate*, the earlier session is kept, the lone one is `separate`, and the joined one is superseded. A superseded session is stopped at once and moved to the chat's Archive.
- **"The latest request"** is the last request the utterance holds, so after a separate request a further continuation joins onto that one, not onto segment 0. For an utterance whose segments all continue each other, this is segments 0..k.
- **Segment k past the window** is a separate request without asking the judge, and the earlier session is kept.
- **Final decisions**: after `finish`, or when the window passes with no new segment, every session still speculative is kept and nothing more is superseded.
- **Segments are handled one at a time per utterance, in index order.** A segment that arrives while the previous one is still being judged waits for that decision, so a request is never joined onto a session that is about to be superseded. A segment that arrives out of order waits for the missing index.
- **When the judge fails**, the joined session is kept, since its text holds every word, and the failure is in the decision's reason and the judgment row. When only one of the two new sessions could be started, that one decides.
- **Effort** is `default_effort` unless the text contains one of `effort_raising_phrases` (any case), and then `raised_effort`. A raised-effort text never goes to a pooled session, which runs at the default.

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

## Timings and latency

All times are milliseconds since the epoch, server clock. Per utterance:
speech start, received, finished, finalized, cancelled. Per segment: speech
end, received. Per session: dispatch started, dispatched (the send returned),
first `block` event, `result` event, stopped. `GET /api/voice/latency` groups
every dispatched session and reports, per group, the median of
`speech_end_to_received`, `received_to_dispatched`, `speech_end_to_dispatched`,
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

These nine are stored in the database: the environment seeds each once, and
after that `PATCH /api/voice/settings` changes them. A new
`continuation_judge_model_role` is checked against model-store's roles, a new
`default_model` against its models, and a new `instance_id` against
llm-bridge-server's instances; when the owner cannot be asked the answer is
502. The phone reads `endpoint_silence_ms` and `eager_silence_ms`.

Read only from the environment: `VOICE_INTAKE_ADDR` (127.0.0.1:8332),
`VOICE_INTAKE_DATA_DIR`, `VOICE_INTAKE_LLM_BRIDGE_URL`,
`VOICE_INTAKE_MODEL_STORE_URL`, `VOICE_INTAKE_SESSION_WORKING_DIR` (empty: the
instance's directory), `VOICE_INTAKE_RAISED_EFFORT` (high), and
`LLMBRIDGE_SERVICE_TOKEN` (required). A set `VOICE_INTAKE_` variable that is
not declared stops the start.

## Host setup

The unit file sets only the listen address. Put the rest in a host-local
drop-in, `~/.config/systemd/user/voice-intake.service.d/host.conf`, mode 600:
an `EnvironmentFile=` holding `LLMBRIDGE_SERVICE_TOKEN`, and the instance and
working directory. The prompt every voice session reads is the project prompt
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
- `GET /api/voice/utterances/{id}` and the list carry more fields than the contract names: the phone's own timestamps beside the server-time ones, the clock skew, audio details, errors and the continuation judgments.
