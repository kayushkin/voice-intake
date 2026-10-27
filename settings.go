package voiceintake

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/kayushkin/llm-bridge/msg"
	"github.com/kayushkin/llm-bridge/servicesettings"
)

// ServiceName is this service's name in its own settings description.
const ServiceName = "voice-intake"

// OwnedEnvironmentVariablePrefix is the prefix of the variables that are this
// service's alone. A set variable carrying it that SettingDefinitions does not
// declare stops the service from starting.
const OwnedEnvironmentVariablePrefix = "VOICE_INTAKE_"

// Keys of the deploy settings, read from the environment at start.
const (
	SettingListenAddress           = "listen_address"
	SettingDataDirectory           = "data_directory"
	SettingLLMBridgeURL            = "llm_bridge_url"
	SettingLLMBridgeServiceToken   = "llm_bridge_service_token"
	SettingModelStoreURL           = "model_store_url"
	SettingSessionWorkingDirectory = "session_working_directory"
	SettingRaisedEffort            = "raised_effort"
)

// Keys of the settings the phone and the operator read and change through
// GET and PATCH /api/voice/settings. Each key is the contract's field name.
const (
	SettingPoolSize                       = "pool_size"
	SettingContinuationWindowMilliseconds = "continuation_window_ms"
	SettingDefaultEffort                  = "default_effort"
	SettingDefaultModel                   = "default_model"
	SettingEffortRaisingPhrases           = "effort_raising_phrases"
	SettingContinuationJudgeModelRole     = "continuation_judge_model_role"
	SettingInstanceID                     = "instance_id"
	SettingEndpointSilenceMilliseconds    = "endpoint_silence_ms"
	SettingEagerSilenceMilliseconds       = "eager_silence_ms"
)

// DefaultListenAddress is where the service listens with nothing set.
// Loopback, deliberately: the service trusts X-Principal-Id from loopback.
const DefaultListenAddress = "127.0.0.1:8332"

// DefaultDataDir is where the database lives when nothing says otherwise.
func DefaultDataDir() string {
	home, err := os.UserHomeDir()
	if err != nil {
		home = "."
	}
	return filepath.Join(home, ".config", "voice-intake")
}

// SettingDefinitions declares every environment variable the service reads,
// once.
func SettingDefinitions() []servicesettings.Definition {
	wiring, path, secret, behaviour := msg.ServiceSettingKindWiring, msg.ServiceSettingKindPath, msg.ServiceSettingKindSecret, msg.ServiceSettingKindBehaviour
	text, integer, list, role := msg.ServiceSettingValueTypeString, msg.ServiceSettingValueTypeInteger, msg.ServiceSettingValueTypeStringList, msg.ServiceSettingValueTypeModelRole
	return []servicesettings.Definition{
		{Key: SettingListenAddress, EnvironmentVariable: "VOICE_INTAKE_ADDR", Kind: wiring, ValueType: text, Default: DefaultListenAddress,
			Description: "The address the HTTP server listens on. Keep it on loopback: X-Principal-Id is trusted only from loopback."},
		{Key: SettingDataDirectory, EnvironmentVariable: "VOICE_INTAKE_DATA_DIR", Kind: path, ValueType: text, Default: DefaultDataDir(),
			Description: "The directory that holds voice-intake.db and the audio the phone uploads."},
		{Key: SettingLLMBridgeURL, EnvironmentVariable: "VOICE_INTAKE_LLM_BRIDGE_URL", Kind: wiring, ValueType: text, Default: "http://127.0.0.1:8160",
			Description: "Where llm-bridge-server answers: sessions are created, sent to, stopped and streamed there, and the continuation judge is asked through its POST /oneshot."},
		{Key: SettingLLMBridgeServiceToken, EnvironmentVariable: "LLMBRIDGE_SERVICE_TOKEN", Kind: secret, ValueType: text, Required: true,
			Description: "llm-bridge-server's service token, sent as X-LLM-Bridge-Service-Token, with X-Principal-Id naming the person when a session is created for them."},
		{Key: SettingModelStoreURL, EnvironmentVariable: "VOICE_INTAKE_MODEL_STORE_URL", Kind: wiring, ValueType: text, Default: "http://127.0.0.1:8155",
			Description: "Where model-store answers: a new continuation_judge_model_role is checked against its roles and a new default_model against its models."},
		{Key: SettingSessionWorkingDirectory, EnvironmentVariable: "VOICE_INTAKE_SESSION_WORKING_DIR", Kind: path, ValueType: text,
			Description: "The working directory of every voice session. The prompt a voice session reads is the project prompt collection rooted here. Empty leaves the instance's own directory."},
		{Key: SettingRaisedEffort, EnvironmentVariable: "VOICE_INTAKE_RAISED_EFFORT", Kind: behaviour, ValueType: text, Default: "high",
			Description: "The effort a session runs at when its text contains one of effort_raising_phrases."},

		{Key: SettingPoolSize, EnvironmentVariable: "VOICE_INTAKE_POOL_SIZE", Kind: behaviour, ValueType: integer, Default: "1", Editable: true,
			Description: "How many started sessions to keep ready per person, so the first segment of an utterance goes to a running session."},
		{Key: SettingContinuationWindowMilliseconds, EnvironmentVariable: "VOICE_INTAKE_CONTINUATION_WINDOW_MS", Kind: behaviour, ValueType: integer, Default: "8000", Editable: true,
			Description: "A segment received within this many milliseconds of the previous segment's speech end may continue it, and the judge is asked. Later than that, it is a separate request, and the earlier decisions are final."},
		{Key: SettingDefaultEffort, EnvironmentVariable: "VOICE_INTAKE_DEFAULT_EFFORT", Kind: behaviour, ValueType: text, Default: "low", Editable: true,
			Description: "The effort a voice session runs at unless its text raises it."},
		{Key: SettingDefaultModel, EnvironmentVariable: "VOICE_INTAKE_DEFAULT_MODEL", Kind: behaviour, ValueType: text, Editable: true,
			Description: "The model a voice session runs on, by model-store id. Empty leaves the harness default."},
		{Key: SettingEffortRaisingPhrases, EnvironmentVariable: "VOICE_INTAKE_EFFORT_RAISING_PHRASES", Kind: behaviour, ValueType: list, Default: "think hard,think carefully,take your time", Editable: true,
			Description: "Phrases that, found in a session's text without regard to case, start it at raised_effort instead of default_effort."},
		{Key: SettingContinuationJudgeModelRole, EnvironmentVariable: "VOICE_INTAKE_CONTINUATION_JUDGE_MODEL_ROLE", Kind: behaviour, ValueType: role, Default: "efficient", Editable: true,
			Description: "The model-store role asked whether a new segment continues the earlier request."},
		{Key: SettingInstanceID, EnvironmentVariable: "VOICE_INTAKE_INSTANCE_ID", Kind: behaviour, ValueType: text, Editable: true,
			Description: "The llm-bridge-server instance voice sessions start on; its harness type is read from the bridge."},
		{Key: SettingEndpointSilenceMilliseconds, EnvironmentVariable: "VOICE_INTAKE_ENDPOINT_SILENCE_MS", Kind: behaviour, ValueType: integer, Default: "900", Editable: true,
			Description: "Read by the phone: the silence that ends a segment under the endpoint strategy."},
		{Key: SettingEagerSilenceMilliseconds, EnvironmentVariable: "VOICE_INTAKE_EAGER_SILENCE_MS", Kind: behaviour, ValueType: integer, Default: "350", Editable: true,
			Description: "Read by the phone: the silence that ends a segment under the eager strategy."},
	}
}

// NewSettingsRegistry reads this service's settings from environment.
func NewSettingsRegistry(environment servicesettings.Environment) (*servicesettings.Registry, error) {
	return servicesettings.New(ServiceName, []string{OwnedEnvironmentVariablePrefix}, SettingDefinitions(), environment)
}

// voiceSettingKeys are the fields of GET and PATCH /api/voice/settings, in the
// contract's order.
var voiceSettingKeys = []string{
	SettingPoolSize, SettingContinuationWindowMilliseconds, SettingDefaultEffort, SettingDefaultModel,
	SettingEffortRaisingPhrases, SettingContinuationJudgeModelRole, SettingInstanceID,
	SettingEndpointSilenceMilliseconds, SettingEagerSilenceMilliseconds,
}

func voiceSettingValueType(key string) msg.ServiceSettingValueType {
	for _, definition := range SettingDefinitions() {
		if definition.Key == key {
			return definition.ValueType
		}
	}
	panic("voice-intake: no definition for setting " + key)
}

// VoiceSettings is GET /api/voice/settings: each field typed as JSON, not as
// the registry's text.
func VoiceSettings(registry *servicesettings.Registry) map[string]any {
	answer := make(map[string]any, len(voiceSettingKeys))
	for _, key := range voiceSettingKeys {
		switch voiceSettingValueType(key) {
		case msg.ServiceSettingValueTypeInteger:
			answer[key] = registry.Integer(key)
		case msg.ServiceSettingValueTypeStringList:
			phrases := registry.StringList(key)
			if phrases == nil {
				phrases = []string{}
			}
			answer[key] = phrases
		case msg.ServiceSettingValueTypeModelRole:
			answer[key] = registry.ModelRole(key)
		default:
			answer[key] = registry.String(key)
		}
	}
	return answer
}

// voiceSettingsPatch turns a PATCH body into registry text per key. Any field
// the contract does not name, and any value of the wrong JSON type, is an
// error, and then nothing is written.
func voiceSettingsPatch(body []byte) (map[string]string, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(body, &fields); err != nil {
		return nil, fmt.Errorf("body must be a JSON object: %v", err)
	}
	known := make(map[string]bool, len(voiceSettingKeys))
	for _, key := range voiceSettingKeys {
		known[key] = true
	}
	changes := make(map[string]string, len(fields))
	for key, raw := range fields {
		if !known[key] {
			return nil, fmt.Errorf("unknown field %q; the fields are %s", key, strings.Join(voiceSettingKeys, ", "))
		}
		switch voiceSettingValueType(key) {
		case msg.ServiceSettingValueTypeInteger:
			var number int
			if err := json.Unmarshal(raw, &number); err != nil || number < 0 {
				return nil, fmt.Errorf("%s must be a whole number of zero or more, got %s", key, raw)
			}
			changes[key] = strconv.Itoa(number)
		case msg.ServiceSettingValueTypeStringList:
			var phrases []string
			if err := json.Unmarshal(raw, &phrases); err != nil {
				return nil, fmt.Errorf("%s must be a list of strings, got %s", key, raw)
			}
			for _, phrase := range phrases {
				if strings.Contains(phrase, ",") {
					return nil, fmt.Errorf("%s: %q holds a comma, which the stored list uses to separate phrases", key, phrase)
				}
			}
			changes[key] = strings.Join(phrases, ",")
		default:
			var value string
			if err := json.Unmarshal(raw, &value); err != nil {
				return nil, fmt.Errorf("%s must be a string, got %s", key, raw)
			}
			changes[key] = value
		}
	}
	return changes, nil
}

// DispatchSettings is what one dispatch decision reads, taken from the
// registry at the moment of use.
type DispatchSettings struct {
	PoolSize             int
	ContinuationWindow   time.Duration
	DefaultEffort        string
	RaisedEffort         string
	DefaultModel         string
	EffortRaisingPhrases []string
	JudgeModelRole       string
	InstanceID           string
	WorkingDirectory     string
}

// DispatchSettingsFrom reads DispatchSettings from the registry.
func DispatchSettingsFrom(registry *servicesettings.Registry) DispatchSettings {
	return DispatchSettings{
		PoolSize:             registry.Integer(SettingPoolSize),
		ContinuationWindow:   time.Duration(registry.Integer(SettingContinuationWindowMilliseconds)) * time.Millisecond,
		DefaultEffort:        registry.String(SettingDefaultEffort),
		RaisedEffort:         registry.String(SettingRaisedEffort),
		DefaultModel:         registry.String(SettingDefaultModel),
		EffortRaisingPhrases: registry.StringList(SettingEffortRaisingPhrases),
		JudgeModelRole:       registry.ModelRole(SettingContinuationJudgeModelRole),
		InstanceID:           registry.String(SettingInstanceID),
		WorkingDirectory:     registry.String(SettingSessionWorkingDirectory),
	}
}

// RegisterSettingsHandler serves the registry at GET /settings, the shape
// every service on the host answers. Changes go through PATCH
// /api/voice/settings, which names the person making them.
func RegisterSettingsHandler(mux *http.ServeMux, registry *servicesettings.Registry) {
	mux.Handle("GET /settings", servicesettings.Handler(registry, "/settings"))
}
