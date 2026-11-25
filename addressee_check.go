package voiceintake

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/kayushkin/llm-bridge/msg"
)

// AddresseeVerdict is the addressee check's answer: were the words meant
// for the assistant, or did the microphone pick up something else?
type AddresseeVerdict struct {
	ForAssistant bool   `json:"for_assistant"`
	Reason       string `json:"reason"`
	// Model is the model that answered, as the bridge reports it.
	Model string `json:"-"`
}

// AddresseeJudge decides whether the words of an utterance were meant for
// the assistant.
type AddresseeJudge interface {
	Judge(ctx context.Context, modelRole, text string) (AddresseeVerdict, error)
}

// BridgeAddresseeJudge asks one fast model through llm-bridge-server's
// POST /oneshot, which routes a model-store role to the first model that
// answers.
type BridgeAddresseeJudge struct {
	BaseURL      string
	ServiceToken string
	Client       *http.Client
}

// addresseeCheckLimit is how long one check may take. The session answers
// beside it, so a slow check holds nothing up; past this it is recorded as
// failed.
const addresseeCheckLimit = 30 * time.Second

// addresseeCheckMaximumTokens leaves a model that thinks before it answers
// room to do both. With 200, on 2026-11-23, a model spent every token
// thinking and answered nothing, seven times in one utterance.
const addresseeCheckMaximumTokens = 2000

// NewBridgeAddresseeJudge builds a judge.
func NewBridgeAddresseeJudge(baseURL, serviceToken string) *BridgeAddresseeJudge {
	return &BridgeAddresseeJudge{BaseURL: strings.TrimRight(baseURL, "/"), ServiceToken: serviceToken, Client: &http.Client{Timeout: addresseeCheckLimit}}
}

const addresseeCheckSystemPrompt = `A person pressed a voice button on their phone, or the phone started recording by accident. You read the speech-recognition transcript of what the microphone heard, which may hold recognition errors.

Decide whether the words are meant for the voice assistant: a question, a request, a note or a command spoken to it, however short or casual. They are not meant for it when they are plainly one side of a talk with other people, talk between other people, a phone call, or a television or radio picked up by accident: answers such as "yeah" and "oh no" with nothing asked, remarks about people and events addressed to someone present, speech that changes subject as a conversation does.

When unsure, answer that the words are for the assistant: wrongly holding back a real request costs more than answering a stray one.

Examples:
- "what's the weather like in the city tomorrow": for the assistant.
- "remind me to call the dentist at nine": for the assistant.
- "add milk to the list": for the assistant.
- "Yeah, they're doing like daily calls like every morning. That's why, yeah. I was really bad last weekend.": not for the assistant.
- "No, like the one group has to paint the other one. That could be interesting.": not for the assistant.

Answer with for_assistant true or false and a reason of a few words.`

var addresseeVerdictSchema = json.RawMessage(`{"type":"object","properties":{"for_assistant":{"type":"boolean"},"reason":{"type":"string"}},"required":["for_assistant","reason"],"additionalProperties":false}`)

// Judge asks the model.
func (j *BridgeAddresseeJudge) Judge(ctx context.Context, modelRole, text string) (AddresseeVerdict, error) {
	if modelRole == "" {
		return AddresseeVerdict{}, fmt.Errorf("%s is empty, so there is no model to ask", SettingAddresseeCheckModelRole)
	}
	request := msg.OneShotRequest{
		SystemPrompt: addresseeCheckSystemPrompt,
		Prompt:       fmt.Sprintf("Transcript: %q", text),
		ModelRole:    modelRole,
		Caller:       "voice-intake-addressee-check",
		Schema:       addresseeVerdictSchema,
		MaxTokens:    addresseeCheckMaximumTokens,
	}
	encoded, err := json.Marshal(request)
	if err != nil {
		return AddresseeVerdict{}, err
	}
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, j.BaseURL+"/oneshot", bytes.NewReader(encoded))
	if err != nil {
		return AddresseeVerdict{}, err
	}
	httpRequest.Header.Set("Content-Type", "application/json")
	httpRequest.Header.Set("X-LLM-Bridge-Service-Token", j.ServiceToken)
	response, err := j.Client.Do(httpRequest)
	if err != nil {
		return AddresseeVerdict{}, fmt.Errorf("llm-bridge-server POST /oneshot: %w", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return AddresseeVerdict{}, err
	}
	if response.StatusCode != http.StatusOK {
		return AddresseeVerdict{}, fmt.Errorf("llm-bridge-server POST /oneshot answered %d: %s", response.StatusCode, strings.TrimSpace(string(body)))
	}
	var answer struct {
		Text   string          `json:"text"`
		Parsed json.RawMessage `json:"parsed"`
		Model  string          `json:"model"`
	}
	if err := json.Unmarshal(body, &answer); err != nil {
		return AddresseeVerdict{}, fmt.Errorf("decode /oneshot answer: %w", err)
	}
	if len(answer.Parsed) == 0 {
		return AddresseeVerdict{}, fmt.Errorf("model %s answered without the verdict schema: %q", answer.Model, answer.Text)
	}
	var verdict struct {
		ForAssistant *bool  `json:"for_assistant"`
		Reason       string `json:"reason"`
	}
	if err := json.Unmarshal(answer.Parsed, &verdict); err != nil || verdict.ForAssistant == nil {
		return AddresseeVerdict{}, fmt.Errorf("model %s answered %s, which holds no for_assistant verdict", answer.Model, answer.Parsed)
	}
	return AddresseeVerdict{ForAssistant: *verdict.ForAssistant, Reason: verdict.Reason, Model: answer.Model}, nil
}
