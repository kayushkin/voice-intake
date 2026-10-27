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

// ContinuationVerdict is the judge's answer: does the new segment continue
// the earlier request, or is it a request of its own?
type ContinuationVerdict struct {
	Continues bool   `json:"continues"`
	Reason    string `json:"reason"`
	// Model is the model that answered, as the bridge reports it.
	Model string `json:"-"`
}

// ContinuationJudge decides whether a new segment continues an earlier
// request.
type ContinuationJudge interface {
	Judge(ctx context.Context, modelRole, earlierText, newText string) (ContinuationVerdict, error)
}

// BridgeContinuationJudge asks one fast model through llm-bridge-server's
// POST /oneshot, which routes a model-store role to the first model that
// answers.
type BridgeContinuationJudge struct {
	BaseURL      string
	ServiceToken string
	Client       *http.Client
}

// NewBridgeContinuationJudge builds a judge.
func NewBridgeContinuationJudge(baseURL, serviceToken string) *BridgeContinuationJudge {
	return &BridgeContinuationJudge{BaseURL: strings.TrimRight(baseURL, "/"), ServiceToken: serviceToken, Client: &http.Client{Timeout: 20 * time.Second}}
}

const continuationJudgeSystemPrompt = `You sort speech from one press of a voice button. The person spoke, paused, and spoke again. Both parts are speech-recognition transcripts and may hold recognition errors.

Decide whether the new speech continues the earlier request (adds to it, finishes it, corrects it, or narrows it, so the two should be read as one message), or is a separate request that stands on its own.

Examples:
- earlier "what's the weather like", new "in São Paulo": continues.
- earlier "remind me to call the dentist", new "tomorrow at nine": continues.
- earlier "what's the weather like", new "can you add cheese to my burger": separate.
- earlier "turn off the lights", new "and what time is it": separate.

Answer with continues true or false and a reason of a few words.`

var continuationVerdictSchema = json.RawMessage(`{"type":"object","properties":{"continues":{"type":"boolean"},"reason":{"type":"string"}},"required":["continues","reason"],"additionalProperties":false}`)

// Judge asks the model.
func (j *BridgeContinuationJudge) Judge(ctx context.Context, modelRole, earlierText, newText string) (ContinuationVerdict, error) {
	if modelRole == "" {
		return ContinuationVerdict{}, fmt.Errorf("continuation_judge_model_role is empty, so there is no model to ask")
	}
	request := msg.OneShotRequest{
		SystemPrompt: continuationJudgeSystemPrompt,
		Prompt:       fmt.Sprintf("Earlier speech: %q\nNew speech: %q", earlierText, newText),
		ModelRole:    modelRole,
		Caller:       "voice-intake-continuation-judge",
		Schema:       continuationVerdictSchema,
		MaxTokens:    200,
	}
	encoded, err := json.Marshal(request)
	if err != nil {
		return ContinuationVerdict{}, err
	}
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, j.BaseURL+"/oneshot", bytes.NewReader(encoded))
	if err != nil {
		return ContinuationVerdict{}, err
	}
	httpRequest.Header.Set("Content-Type", "application/json")
	httpRequest.Header.Set("X-LLM-Bridge-Service-Token", j.ServiceToken)
	response, err := j.Client.Do(httpRequest)
	if err != nil {
		return ContinuationVerdict{}, fmt.Errorf("llm-bridge-server POST /oneshot: %w", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return ContinuationVerdict{}, err
	}
	if response.StatusCode != http.StatusOK {
		return ContinuationVerdict{}, fmt.Errorf("llm-bridge-server POST /oneshot answered %d: %s", response.StatusCode, strings.TrimSpace(string(body)))
	}
	var answer struct {
		Text   string          `json:"text"`
		Parsed json.RawMessage `json:"parsed"`
		Model  string          `json:"model"`
	}
	if err := json.Unmarshal(body, &answer); err != nil {
		return ContinuationVerdict{}, fmt.Errorf("decode /oneshot answer: %w", err)
	}
	if len(answer.Parsed) == 0 {
		return ContinuationVerdict{}, fmt.Errorf("model %s answered without the verdict schema: %q", answer.Model, answer.Text)
	}
	var verdict struct {
		Continues *bool  `json:"continues"`
		Reason    string `json:"reason"`
	}
	if err := json.Unmarshal(answer.Parsed, &verdict); err != nil || verdict.Continues == nil {
		return ContinuationVerdict{}, fmt.Errorf("model %s answered %s, which holds no continues verdict", answer.Model, answer.Parsed)
	}
	return ContinuationVerdict{Continues: *verdict.Continues, Reason: verdict.Reason, Model: answer.Model}, nil
}
