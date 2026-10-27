package voiceintake

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/kayushkin/llm-bridge/msg"
)

// SessionCreation is what a new voice session is started with.
type SessionCreation struct {
	PrincipalID      string
	InstanceID       string
	Effort           string
	Model            string
	WorkingDirectory string
	DisplayName      string
	// MaxBudgetUSD is the session's spend ceiling; zero sets none.
	MaxBudgetUSD float64
}

// BridgeEvent is one llm-bridge-server event, passed on unchanged.
type BridgeEvent struct {
	Type string
	Data json.RawMessage
}

// SessionBridge is everything voice-intake asks of llm-bridge-server.
type SessionBridge interface {
	CreateSession(ctx context.Context, creation SessionCreation) (string, error)
	SendMessage(ctx context.Context, sessionID, text string) error
	StopSession(ctx context.Context, sessionID string) error
	ArchiveSession(ctx context.Context, sessionID string) error
	RenameSession(ctx context.Context, sessionID, displayName string) error
	SessionState(ctx context.Context, sessionID string) (string, error)
	// StreamSessionEvents calls connected once the stream is open, then
	// deliver for each event until deliver returns false, the stream ends or
	// ctx is done.
	StreamSessionEvents(ctx context.Context, sessionID string, connected func(), deliver func(BridgeEvent) bool) error
	ShareFile(ctx context.Context, sessionID, filename, contentType string, content []byte) error
	InstanceExists(ctx context.Context, instanceID string) (bool, error)
	// InstanceOneshot is one model call on an instance with no session.
	InstanceOneshot(ctx context.Context, instanceID, model, systemPrompt, prompt string) (OneshotAnswer, error)
}

// OneshotAnswer is the bridge's answer to a oneshot call.
type OneshotAnswer struct {
	Text  string `json:"text"`
	Model string `json:"model"`
}

// HTTPSessionBridge calls llm-bridge-server as the internal service.
type HTTPSessionBridge struct {
	BaseURL      string
	ServiceToken string
	Client       *http.Client
	// StreamClient has no overall timeout: an event stream stays open for
	// the length of a turn.
	StreamClient *http.Client

	harnessByInstance sync.Map
}

// NewHTTPSessionBridge builds a bridge client.
func NewHTTPSessionBridge(baseURL, serviceToken string) *HTTPSessionBridge {
	return &HTTPSessionBridge{
		BaseURL:      strings.TrimRight(baseURL, "/"),
		ServiceToken: serviceToken,
		Client:       &http.Client{Timeout: 60 * time.Second},
		StreamClient: &http.Client{},
	}
}

// voiceSessionPurpose and voiceSessionOrigin classify every session this
// service creates. They are real conversations with the person, so they are
// interactive and appear in that person's chat.
const (
	voiceSessionPurpose = "voice"
	voiceSessionOrigin  = "voice-intake"
)

func (b *HTTPSessionBridge) request(ctx context.Context, method, path, principalID string, body io.Reader, contentType string) (*http.Request, error) {
	request, err := http.NewRequestWithContext(ctx, method, b.BaseURL+path, body)
	if err != nil {
		return nil, err
	}
	request.Header.Set("X-LLM-Bridge-Service-Token", b.ServiceToken)
	if principalID != "" {
		request.Header.Set("X-Principal-Id", principalID)
	}
	if contentType != "" {
		request.Header.Set("Content-Type", contentType)
	}
	return request, nil
}

func (b *HTTPSessionBridge) do(ctx context.Context, method, path, principalID string, body any, answer any) error {
	var reader io.Reader
	contentType := ""
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(encoded)
		contentType = "application/json"
	}
	request, err := b.request(ctx, method, path, principalID, reader, contentType)
	if err != nil {
		return err
	}
	response, err := b.Client.Do(request)
	if err != nil {
		return fmt.Errorf("llm-bridge-server %s %s: %w", method, path, err)
	}
	defer response.Body.Close()
	responseBody, err := io.ReadAll(response.Body)
	if err != nil {
		return fmt.Errorf("llm-bridge-server %s %s: read answer: %w", method, path, err)
	}
	if response.StatusCode/100 != 2 {
		return fmt.Errorf("llm-bridge-server %s %s answered %d: %s", method, path, response.StatusCode, strings.TrimSpace(string(responseBody)))
	}
	if answer != nil {
		if err := json.Unmarshal(responseBody, answer); err != nil {
			return fmt.Errorf("llm-bridge-server %s %s: decode answer: %w", method, path, err)
		}
	}
	return nil
}

// instanceHarness reads the harness type of an instance from the bridge,
// which owns it, and keeps the answer: an instance's harness does not change.
func (b *HTTPSessionBridge) instanceHarness(ctx context.Context, instanceID string) (string, error) {
	if harness, ok := b.harnessByInstance.Load(instanceID); ok {
		return harness.(string), nil
	}
	instances, err := b.instances(ctx)
	if err != nil {
		return "", err
	}
	for _, instance := range instances {
		if instance.ID == instanceID {
			b.harnessByInstance.Store(instanceID, instance.HarnessType)
			return instance.HarnessType, nil
		}
	}
	return "", fmt.Errorf("llm-bridge-server has no instance %q", instanceID)
}

type bridgeInstance struct {
	ID          string `json:"id"`
	HarnessType string `json:"harness_type"`
}

func (b *HTTPSessionBridge) instances(ctx context.Context) ([]bridgeInstance, error) {
	var instances []bridgeInstance
	if err := b.do(ctx, http.MethodGet, "/instances", "", nil, &instances); err != nil {
		return nil, err
	}
	return instances, nil
}

// InstanceExists asks the bridge whether it has the instance.
func (b *HTTPSessionBridge) InstanceExists(ctx context.Context, instanceID string) (bool, error) {
	instances, err := b.instances(ctx)
	if err != nil {
		return false, err
	}
	for _, instance := range instances {
		if instance.ID == instanceID {
			return true, nil
		}
	}
	return false, nil
}

// CreateSession starts a session as the person, so it is theirs and shows in
// their chat.
func (b *HTTPSessionBridge) CreateSession(ctx context.Context, creation SessionCreation) (string, error) {
	if creation.InstanceID == "" {
		return "", fmt.Errorf("no instance_id is set, so there is nowhere to start a session")
	}
	harness, err := b.instanceHarness(ctx, creation.InstanceID)
	if err != nil {
		return "", err
	}
	harnessConfig := map[string]string{}
	if creation.Effort != "" {
		harnessConfig["effort"] = creation.Effort
	}
	if creation.Model != "" {
		harnessConfig["model"] = creation.Model
	}
	encodedConfig, err := json.Marshal(harnessConfig)
	if err != nil {
		return "", err
	}
	request := msg.CreateSessionRequest{
		Harness:       msg.Harness(harness),
		InstanceID:    creation.InstanceID,
		DisplayName:   creation.DisplayName,
		PrincipalID:   creation.PrincipalID,
		AutoStart:     true,
		Type:          msg.SessionTypeInteractive,
		Purpose:       voiceSessionPurpose,
		Origin:        voiceSessionOrigin,
		HarnessConfig: encodedConfig,
		WorkingDir:    creation.WorkingDirectory,
	}
	if creation.MaxBudgetUSD > 0 {
		budget := creation.MaxBudgetUSD
		request.MaxBudget = &budget
	}
	var answer struct {
		SessionID string `json:"session_id"`
	}
	if err := b.do(ctx, http.MethodPost, "/sessions", creation.PrincipalID, request, &answer); err != nil {
		return "", err
	}
	if answer.SessionID == "" {
		return "", fmt.Errorf("llm-bridge-server POST /sessions answered with no session_id")
	}
	return answer.SessionID, nil
}

// InstanceOneshot calls POST /instances/{id}/oneshot, which takes a model.
func (b *HTTPSessionBridge) InstanceOneshot(ctx context.Context, instanceID, model, systemPrompt, prompt string) (OneshotAnswer, error) {
	var answer OneshotAnswer
	request := msg.OneShotRequest{Prompt: prompt, SystemPrompt: systemPrompt, Model: model, Caller: "voice-intake-replay"}
	err := b.do(ctx, http.MethodPost, "/instances/"+url.PathEscape(instanceID)+"/oneshot", "", request, &answer)
	return answer, err
}

// SendMessage sends text into a session as the person's message.
func (b *HTTPSessionBridge) SendMessage(ctx context.Context, sessionID, text string) error {
	return b.do(ctx, http.MethodPost, "/sessions/"+url.PathEscape(sessionID)+"/send", "", msg.SendMessageRequest{Message: text}, nil)
}

// StopSession ends a session.
func (b *HTTPSessionBridge) StopSession(ctx context.Context, sessionID string) error {
	return b.do(ctx, http.MethodPost, "/sessions/"+url.PathEscape(sessionID)+"/stop", "", map[string]any{}, nil)
}

// ArchiveSession moves a session to the chat's Archive.
func (b *HTTPSessionBridge) ArchiveSession(ctx context.Context, sessionID string) error {
	return b.do(ctx, http.MethodPost, "/sessions/"+url.PathEscape(sessionID)+"/mark-done", "", map[string]bool{"done": true}, nil)
}

// RenameSession sets a session's title.
func (b *HTTPSessionBridge) RenameSession(ctx context.Context, sessionID, displayName string) error {
	return b.do(ctx, http.MethodPost, "/sessions/"+url.PathEscape(sessionID)+"/rename", "", msg.RenameSessionRequest{DisplayName: displayName}, nil)
}

// SessionState reads a session's state.
func (b *HTTPSessionBridge) SessionState(ctx context.Context, sessionID string) (string, error) {
	var answer struct {
		State string `json:"state"`
	}
	if err := b.do(ctx, http.MethodGet, "/sessions/"+url.PathEscape(sessionID), "", nil, &answer); err != nil {
		return "", err
	}
	return answer.State, nil
}

// ShareFile shares a file into a session through the bridge, which keeps it
// in file-store.
func (b *HTTPSessionBridge) ShareFile(ctx context.Context, sessionID, filename, contentType string, content []byte) error {
	path := "/sessions/" + url.PathEscape(sessionID) + "/files?filename=" + url.QueryEscape(filename)
	request, err := b.request(ctx, http.MethodPost, path, "", bytes.NewReader(content), contentType)
	if err != nil {
		return err
	}
	response, err := b.Client.Do(request)
	if err != nil {
		return fmt.Errorf("llm-bridge-server POST %s: %w", path, err)
	}
	defer response.Body.Close()
	if response.StatusCode/100 != 2 {
		body, _ := io.ReadAll(response.Body)
		return fmt.Errorf("llm-bridge-server POST %s answered %d: %s", path, response.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}

// StreamSessionEvents reads the session's event stream.
func (b *HTTPSessionBridge) StreamSessionEvents(ctx context.Context, sessionID string, connected func(), deliver func(BridgeEvent) bool) error {
	request, err := b.request(ctx, http.MethodGet, "/sessions/"+url.PathEscape(sessionID)+"/events", "", nil, "")
	if err != nil {
		return err
	}
	request.Header.Set("Accept", "text/event-stream")
	response, err := b.StreamClient.Do(request)
	if err != nil {
		return fmt.Errorf("llm-bridge-server events of %s: %w", sessionID, err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(response.Body)
		return fmt.Errorf("llm-bridge-server events of %s answered %d: %s", sessionID, response.StatusCode, strings.TrimSpace(string(body)))
	}
	connected()
	return readServerSentEvents(response.Body, deliver)
}

// readServerSentEvents parses a text/event-stream body.
func readServerSentEvents(body io.Reader, deliver func(BridgeEvent) bool) error {
	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 64*1024), 16*1024*1024)
	var eventType string
	var data strings.Builder
	for scanner.Scan() {
		line := scanner.Text()
		switch {
		case line == "":
			if data.Len() > 0 {
				if !deliver(BridgeEvent{Type: eventType, Data: json.RawMessage(data.String())}) {
					return nil
				}
			}
			eventType = ""
			data.Reset()
		case strings.HasPrefix(line, ":"):
		case strings.HasPrefix(line, "event:"):
			eventType = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
		case strings.HasPrefix(line, "data:"):
			if data.Len() > 0 {
				data.WriteByte('\n')
			}
			data.WriteString(strings.TrimPrefix(strings.TrimPrefix(line, "data:"), " "))
		}
	}
	return scanner.Err()
}
