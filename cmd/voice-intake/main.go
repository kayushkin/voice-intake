// Command voice-intake serves the voice API: phone transcripts in, agent
// sessions out.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/kayushkin/llm-bridge/servicesettings"
	voiceintake "github.com/kayushkin/voice-intake"
)

func main() {
	registry, err := voiceintake.NewSettingsRegistry(servicesettings.ProcessEnvironment())
	if err != nil {
		log.Fatalf("voice-intake: %v", err)
	}
	if err := registry.CheckRequired(); err != nil {
		log.Fatalf("voice-intake: %v", err)
	}
	dataDirectory := registry.String(voiceintake.SettingDataDirectory)
	store, err := voiceintake.OpenStore(dataDirectory)
	if err != nil {
		log.Fatalf("voice-intake: %v", err)
	}
	defer store.Close()
	if err := registry.AttachStoredValues(store.Settings(), nil); err != nil {
		log.Fatalf("voice-intake: %v", err)
	}

	bridgeURL := registry.String(voiceintake.SettingLLMBridgeURL)
	serviceToken := registry.String(voiceintake.SettingLLMBridgeServiceToken)
	bridge := voiceintake.NewHTTPSessionBridge(bridgeURL, serviceToken)
	judge := voiceintake.NewBridgeContinuationJudge(bridgeURL, serviceToken)
	settings := func() voiceintake.DispatchSettings { return voiceintake.DispatchSettingsFrom(registry) }
	pool := voiceintake.NewSessionPool(store, bridge, settings, time.Now)
	dispatcher := voiceintake.NewDispatcher(store, bridge, judge, pool, settings, time.Now, filepath.Join(dataDirectory, "audio"))
	transcribers := voiceintake.Transcribers{}
	for name, inferenceURL := range registry.StringMap(voiceintake.SettingTranscribers) {
		transcribers[name] = voiceintake.NewWhisperServerTranscriber(inferenceURL)
	}
	var detector voiceintake.SpeechDetector
	if detectionURL := registry.String(voiceintake.SettingSpeechDetectionURL); detectionURL != "" {
		detector = voiceintake.NewSpeechServerDetector(detectionURL)
	} else {
		log.Printf("voice-intake: VOICE_INTAKE_SPEECH_DETECTION_URL is empty; streaming utterances will be refused with 503")
	}
	dispatcher.SetSpeechServices(detector, transcribers)
	dispatcher.SetReplayRecordingDirectory(filepath.Join(dataDirectory, "replay-recordings"))
	server := voiceintake.NewServer(store, dispatcher, pool, registry, transcribers, time.Now)
	server.SetAppReleaseDirectory(filepath.Join(dataDirectory, "app-releases"))
	server.SetAppReleasePublishToken(registry.String(voiceintake.SettingAppReleasePublishToken))

	modelStoreURL := strings.TrimRight(registry.String(voiceintake.SettingModelStoreURL), "/")
	server.SetSettingValidator(voiceintake.SettingContinuationJudgeModelRole, func(ctx context.Context, role string) error {
		var answer struct {
			Roles []string `json:"roles"`
		}
		if err := getJSON(ctx, modelStoreURL+"/api/roles", &answer); err != nil {
			return fmt.Errorf("%w: model-store: %v", servicesettings.ErrSettingOwnerUnavailable, err)
		}
		for _, known := range answer.Roles {
			if known == role {
				return nil
			}
		}
		return fmt.Errorf("model-store has no role %q; it has %s", role, strings.Join(answer.Roles, ", "))
	})
	validateModel := func(ctx context.Context, model string) error {
		var models []struct {
			ID string `json:"id"`
		}
		if err := getJSON(ctx, modelStoreURL+"/api/models", &models); err != nil {
			return fmt.Errorf("%w: model-store: %v", servicesettings.ErrSettingOwnerUnavailable, err)
		}
		for _, known := range models {
			if known.ID == model {
				return nil
			}
		}
		return fmt.Errorf("model-store has no model with id %q", model)
	}
	server.SetSettingValidator(voiceintake.SettingTranscriber, func(ctx context.Context, name string) error {
		_, err := transcribers.Named(name)
		return err
	})
	validateInstance := func(ctx context.Context, instanceID string) error {
		exists, err := bridge.InstanceExists(ctx, instanceID)
		if err != nil {
			return fmt.Errorf("%w: llm-bridge-server: %v", servicesettings.ErrSettingOwnerUnavailable, err)
		}
		if !exists {
			return fmt.Errorf("llm-bridge-server has no instance %q", instanceID)
		}
		return nil
	}
	server.SetSettingValidator(voiceintake.SettingDefaultModel, validateModel)
	server.SetSettingValidator(voiceintake.SettingRaisedModel, validateModel)
	server.SetSettingValidator(voiceintake.SettingInstanceID, validateInstance)
	server.SetSettingValidator(voiceintake.SettingRaisedInstanceID, validateInstance)
	registry.OnChange(func(key string) {
		go pool.Resync(context.Background())
	})

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		if err := pool.Restore(ctx); err != nil {
			log.Printf("[pool] ERROR restore: %v", err)
		}
	}()

	address := registry.String(voiceintake.SettingListenAddress)
	log.Printf("voice-intake listening on %s, llm-bridge-server at %s", address, bridgeURL)
	httpServer := &http.Server{Addr: address, Handler: server.Handler(), ReadHeaderTimeout: 10 * time.Second}
	log.Fatal(httpServer.ListenAndServe())
}

func getJSON(ctx context.Context, url string, into any) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %s answered %d", url, response.StatusCode)
	}
	return json.NewDecoder(response.Body).Decode(into)
}
