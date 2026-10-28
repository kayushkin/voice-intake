package voiceintake

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/kayushkin/llm-bridge/servicesettings"
)

const testPublishToken = "publish-token-for-tests"

func newAppReleaseServer(t *testing.T, publishToken string) *httptest.Server {
	t.Helper()
	registry, err := NewSettingsRegistry(servicesettings.MapEnvironment(map[string]string{"LLMBRIDGE_SERVICE_TOKEN": "x"}))
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	store, err := OpenStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	server := NewServer(store, nil, nil, registry, nil, time.Now)
	server.SetAppReleaseDirectory(directory + "/app-releases")
	server.SetAppReleasePublishToken(publishToken)
	httpServer := httptest.NewServer(server.Handler())
	t.Cleanup(httpServer.Close)
	return httpServer
}

func fakeAPK(content string) []byte { return append([]byte("PK\x03\x04"), content...) }

func publish(t *testing.T, server *httptest.Server, token string, versionCode, versionName string, body []byte) (*http.Response, []byte) {
	t.Helper()
	query := url.Values{"version_code": {versionCode}, "version_name": {versionName}, "commit": {"abc1234"}}
	request, _ := http.NewRequest(http.MethodPost, server.URL+"/admin/app/releases?"+query.Encode(), bytes.NewReader(body))
	request.Header.Set("Content-Type", AppReleaseContentType)
	if token != "" {
		request.Header.Set(AppReleasePublishTokenHeader, token)
	}
	return do(t, request)
}

func do(t *testing.T, request *http.Request) (*http.Response, []byte) {
	t.Helper()
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return response, body
}

func phoneGet(t *testing.T, server *httptest.Server, path string) (*http.Response, []byte) {
	t.Helper()
	request, _ := http.NewRequest(http.MethodGet, server.URL+path, nil)
	request.Header.Set("X-Principal-Id", "principal_000001")
	return do(t, request)
}

func TestAPublishedReleaseIsTheLatestAndDownloadsWithItsHash(t *testing.T) {
	server := newAppReleaseServer(t, testPublishToken)

	if response, _ := phoneGet(t, server, "/api/voice/app/releases/latest"); response.StatusCode != http.StatusNotFound {
		t.Fatalf("latest with nothing published = %d, want 404", response.StatusCode)
	}
	apk := fakeAPK("version twenty")
	response, body := publish(t, server, testPublishToken, "20", "0.6.0", apk)
	if response.StatusCode != http.StatusCreated {
		t.Fatalf("publish = %d %s", response.StatusCode, body)
	}
	digest := sha256.Sum256(apk)
	wantHash := hex.EncodeToString(digest[:])

	response, body = phoneGet(t, server, "/api/voice/app/releases/latest")
	if response.StatusCode != http.StatusOK {
		t.Fatalf("latest = %d %s", response.StatusCode, body)
	}
	var latest map[string]any
	if err := json.Unmarshal(body, &latest); err != nil {
		t.Fatal(err)
	}
	if latest["version_code"] != float64(20) || latest["version_name"] != "0.6.0" || latest["commit"] != "abc1234" ||
		latest["sha256"] != wantHash || latest["size_bytes"] != float64(len(apk)) || latest["published_at_ms"] == nil {
		t.Fatalf("latest = %s", body)
	}
	if _, leaked := latest["filename"]; leaked {
		t.Errorf("latest carries the server's filename: %s", body)
	}

	response, body = phoneGet(t, server, "/api/voice/app/releases/20/apk")
	if response.StatusCode != http.StatusOK || !bytes.Equal(body, apk) {
		t.Fatalf("download = %d, %d bytes", response.StatusCode, len(body))
	}
	if got := response.Header.Get("Content-Type"); got != AppReleaseContentType {
		t.Errorf("Content-Type = %q", got)
	}
	if response, _ := phoneGet(t, server, "/api/voice/app/releases/21/apk"); response.StatusCode != http.StatusNotFound {
		t.Errorf("an unpublished version = %d, want 404", response.StatusCode)
	}

	// A later release becomes the latest; the earlier one still downloads.
	if response, body := publish(t, server, testPublishToken, "21", "0.6.1", fakeAPK("version twenty-one")); response.StatusCode != http.StatusCreated {
		t.Fatalf("second publish = %d %s", response.StatusCode, body)
	}
	_, body = phoneGet(t, server, "/api/voice/app/releases/latest")
	if err := json.Unmarshal(body, &latest); err != nil || latest["version_code"] != float64(21) {
		t.Fatalf("latest after 21 = %s", body)
	}
	if response, body := phoneGet(t, server, "/api/voice/app/releases/20/apk"); response.StatusCode != http.StatusOK || !bytes.Equal(body, apk) {
		t.Fatalf("release 20 after 21 = %d", response.StatusCode)
	}

	request, _ := http.NewRequest(http.MethodGet, server.URL+"/admin/app/releases", nil)
	request.Header.Set(AppReleasePublishTokenHeader, testPublishToken)
	response, body = do(t, request)
	var every []AppRelease
	if err := json.Unmarshal(body, &every); err != nil || response.StatusCode != http.StatusOK || len(every) != 2 || every[0].VersionCode != 21 {
		t.Fatalf("list = %d %s", response.StatusCode, body)
	}
}

func TestPublishingRefusesABadTokenAnOldVersionAndANonAPK(t *testing.T) {
	server := newAppReleaseServer(t, testPublishToken)
	if response, _ := publish(t, server, "", "20", "0.6.0", fakeAPK("x")); response.StatusCode != http.StatusUnauthorized {
		t.Errorf("no token = %d, want 401", response.StatusCode)
	}
	if response, _ := publish(t, server, "wrong", "20", "0.6.0", fakeAPK("x")); response.StatusCode != http.StatusUnauthorized {
		t.Errorf("wrong token = %d, want 401", response.StatusCode)
	}
	if response, _ := publish(t, server, testPublishToken, "20", "0.6.0", []byte("not a zip")); response.StatusCode != http.StatusBadRequest {
		t.Errorf("not an APK = %d, want 400", response.StatusCode)
	}
	if response, _ := publish(t, server, testPublishToken, "zero", "0.6.0", fakeAPK("x")); response.StatusCode != http.StatusBadRequest {
		t.Errorf("bad version_code = %d, want 400", response.StatusCode)
	}
	if response, body := publish(t, server, testPublishToken, "20", "0.6.0", fakeAPK("x")); response.StatusCode != http.StatusCreated {
		t.Fatalf("publish = %d %s", response.StatusCode, body)
	}
	if response, _ := publish(t, server, testPublishToken, "20", "0.6.0", fakeAPK("y")); response.StatusCode != http.StatusConflict {
		t.Errorf("same version_code again = %d, want 409", response.StatusCode)
	}
	if response, _ := publish(t, server, testPublishToken, "19", "0.5.9", fakeAPK("y")); response.StatusCode != http.StatusConflict {
		t.Errorf("lower version_code = %d, want 409", response.StatusCode)
	}
	// The phone routes stay behind the principal gate.
	if response, _ := do(t, mustRequest(t, server.URL+"/api/voice/app/releases/latest")); response.StatusCode != http.StatusUnauthorized {
		t.Errorf("latest without X-Principal-Id = %d, want 401", response.StatusCode)
	}
}

func TestPublishingWithNoTokenSetIsUnavailable(t *testing.T) {
	server := newAppReleaseServer(t, "")
	if response, _ := publish(t, server, "anything", "20", "0.6.0", fakeAPK("x")); response.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("publish with no token configured = %d, want 503", response.StatusCode)
	}
}

func mustRequest(t *testing.T, address string) *http.Request {
	t.Helper()
	request, err := http.NewRequest(http.MethodGet, address, nil)
	if err != nil {
		t.Fatal(err)
	}
	return request
}
