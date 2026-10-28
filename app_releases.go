package voiceintake

import (
	"bytes"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// The phone app's releases. voice-intake owns the phone client's releases: a
// build machine publishes each signed APK through the loopback-only admin
// route, and the app asks the phone routes whether a newer one exists and
// downloads it, behind the same sign-in as the rest of /api/voice/.

// AppReleasePublishTokenHeader carries the publish token on the admin route.
const AppReleasePublishTokenHeader = "X-Voice-Intake-Publish-Token"

// AppReleaseContentType is the content type of an APK.
const AppReleaseContentType = "application/vnd.android.package-archive"

// maximumAppReleaseBytes bounds a published APK.
const maximumAppReleaseBytes = 256 << 20

// zipLocalFileHeader is how every APK, a zip file, begins.
var zipLocalFileHeader = []byte("PK\x03\x04")

// AppRelease is one published build of the phone app.
type AppRelease struct {
	VersionCode   int64  `json:"version_code"`
	VersionName   string `json:"version_name"`
	Commit        string `json:"commit"`
	SHA256        string `json:"sha256"`
	SizeBytes     int64  `json:"size_bytes"`
	PublishedAtMs int64  `json:"published_at_ms"`
	// Filename is where the APK lies in the release directory; not served.
	Filename string `json:"-"`
}

// ErrAppReleaseNotNewer is a publish whose version code is not above every
// published one.
var ErrAppReleaseNotNewer = errors.New("version_code is not greater than every published release")

// InsertAppRelease records a release. A version code at or below the highest
// already published is ErrAppReleaseNotNewer, so the phone never sees the
// latest release go backwards.
func (s *Store) InsertAppRelease(release AppRelease) error {
	var highest sql.NullInt64
	if err := s.db.QueryRow(`SELECT MAX(version_code) FROM app_releases`).Scan(&highest); err != nil {
		return fmt.Errorf("read highest app release: %w", err)
	}
	if highest.Valid && release.VersionCode <= highest.Int64 {
		return fmt.Errorf("%w: %d is not above %d", ErrAppReleaseNotNewer, release.VersionCode, highest.Int64)
	}
	return s.exec(`INSERT INTO app_releases (version_code, version_name, commit_sha, sha256, size_bytes, filename, published_at_ms)
		VALUES (?, ?, ?, ?, ?, ?, ?)`,
		release.VersionCode, release.VersionName, release.Commit, release.SHA256, release.SizeBytes, release.Filename, release.PublishedAtMs)
}

const appReleaseColumns = `version_code, version_name, commit_sha, sha256, size_bytes, filename, published_at_ms`

func scanAppRelease(row rowScanner) (AppRelease, error) {
	var release AppRelease
	err := row.Scan(&release.VersionCode, &release.VersionName, &release.Commit, &release.SHA256, &release.SizeBytes, &release.Filename, &release.PublishedAtMs)
	if errors.Is(err, sql.ErrNoRows) {
		return AppRelease{}, ErrNotFound
	}
	return release, err
}

// LatestAppRelease is the release with the highest version code.
func (s *Store) LatestAppRelease() (AppRelease, error) {
	return scanAppRelease(s.db.QueryRow(`SELECT ` + appReleaseColumns + ` FROM app_releases ORDER BY version_code DESC LIMIT 1`))
}

// GetAppRelease is the release with this version code.
func (s *Store) GetAppRelease(versionCode int64) (AppRelease, error) {
	return scanAppRelease(s.db.QueryRow(`SELECT `+appReleaseColumns+` FROM app_releases WHERE version_code = ?`, versionCode))
}

// AppReleases is every release, newest first.
func (s *Store) AppReleases() ([]AppRelease, error) {
	rows, err := s.db.Query(`SELECT ` + appReleaseColumns + ` FROM app_releases ORDER BY version_code DESC`)
	if err != nil {
		return nil, fmt.Errorf("list app releases: %w", err)
	}
	defer rows.Close()
	releases := []AppRelease{}
	for rows.Next() {
		release, err := scanAppRelease(rows)
		if err != nil {
			return nil, err
		}
		releases = append(releases, release)
	}
	return releases, rows.Err()
}

// SetAppReleaseDirectory names the directory published APKs are kept in.
// Unset, the release routes answer 503.
func (s *Server) SetAppReleaseDirectory(directory string) { s.appReleaseDirectory = directory }

// SetAppReleasePublishToken sets the token the admin route demands. Empty,
// publishing answers 503.
func (s *Server) SetAppReleasePublishToken(token string) { s.appReleasePublishToken = token }

// requirePublishTokenFromLoopback admits a request only from this host and
// only with the publish token. dash does not proxy /admin/, so the phone
// cannot reach it at all.
func (s *Server) requirePublishTokenFromLoopback(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		host, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil || !net.ParseIP(host).IsLoopback() {
			writeError(w, http.StatusForbidden, "the admin routes take requests only from this host")
			return
		}
		if s.appReleasePublishToken == "" {
			writeError(w, http.StatusServiceUnavailable, "VOICE_INTAKE_APP_RELEASE_PUBLISH_TOKEN is not set, so no release can be published")
			return
		}
		given := r.Header.Get(AppReleasePublishTokenHeader)
		if subtle.ConstantTimeCompare([]byte(given), []byte(s.appReleasePublishToken)) != 1 {
			writeError(w, http.StatusUnauthorized, AppReleasePublishTokenHeader+" is missing or wrong")
			return
		}
		next(w, r)
	}
}

// publishAppRelease is POST /admin/app/releases?version_code=…&version_name=…&commit=…
// with the APK as the body. It stores the file, computes its sha256 and
// records the release.
func (s *Server) publishAppRelease(w http.ResponseWriter, r *http.Request) {
	if s.appReleaseDirectory == "" {
		writeError(w, http.StatusServiceUnavailable, "no app release directory is set")
		return
	}
	query := r.URL.Query()
	var problems []string
	versionCode, err := strconv.ParseInt(query.Get("version_code"), 10, 64)
	if err != nil || versionCode <= 0 {
		problems = append(problems, fmt.Sprintf("version_code %q must be a whole number above zero", query.Get("version_code")))
	}
	versionName := strings.TrimSpace(query.Get("version_name"))
	if versionName == "" {
		problems = append(problems, "version_name is required")
	}
	commit := strings.TrimSpace(query.Get("commit"))
	if commit == "" {
		problems = append(problems, "commit is required")
	}
	if contentType := r.Header.Get("Content-Type"); contentType != AppReleaseContentType {
		problems = append(problems, fmt.Sprintf("Content-Type %q: an APK is sent as %s", contentType, AppReleaseContentType))
	}
	if len(problems) > 0 {
		writeError(w, http.StatusBadRequest, strings.Join(problems, "; "))
		return
	}
	if _, err := s.store.GetAppRelease(versionCode); err == nil {
		writeError(w, http.StatusConflict, fmt.Sprintf("version_code %d is already published", versionCode))
		return
	} else if !errors.Is(err, ErrNotFound) {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if err := os.MkdirAll(s.appReleaseDirectory, 0o700); err != nil {
		writeError(w, http.StatusInternalServerError, "create app release directory: "+err.Error())
		return
	}
	temporary, err := os.CreateTemp(s.appReleaseDirectory, ".upload-*.apk")
	if err != nil {
		writeError(w, http.StatusInternalServerError, "create upload file: "+err.Error())
		return
	}
	temporaryPath := temporary.Name()
	keepTemporary := false
	defer func() {
		if !keepTemporary {
			os.Remove(temporaryPath)
		}
	}()
	hash := sha256.New()
	sizeBytes, copyErr := io.Copy(io.MultiWriter(temporary, hash), http.MaxBytesReader(w, r.Body, maximumAppReleaseBytes))
	closeErr := temporary.Close()
	if copyErr != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(copyErr, &tooLarge) {
			writeError(w, http.StatusRequestEntityTooLarge, fmt.Sprintf("an APK may be at most %d bytes", maximumAppReleaseBytes))
			return
		}
		writeError(w, http.StatusBadRequest, "read the APK: "+copyErr.Error())
		return
	}
	if closeErr != nil {
		writeError(w, http.StatusInternalServerError, "write the APK: "+closeErr.Error())
		return
	}
	if err := checkLooksLikeAnAPK(temporaryPath); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	filename := fmt.Sprintf("voice-bridge-%d.apk", versionCode)
	finalPath := filepath.Join(s.appReleaseDirectory, filename)
	if _, err := os.Stat(finalPath); err == nil {
		writeError(w, http.StatusConflict, fmt.Sprintf("%s already exists in the release directory with no release row; move it away by hand", filename))
		return
	}
	if err := os.Rename(temporaryPath, finalPath); err != nil {
		writeError(w, http.StatusInternalServerError, "keep the APK: "+err.Error())
		return
	}
	keepTemporary = true
	release := AppRelease{VersionCode: versionCode, VersionName: versionName, Commit: commit, SHA256: hex.EncodeToString(hash.Sum(nil)),
		SizeBytes: sizeBytes, PublishedAtMs: s.now().UnixMilli(), Filename: filename}
	if err := s.store.InsertAppRelease(release); err != nil {
		os.Remove(finalPath)
		if errors.Is(err, ErrAppReleaseNotNewer) {
			writeError(w, http.StatusConflict, err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, release)
}

func checkLooksLikeAnAPK(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("read back the APK: %w", err)
	}
	defer file.Close()
	start := make([]byte, len(zipLocalFileHeader))
	if _, err := io.ReadFull(file, start); err != nil || !bytes.Equal(start, zipLocalFileHeader) {
		return errors.New("the body is not an APK: it does not start as a zip file")
	}
	return nil
}

// listAppReleases is GET /admin/app/releases: every release, newest first.
func (s *Server) listAppReleases(w http.ResponseWriter, r *http.Request) {
	releases, err := s.store.AppReleases()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, releases)
}

// latestAppRelease is GET /api/voice/app/releases/latest.
func (s *Server) latestAppRelease(w http.ResponseWriter, r *http.Request) {
	release, err := s.store.LatestAppRelease()
	if errors.Is(err, ErrNotFound) {
		writeError(w, http.StatusNotFound, "no app release is published")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, release)
}

// downloadAppRelease is GET /api/voice/app/releases/{version_code}/apk. It
// answers Range requests, so a broken download can resume.
func (s *Server) downloadAppRelease(w http.ResponseWriter, r *http.Request) {
	if s.appReleaseDirectory == "" {
		writeError(w, http.StatusServiceUnavailable, "no app release directory is set")
		return
	}
	versionCode, err := strconv.ParseInt(r.PathValue("version_code"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("version code %q is not a whole number", r.PathValue("version_code")))
		return
	}
	release, err := s.store.GetAppRelease(versionCode)
	if errors.Is(err, ErrNotFound) {
		writeError(w, http.StatusNotFound, fmt.Sprintf("no app release %d", versionCode))
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	file, err := os.Open(filepath.Join(s.appReleaseDirectory, release.Filename))
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("open the APK of release %d: %v", versionCode, err))
		return
	}
	defer file.Close()
	w.Header().Set("Content-Type", AppReleaseContentType)
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="voice-bridge-%s.apk"`, release.VersionName))
	w.Header().Set("ETag", `"`+release.SHA256+`"`)
	http.ServeContent(w, r, "", time.UnixMilli(release.PublishedAtMs), file)
}
