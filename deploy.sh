#!/usr/bin/env bash
set -euo pipefail

# One shared gate decides whether this tree may be deployed (main clone, default
# branch, clean, pushed, not behind, and the same for every tree the build reads).
# It lives in healthcheck/scripts/deploy-gate.sh. Do not inline or copy it.
( cd "$(dirname "$0")" && "$HOME/bin/deploy-gate" check )

REPO_DIR="$(cd "$(dirname "$0")" && pwd)"
BIN_DIR="$HOME/bin"
SERVICE="voice-intake.service"
BINARY="voice-intake"
UNIT_SRC="$REPO_DIR/$SERVICE"
UNIT_DEST="$HOME/.config/systemd/user/$SERVICE"


cd "$REPO_DIR"

export PATH="$HOME/.local/share/mise/shims:$PATH"
export XDG_RUNTIME_DIR="${XDG_RUNTIME_DIR:-/run/user/$(id -u)}"
export DBUS_SESSION_BUS_ADDRESS="${DBUS_SESSION_BUS_ADDRESS:-unix:path=${XDG_RUNTIME_DIR}/bus}"

echo "==> Testing..."
go test ./...

# speech-server links whisper.cpp's libparakeet and libwhisper through cgo.
# Where their headers and libraries are is host setup: a build file outside
# the repo sets CGO_CFLAGS and CGO_LDFLAGS (README "Host setup").
SPEECH_SERVER_BUILD_ENV="${XDG_CONFIG_HOME:-$HOME/.config}/speech-server/build.env"
if [ ! -f "$SPEECH_SERVER_BUILD_ENV" ]; then
  echo "ERROR: $SPEECH_SERVER_BUILD_ENV is missing; it sets CGO_CFLAGS and CGO_LDFLAGS for speech-server (README \"Host setup\")"
  exit 1
fi
echo "==> Building speech-server..."
( set -a; . "$SPEECH_SERVER_BUILD_ENV"; set +a; cd speech-server && go vet . && go build -o speech-server . )

echo "==> Building $BINARY..."
go build -o "$BINARY" ./cmd/voice-intake
echo "    built: $(ls -lh "$BINARY" | awk '{print $5}')"

# A set VOICE_INTAKE_ variable that settings.go does not declare stops the new
# binary at boot. Ask before the old one is stopped: build the registry from the
# running service's own environment. The test prints a verdict, never a value.
echo "==> Checking the running service's environment against the declared settings..."
live_pid="$(systemctl --user show -p MainPID --value "$SERVICE" 2>/dev/null || true)"
if [ -n "$live_pid" ] && [ "$live_pid" != "0" ]; then
  go test -count=1 -run '^TestTheLiveProcessEnvironmentBuildsARegistry$' . -args -live-environment-file="/proc/$live_pid/environ"
else
  echo "    $SERVICE is not running, so there is no environment to check"
fi

echo "==> Installing systemd unit..."
mkdir -p "$(dirname "$UNIT_DEST")"
cp "$UNIT_SRC" "$UNIT_DEST"

echo "==> Installing and restarting speech-server..."
cp "$REPO_DIR/speech-server.service" "$HOME/.config/systemd/user/speech-server.service"
systemctl --user stop speech-server.service 2>/dev/null || true
cp speech-server/speech-server "$BIN_DIR/speech-server"
systemctl --user daemon-reload
systemctl --user enable speech-server.service >/dev/null
systemctl --user start speech-server.service
SPEECH_ADDR="$(sed -n 's/^Environment=SPEECH_SERVER_ADDR=//p' "$REPO_DIR/speech-server.service")"
for attempt in $(seq 1 30); do
  curl -sf "http://$SPEECH_ADDR/health" >/dev/null && break
  sleep 1
done
curl -sfS "http://$SPEECH_ADDR/health" >/dev/null || { echo "ERROR: speech-server did not answer /health"; journalctl --user -u speech-server.service -n 20 --no-pager; exit 1; }
echo "    speech-server answers on $SPEECH_ADDR"

echo "==> Stopping $SERVICE..."
systemctl --user stop "$SERVICE" 2>/dev/null || true
sleep 1

echo "==> Installing the binary to $BIN_DIR..."
mkdir -p "$BIN_DIR"
cp "$BINARY" "$BIN_DIR/$BINARY"

echo "==> Starting $SERVICE..."
systemctl --user daemon-reload
systemctl --user enable "$SERVICE" >/dev/null
systemctl --user start "$SERVICE"

echo "==> Verifying..."
sleep 2
if systemctl --user is-active --quiet "$SERVICE"; then
  echo "    $SERVICE is running"
  journalctl --user -u "$SERVICE" -n 5 --no-pager 2>&1 | grep -v '^--' || true
else
  echo "ERROR: $SERVICE failed to start"
  journalctl --user -u "$SERVICE" -n 20 --no-pager 2>&1
  exit 1
fi

# A running process is not a working one: prove the routes answer. The address
# is read from the unit just installed, not assumed.
echo "==> Smoke-checking the API..."
ADDR="$(sed -n 's/^Environment=VOICE_INTAKE_ADDR=//p' "$UNIT_SRC")"
if [ -z "$ADDR" ]; then
  echo "ERROR: $UNIT_SRC sets no VOICE_INTAKE_ADDR"
  exit 1
fi
BASE="http://${ADDR}"
curl -sfS "$BASE/health" >/dev/null || { echo "ERROR: /health did not answer"; exit 1; }
curl -sfS "$BASE/settings" >/dev/null || { echo "ERROR: /settings did not answer"; exit 1; }
# A route behind the principal gate, with a principal that names nobody: it
# proves the gate and the stored settings answer without touching anyone's data.
curl -sfS -H 'X-Principal-Id: deploy-smoke-check' "$BASE/api/voice/settings" >/dev/null || { echo "ERROR: /api/voice/settings did not answer"; exit 1; }
code="$(curl -s -o /dev/null -w '%{http_code}' "$BASE/api/voice/settings")"
[ "$code" = "401" ] || { echo "ERROR: /api/voice/settings without X-Principal-Id answered $code, want 401"; exit 1; }
echo "    /health, /settings and /api/voice/settings answered, and the principal gate refused a bare call"

# The bind is part of the contract: the service trusts X-Principal-Id from
# loopback and must not be reachable off-host. Fail the deploy if it is listening anywhere else.
echo "==> Checking the bind..."
if ss -tlnH "sport = :${ADDR##*:}" | grep -q '127\.0\.0\.1:'; then
  echo "    bound to 127.0.0.1"
else
  echo "ERROR: voice-intake is not bound to 127.0.0.1:"
  ss -tlnp "sport = :${ADDR##*:}"
  exit 1
fi

echo "==> Done."

# Last act: write this deploy to repo-store's ledger, so the next agent sees what is live.
( cd "$(dirname "$0")" && "$HOME/bin/deploy-gate" record )
