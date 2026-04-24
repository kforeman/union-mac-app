#!/usr/bin/env bash
# One-shot installer for union-status (macOS menu bar app).
#
#   curl -fsSL https://raw.githubusercontent.com/kumare3/union-mac-app/main/install.sh | sh
#
# Installs the `union-status` CLI into uv's tool env and registers a
# per-user launchd agent so the menu bar icon appears on login.

set -euo pipefail

REPO="git+https://github.com/kumare3/union-mac-app"

if ! command -v uv >/dev/null 2>&1; then
  echo "uv is required. Install it first:" >&2
  echo "  curl -LsSf https://astral.sh/uv/install.sh | sh" >&2
  exit 1
fi

uv tool install --force "$REPO"

BIN="$(command -v union-status 2>/dev/null || true)"
if [ -z "$BIN" ]; then
  # uv tool installs into ~/.local/bin by default; PATH may not be wired up
  # yet in this shell session.
  BIN="$HOME/.local/bin/union-status"
fi
if [ ! -x "$BIN" ]; then
  echo "union-status binary not found after install (looked at: $BIN)" >&2
  exit 1
fi

LABEL="com.$(whoami).union-status"
PLIST="$HOME/Library/LaunchAgents/$LABEL.plist"
LOG_DIR="$HOME/Library/Logs"
mkdir -p "$LOG_DIR" "$(dirname "$PLIST")"

cat > "$PLIST" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>$LABEL</string>
  <key>ProgramArguments</key>
  <array>
    <string>$BIN</string>
  </array>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
  <key>StandardOutPath</key><string>$LOG_DIR/union-status.log</string>
  <key>StandardErrorPath</key><string>$LOG_DIR/union-status.log</string>
</dict>
</plist>
EOF

# If an older agent is loaded, swap it for the new one.
launchctl unload "$PLIST" 2>/dev/null || true
launchctl load -w "$PLIST"

cat <<EOF

Installed.
  binary:  $BIN
  plist:   $PLIST
  logs:    $LOG_DIR/union-status.log

The Union menu bar icon should appear within a few seconds. Re-run this
command to upgrade; uninstall with:
  launchctl unload "$PLIST" && rm "$PLIST" && uv tool uninstall union-status
EOF
