#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
STATUS_FILE="$PROJECT_DIR/.deploy_status.json"
TMP_DIR="$(mktemp -d)"
STATE_BACKUP_FILE="$TMP_DIR/page_tracker_state.json"
STEP="starting"
FROM_COMMIT="$(git -C "$PROJECT_DIR" rev-parse HEAD 2>/dev/null || echo unknown)"
TO_COMMIT="$FROM_COMMIT"

write_status() {
  cat > "$STATUS_FILE" <<EOF
{
  "status": "$1",
  "step": "$STEP",
  "fromCommit": "$FROM_COMMIT",
  "toCommit": "$TO_COMMIT",
  "updatedAt": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
}
EOF
}

trap 'write_status "failed"' ERR
trap 'rm -rf "$TMP_DIR"' EXIT

cd "$PROJECT_DIR"

STEP="prepare"
write_status "running"

if [ -f "$PROJECT_DIR/.page_tracker_state.json" ]; then
  cp "$PROJECT_DIR/.page_tracker_state.json" "$STATE_BACKUP_FILE"
fi

rm -f "$PROJECT_DIR"/rss/*.csv
git checkout -- .page_tracker_state.json 2>/dev/null || true

STEP="pull"
write_status "running"
git pull origin main
TO_COMMIT="$(git rev-parse HEAD 2>/dev/null || echo "$FROM_COMMIT")"

if [ -f "$STATE_BACKUP_FILE" ]; then
  cp "$STATE_BACKUP_FILE" "$PROJECT_DIR/.page_tracker_state.json"
fi

STEP="restart"
write_status "restarting"
sudo -n systemctl restart ns-bot

STEP="complete"
write_status "success"
