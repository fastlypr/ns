#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
STATUS_FILE="$PROJECT_DIR/.deploy_status.json"
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

cd "$PROJECT_DIR"

STEP="pull"
git pull origin main
TO_COMMIT="$(git rev-parse HEAD 2>/dev/null || echo "$FROM_COMMIT")"

STEP="restart"
write_status "restarting"
sudo -n systemctl restart ns-bot
