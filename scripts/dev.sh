#!/usr/bin/env bash
# Stand up the disponent dev environment: build the workspace and check the
# tools the backends need. Safe to run from anywhere.
set -euo pipefail
cd "$(dirname "$0")/.."

# tmux backs the local session backend and its integration tests.
if ! command -v tmux >/dev/null 2>&1; then
  echo "warning: tmux not found — the local backend and its tests need it" >&2
  echo "  install with 'brew install tmux' (macOS) or 'apt-get install tmux' (Debian/Ubuntu)" >&2
fi

echo "building the workspace…"
cargo build

echo
echo "dev environment ready:"
echo "  cargo test        # run the suite (local-backend tests need tmux)"
echo "  scripts/gen.sh    # regenerate code from the pinned fluessig schema"
