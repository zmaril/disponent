#!/usr/bin/env bash
# Local dev loop: check the code, then reinstall the `disponent` binary that the
# Claude Code MCP config points at, so a fresh `claude` session picks up your
# changes.
#
# The MCP server is registered (user scope) as:
#   disponent -> $HOME/.cargo/bin/disponent mcp
# `cargo install --path crates/disponent-cli --force` overwrites exactly that
# binary, so after this runs you just start a NEW claude session (MCP servers
# are enumerated at startup — a session already open won't see the new build).
#
# Usage:
#   scripts/go_dev.sh            # fmt-check + clippy + test, then reinstall
#   scripts/go_dev.sh --quick    # skip the checks, just build + reinstall
#   scripts/go_dev.sh --gen      # run scripts/gen.sh first (needs fluessig)
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO"

QUICK=0
GEN=0
for arg in "$@"; do
  case "$arg" in
    --quick) QUICK=1 ;;
    --gen)   GEN=1 ;;
    -h|--help) grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "unknown flag: $arg (try --help)" >&2; exit 2 ;;
  esac
done

step() { printf '\n\033[1;36m==> %s\033[0m\n' "$1"; }

if [ "$GEN" -eq 1 ]; then
  step "regenerate from crates/disponent-schema"
  scripts/gen.sh
fi

if [ "$QUICK" -eq 0 ]; then
  step "cargo fmt --all --check"
  cargo fmt --all --check

  step "cargo clippy --all-targets -- -D warnings"
  cargo clippy --all-targets -- -D warnings

  step "cargo test"
  cargo test
fi

step "cargo install --path crates/disponent-cli --force"
cargo install --path crates/disponent-cli --force

BIN="$(command -v disponent || echo "$HOME/.cargo/bin/disponent")"
step "installed"
echo "  binary : $BIN"
echo "  version: $("$BIN" --version 2>/dev/null || echo '?')"
echo
echo "Start a NEW claude session to pick up this build (MCP servers load at startup)."
echo "Check it with:  claude mcp get disponent"
