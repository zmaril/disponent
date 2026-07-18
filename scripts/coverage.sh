#!/usr/bin/env bash
# Code coverage for the disponent workspace via cargo-llvm-cov (stable +
# llvm-tools-preview). Presence-checked by housekeeper's `coverage` check.
#   Install once:  cargo install cargo-llvm-cov
#   Usage:         scripts/coverage.sh            # summary to stdout
#                  scripts/coverage.sh --html      # HTML report under target/llvm-cov
set -euo pipefail
cd "$(dirname "$0")/.."
exec cargo llvm-cov --workspace --summary-only "$@"
