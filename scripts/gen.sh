#!/usr/bin/env bash
# Regenerate every artifact derived from the fluessig catalog: the emitted
# schema/{catalog,api}.json, the schema-as-code module (schema_gen.rs), the
# schema docs, and the MCP surface (DTOs + trait + dispatch + tools manifest).
#
# fluessig is located via $FLUESSIG_DIR:
#   - locally: a sibling checkout (defaults to ../fluessig next to this repo).
#   - in CI:   a pinned clone, exported as FLUESSIG_DIR.
#
# The chain: schema/disponent.tsp --(emitter)--> schema/{catalog,api}.json
#            --(fluessig-gen)--> the committed generated files.
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FLUESSIG_DIR="${FLUESSIG_DIR:-$(cd "$REPO/../fluessig" 2>/dev/null && pwd || true)}"

if [ -z "${FLUESSIG_DIR:-}" ] || [ ! -f "$FLUESSIG_DIR/Cargo.toml" ]; then
  echo "error: fluessig not found. Set FLUESSIG_DIR to a fluessig checkout" >&2
  echo "       (git clone https://github.com/zmaril/fluessig), or place one at ../fluessig." >&2
  exit 1
fi

# 1. disponent.tsp -> catalog.json + api.json (the emitter needs its node deps)
if [ ! -d "$FLUESSIG_DIR/emitter/node_modules" ]; then
  (cd "$FLUESSIG_DIR/emitter" && npm install)
fi
# disponent.tsp imports the decorator library by the relative path it has when
# it sits beside the tool (`./typespec/lib.tsp`); compile a staging copy with
# the lib symlinked next to it.
STAGE="$(mktemp -d)"
trap 'rm -rf "$STAGE"' EXIT
cp "$REPO/schema/disponent.tsp" "$STAGE/disponent.tsp"
ln -s "$FLUESSIG_DIR/typespec" "$STAGE/typespec"
(cd "$FLUESSIG_DIR/emitter" && node emit.mjs "$STAGE/disponent.tsp" --out "$REPO/schema")

# 2. catalog.json + api.json -> the committed generated files
cargo run -q --manifest-path "$FLUESSIG_DIR/Cargo.toml" --bin fluessig-gen -- \
  "$REPO/schema/catalog.json" "$REPO/crates/disponent-core/src/schema_gen.rs" \
  --docs "$REPO/schema/schema_docs.json" \
  --api "$REPO/schema/api.json" \
  --mcp "$REPO/crates/disponent-core/src/mcp_generated.rs" \
  --node "$REPO/crates/disponent-node/src/generated.rs" \
  --python "$REPO/crates/disponent-python/src/generated.rs" \
  --ruby "$REPO/crates/disponent-ruby/src/generated.rs" \
  --banner-note 'straitjacket-allow-file:duplication — generated code repeats by design.'
