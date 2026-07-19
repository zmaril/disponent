#!/usr/bin/env bash
# Regenerate every artifact derived from the fluessig catalog: the emitted
# schema/{catalog,api}.json, the schema-as-code module (schema_gen.rs), the
# schema docs, and the MCP surface (DTOs + trait + dispatch + tools manifest).
#
# disponent's schema is authored as Rust derives in crates/disponent-schema (the
# fluessig Rust-derive front end) — there is no TypeSpec or Node in this chain
# anymore.
#
# fluessig is located via $FLUESSIG_DIR:
#   - locally: a sibling checkout (defaults to ../fluessig next to this repo).
#   - in CI:   a pinned clone (see .github/workflows/ci.yml), exported as FLUESSIG_DIR.
#
# The chain: crates/disponent-schema --(cargo run emit bins)--> schema/{catalog,api}.json
#            schema/catalog.json --(fluessig-gen)--> the committed generated files.
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FLUESSIG_DIR="${FLUESSIG_DIR:-$(cd "$REPO/../fluessig" 2>/dev/null && pwd || true)}"

if [ -z "${FLUESSIG_DIR:-}" ] || [ ! -f "$FLUESSIG_DIR/Cargo.toml" ]; then
  echo "error: fluessig not found. Set FLUESSIG_DIR to a fluessig checkout" >&2
  echo "       (git clone https://github.com/zmaril/fluessig), or place one at ../fluessig." >&2
  exit 1
fi

# 1. crates/disponent-schema (the derive front end) -> catalog.json + api.json.
#    The schema crate is its own single-crate workspace; it reaches the fluessig
#    derive crates through a gitignored `fluessig/` symlink → $FLUESSIG_DIR (the
#    same $FLUESSIG_DIR the downstream fluessig-gen stage uses). Refresh it here so
#    a fresh checkout / a moved FLUESSIG_DIR resolves cleanly.
SCHEMA="$REPO/crates/disponent-schema"
ln -sfn "$FLUESSIG_DIR" "$SCHEMA/fluessig"
cargo run -q --manifest-path "$SCHEMA/Cargo.toml" --bin fluessig-emit \
  > "$REPO/schema/catalog.json"
cargo run -q --manifest-path "$SCHEMA/Cargo.toml" --bin fluessig-emit-api \
  > "$REPO/schema/api.json"

# 2. catalog.json + api.json -> the committed generated files
#
# `--*-union-mode envelope`: keep the EventPayload union projected as the historical
# JSON-string carrier (`payload: String`) across all three bindings — disponent's
# established surface. fluessig #40 made structured tagged-object projection
# (`Either{N}`) the default; opting back into the envelope keeps this migration a
# pure front-end (TypeSpec→derive) + streaming-runtime change, leaving the binding
# payload surface byte-identical. Adopting the structured projection is a separate,
# deliberate decision.
cargo run -q --manifest-path "$FLUESSIG_DIR/Cargo.toml" --bin fluessig-gen -- \
  "$REPO/schema/catalog.json" "$REPO/crates/disponent-core/src/schema_gen.rs" \
  --docs "$REPO/schema/schema_docs.json" \
  --api "$REPO/schema/api.json" \
  --mcp "$REPO/crates/disponent-core/src/mcp_generated.rs" \
  --node "$REPO/crates/disponent-node/src/generated.rs" \
  --python "$REPO/crates/disponent-python/src/generated.rs" \
  --ruby "$REPO/crates/disponent-ruby/src/generated.rs" \
  --node-union-mode envelope \
  --python-union-mode envelope \
  --ruby-union-mode envelope \
  --banner-note 'straitjacket-allow-file:duplication — generated code repeats by design.'
