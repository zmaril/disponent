//! Ruby binding for the disponent engine (Magnus) — the Rust sync engine
//! in-process in Ruby. Ruby's GVL serialises access.
//!
//! **The Magnus surface is GENERATED** (`generated.rs`, from the fluessig
//! catalog's op layer — plain DTOs, wrapped output classes with getters,
//! enums-as-wire-strings, `.next`-nil streams); the engine wiring is
//! hand-written once in `core_impl.rs`. Both `@manual` ops (`wait`, `serveMcp`)
//! are omitted here — as entl-ruby omits its `@manual watch` — since reaching
//! the generated class's private `core` from this module isn't possible; a Ruby
//! caller polls `session()` for `wait`-style behaviour and runs the `disponent`
//! CLI for MCP.

mod core_impl;
mod generated;

pub use generated::*;

use magnus::{Error, Ruby};

#[magnus::init(name = "disponent")]
fn init(ruby: &Ruby) -> Result<(), Error> {
    generated::register(ruby)
}
