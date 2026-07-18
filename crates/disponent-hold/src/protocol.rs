//! The attach wire protocol — a small, symmetric framed stream.
//!
//! disponent owns *both* ends of every attach (the engine observer, pm's
//! server, and its own `hold-attach` CLI), so — unlike shpool, which contorts
//! around an unframed human input stream and a raw SIGWINCH — we can frame both
//! directions. Design §6.
//!
//! ## Handshake
//!
//! On connect the holder writes ONE newline-terminated JSON control line —
//! `{"v":1}\n` — then switches to the binary frame stream below. A client reads
//! bytes up to the first `\n`, parses the version, and only then starts reading
//! frames. (JSON-line handshake chosen over a bare version byte to match
//! disponent's stdio-JSON idiom and leave room to carry more handshake fields
//! later without a wire break.)
//!
//! ## Frames
//!
//! `1 byte kind | 4-byte LE u32 len | len bytes payload`. `len` may be zero
//! (empty payload). Payloads are capped at [`MAX_PAYLOAD`]; the holder splits a
//! larger pty read (or ring replay) into successive [`ServerKind::Data`] frames.
//!
//! The two directions reuse the small integer kind space but mean different
//! things — the stream is symmetric in *shape*, not in *vocabulary*:
//!
//! | kind | server→client ([`ServerKind`]) | client→server ([`ClientKind`]) |
//! |------|--------------------------------|--------------------------------|
//! | 0    | `Data` — raw pty bytes         | `Input` — raw bytes → pty master |
//! | 1    | `Heartbeat` — empty, periodic  | `Resize` — 2×u16 LE cols,rows   |
//! | 2    | `Exit` — child exit (below)    | `Detach` — client is leaving    |
//! | 3    | —                              | `Signal` — i32 LE signal → child |
//!
//! `Signal` (M1) delivers a real signal to the held child's process group — the
//! control frame `kill`/`stop_exec` rides so the engine can end a held agent
//! without a shell to type `C-c` into. Interrupt (`C-c`) still rides an `Input`
//! frame; `Signal` is the harder stop (SIGKILL/SIGTERM), additive to the M0
//! vocabulary.
//!
//! ## Exit payload
//!
//! An [`ServerKind::Exit`] frame's payload is **5 bytes**, unambiguous:
//! `1 byte disposition | 4-byte LE i32 value`. Disposition `0` = the child
//! exited normally and `value` is its exit code; disposition `1` = the child was
//! killed by a signal and `value` is the signal number. (A flat i32-with-high-bit
//! would collide with legitimate codes; the explicit disposition byte can't.)

use std::io::{self, Read, Write};

/// Max payload bytes in one frame (design §6). Larger pty reads / ring replays
/// are split across successive `Data` frames.
pub const MAX_PAYLOAD: usize = 16 * 1024;

/// The handshake line the holder writes first, before any frame.
pub const HANDSHAKE: &[u8] = b"{\"v\":1}\n";

/// Protocol version carried in the handshake.
pub const VERSION: u32 = 1;

/// Server→client frame kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServerKind {
    /// Raw pty bytes.
    Data = 0,
    /// Empty keepalive, sent periodically so a dead client (no clean EOF) is
    /// detected as a `BrokenPipe` on the write.
    Heartbeat = 1,
    /// The child exited; payload is the 5-byte disposition+value (see module doc).
    Exit = 2,
}

/// Client→server frame kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientKind {
    /// Raw bytes to write to the pty master.
    Input = 0,
    /// A resize request: payload is `cols: u16 LE` then `rows: u16 LE`.
    Resize = 1,
    /// The client is detaching (closing its end).
    Detach = 2,
    /// Deliver a real signal to the child's process group: payload is
    /// `signal: i32 LE` (M1 — the control frame `kill` rides).
    Signal = 3,
}

impl ClientKind {
    fn from_u8(b: u8) -> Option<ClientKind> {
        match b {
            0 => Some(ClientKind::Input),
            1 => Some(ClientKind::Resize),
            2 => Some(ClientKind::Detach),
            3 => Some(ClientKind::Signal),
            _ => None,
        }
    }
}

impl ServerKind {
    fn from_u8(b: u8) -> Option<ServerKind> {
        match b {
            0 => Some(ServerKind::Data),
            1 => Some(ServerKind::Heartbeat),
            2 => Some(ServerKind::Exit),
            _ => None,
        }
    }
}

/// How a held child ended — the payload of an [`ServerKind::Exit`] frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Exit {
    /// Exited normally with this status code.
    Code(i32),
    /// Killed by this signal number.
    Signal(i32),
}

impl Exit {
    /// The 5-byte payload: disposition byte + LE i32 value.
    pub fn to_payload(self) -> [u8; 5] {
        let (disp, val) = match self {
            Exit::Code(c) => (0u8, c),
            Exit::Signal(s) => (1u8, s),
        };
        let v = val.to_le_bytes();
        [disp, v[0], v[1], v[2], v[3]]
    }

    /// Parse a 5-byte Exit payload. `None` on a malformed length/disposition.
    pub fn from_payload(p: &[u8]) -> Option<Exit> {
        if p.len() != 5 {
            return None;
        }
        let val = i32::from_le_bytes([p[1], p[2], p[3], p[4]]);
        match p[0] {
            0 => Some(Exit::Code(val)),
            1 => Some(Exit::Signal(val)),
            _ => None,
        }
    }

    /// The process exit code a CLI should propagate: the real code, or the
    /// conventional `128 + signal` for a signal death.
    pub fn process_code(self) -> i32 {
        match self {
            Exit::Code(c) => c,
            Exit::Signal(s) => 128 + s,
        }
    }
}

/// Encode a frame (`kind`, `payload`) onto the wire. `payload` must be
/// `<= MAX_PAYLOAD`; callers that may exceed it (Data) split first.
pub fn encode(kind: u8, payload: &[u8]) -> Vec<u8> {
    debug_assert!(payload.len() <= MAX_PAYLOAD);
    let mut out = Vec::with_capacity(5 + payload.len());
    out.push(kind);
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(payload);
    out
}

/// Encode a server frame.
pub fn encode_server(kind: ServerKind, payload: &[u8]) -> Vec<u8> {
    encode(kind as u8, payload)
}

/// Encode `data` as one or more `Data` frames, each `<= MAX_PAYLOAD`.
pub fn encode_data_chunks(data: &[u8], out: &mut Vec<u8>) {
    for chunk in data.chunks(MAX_PAYLOAD) {
        out.extend_from_slice(&encode_server(ServerKind::Data, chunk));
    }
}

/// Encode a client frame.
pub fn encode_client(kind: ClientKind, payload: &[u8]) -> Vec<u8> {
    encode(kind as u8, payload)
}

/// Encode a `Resize` client frame from cols/rows.
pub fn encode_resize(cols: u16, rows: u16) -> Vec<u8> {
    let mut p = Vec::with_capacity(4);
    p.extend_from_slice(&cols.to_le_bytes());
    p.extend_from_slice(&rows.to_le_bytes());
    encode_client(ClientKind::Resize, &p)
}

/// Encode a `Signal` client frame from a signal number.
pub fn encode_signal(sig: i32) -> Vec<u8> {
    encode_client(ClientKind::Signal, &sig.to_le_bytes())
}

/// One decoded server frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerFrame {
    Data(Vec<u8>),
    Heartbeat,
    Exit(Exit),
}

/// One decoded client frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientFrame {
    Input(Vec<u8>),
    Resize {
        cols: u16,
        rows: u16,
    },
    Detach,
    /// Deliver signal `sig` to the held child's process group.
    Signal(i32),
}

/// Read the `{"v":N}` handshake line from a stream, returning the version.
/// Reads a byte at a time up to the newline (the line is tiny), so no frame
/// bytes are consumed.
pub fn read_handshake<R: Read>(r: &mut R) -> io::Result<u32> {
    let mut line = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        let n = r.read(&mut byte)?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "eof before handshake newline",
            ));
        }
        if byte[0] == b'\n' {
            break;
        }
        line.push(byte[0]);
        if line.len() > 256 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "handshake line too long",
            ));
        }
    }
    // Deliberately tiny hand-parse — no serde in this crate. We only need the
    // integer after `"v":`.
    let s = String::from_utf8_lossy(&line);
    let v = s
        .split("\"v\":")
        .nth(1)
        .and_then(|rest| {
            rest.trim_start()
                .trim_end_matches(['}', ' '])
                .trim()
                .parse::<u32>()
                .ok()
        })
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "bad handshake line"))?;
    Ok(v)
}

/// Read exactly one raw frame header+payload, returning `(kind, payload)`.
/// `Ok(None)` at a clean EOF on the header boundary.
fn read_frame<R: Read>(r: &mut R) -> io::Result<Option<(u8, Vec<u8>)>> {
    let mut header = [0u8; 5];
    // Read the first byte separately so a clean EOF (peer detached) is `None`,
    // not an error.
    match r.read(&mut header[..1])? {
        0 => return Ok(None),
        1 => {}
        _ => unreachable!(),
    }
    r.read_exact(&mut header[1..])?;
    let len = u32::from_le_bytes([header[1], header[2], header[3], header[4]]) as usize;
    if len > MAX_PAYLOAD {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "frame payload exceeds MAX_PAYLOAD",
        ));
    }
    let mut payload = vec![0u8; len];
    r.read_exact(&mut payload)?;
    Ok(Some((header[0], payload)))
}

/// Read one server frame. `Ok(None)` at clean EOF.
pub fn read_server_frame<R: Read>(r: &mut R) -> io::Result<Option<ServerFrame>> {
    let Some((kind, payload)) = read_frame(r)? else {
        return Ok(None);
    };
    let kind = ServerKind::from_u8(kind)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "unknown server kind"))?;
    Ok(Some(match kind {
        ServerKind::Data => ServerFrame::Data(payload),
        ServerKind::Heartbeat => ServerFrame::Heartbeat,
        ServerKind::Exit => ServerFrame::Exit(
            Exit::from_payload(&payload)
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "bad exit payload"))?,
        ),
    }))
}

/// Read one client frame. `Ok(None)` at clean EOF (treated as `Detach`).
pub fn read_client_frame<R: Read>(r: &mut R) -> io::Result<Option<ClientFrame>> {
    let Some((kind, payload)) = read_frame(r)? else {
        return Ok(None);
    };
    let kind = ClientKind::from_u8(kind)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "unknown client kind"))?;
    Ok(Some(match kind {
        ClientKind::Input => ClientFrame::Input(payload),
        ClientKind::Resize => {
            if payload.len() != 4 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "resize payload must be 4 bytes",
                ));
            }
            ClientFrame::Resize {
                cols: u16::from_le_bytes([payload[0], payload[1]]),
                rows: u16::from_le_bytes([payload[2], payload[3]]),
            }
        }
        ClientKind::Detach => ClientFrame::Detach,
        ClientKind::Signal => {
            if payload.len() != 4 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "signal payload must be 4 bytes",
                ));
            }
            ClientFrame::Signal(i32::from_le_bytes([
                payload[0], payload[1], payload[2], payload[3],
            ]))
        }
    }))
}

/// Write the handshake line to a stream.
pub fn write_handshake<W: Write>(w: &mut W) -> io::Result<()> {
    w.write_all(HANDSHAKE)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn exit_payload_round_trips_codes_and_signals() {
        for e in [
            Exit::Code(0),
            Exit::Code(3),
            Exit::Code(-1),
            Exit::Signal(9),
        ] {
            assert_eq!(Exit::from_payload(&e.to_payload()), Some(e));
        }
        assert_eq!(Exit::from_payload(&[7, 0, 0, 0, 0]), None); // bad disposition
        assert_eq!(Exit::from_payload(&[0, 0, 0]), None); // wrong length
    }

    #[test]
    fn process_code_maps_signal_to_128_plus() {
        assert_eq!(Exit::Code(3).process_code(), 3);
        assert_eq!(Exit::Signal(9).process_code(), 137);
    }

    #[test]
    fn handshake_parses_version() {
        let mut c = Cursor::new(HANDSHAKE.to_vec());
        assert_eq!(read_handshake(&mut c).unwrap(), VERSION);
    }

    #[test]
    fn server_frames_round_trip() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&encode_server(ServerKind::Data, b"hello"));
        buf.extend_from_slice(&encode_server(ServerKind::Heartbeat, b""));
        buf.extend_from_slice(&encode_server(
            ServerKind::Exit,
            &Exit::Code(3).to_payload(),
        ));
        let mut c = Cursor::new(buf);
        assert_eq!(
            read_server_frame(&mut c).unwrap(),
            Some(ServerFrame::Data(b"hello".to_vec()))
        );
        assert_eq!(
            read_server_frame(&mut c).unwrap(),
            Some(ServerFrame::Heartbeat)
        );
        assert_eq!(
            read_server_frame(&mut c).unwrap(),
            Some(ServerFrame::Exit(Exit::Code(3)))
        );
        assert_eq!(read_server_frame(&mut c).unwrap(), None);
    }

    #[test]
    fn client_frames_round_trip() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&encode_client(ClientKind::Input, b"world\n"));
        buf.extend_from_slice(&encode_resize(80, 24));
        buf.extend_from_slice(&encode_signal(9));
        buf.extend_from_slice(&encode_client(ClientKind::Detach, b""));
        let mut c = Cursor::new(buf);
        assert_eq!(
            read_client_frame(&mut c).unwrap(),
            Some(ClientFrame::Input(b"world\n".to_vec()))
        );
        assert_eq!(
            read_client_frame(&mut c).unwrap(),
            Some(ClientFrame::Resize { cols: 80, rows: 24 })
        );
        assert_eq!(
            read_client_frame(&mut c).unwrap(),
            Some(ClientFrame::Signal(9))
        );
        assert_eq!(
            read_client_frame(&mut c).unwrap(),
            Some(ClientFrame::Detach)
        );
    }

    #[test]
    fn data_chunks_split_at_max_payload() {
        let data = vec![7u8; MAX_PAYLOAD * 2 + 10];
        let mut out = Vec::new();
        encode_data_chunks(&data, &mut out);
        let mut c = Cursor::new(out);
        let mut total = 0;
        while let Some(f) = read_server_frame(&mut c).unwrap() {
            match f {
                ServerFrame::Data(d) => {
                    assert!(d.len() <= MAX_PAYLOAD);
                    total += d.len();
                }
                _ => panic!("only data expected"),
            }
        }
        assert_eq!(total, data.len());
    }
}
