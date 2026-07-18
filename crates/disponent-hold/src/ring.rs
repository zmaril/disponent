//! The bounded raw byte ring — the holder's scrollback.
//!
//! A byte cap, not a line cap (shpool spools *rendered* lines; a byte ring is
//! the exact-frame analogue — design §11 open-question 2, default 256 KiB). On
//! attach the holder replays the ring as `Data` frames so a late client sees the
//! recent output. This is the M0 replay tier; a vt100 screen repaint for humans
//! is deferred to M3.

use std::collections::VecDeque;

/// A fixed-capacity FIFO of bytes: pushing past `cap` drops the oldest bytes.
pub struct Ring {
    buf: VecDeque<u8>,
    cap: usize,
}

impl Ring {
    /// A ring holding at most `cap` bytes (`cap` of 0 is treated as 1 to keep
    /// the invariant simple — it just never retains anything useful).
    pub fn new(cap: usize) -> Ring {
        Ring {
            buf: VecDeque::new(),
            cap: cap.max(1),
        }
    }

    /// Append bytes, evicting the oldest to stay within `cap`. A single write
    /// larger than `cap` keeps only its trailing `cap` bytes.
    pub fn push(&mut self, data: &[u8]) {
        if data.len() >= self.cap {
            // Only the tail can survive; replace wholesale.
            self.buf.clear();
            self.buf.extend(&data[data.len() - self.cap..]);
            return;
        }
        self.buf.extend(data);
        while self.buf.len() > self.cap {
            self.buf.pop_front();
        }
    }

    /// A contiguous snapshot of the current contents, oldest first.
    pub fn snapshot(&self) -> Vec<u8> {
        self.buf.iter().copied().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retains_within_cap() {
        let mut r = Ring::new(8);
        r.push(b"abc");
        r.push(b"def");
        assert_eq!(r.snapshot(), b"abcdef");
        assert_eq!(r.snapshot().len(), 6);
    }

    #[test]
    fn evicts_oldest_past_cap() {
        let mut r = Ring::new(4);
        r.push(b"abcdef");
        assert_eq!(r.snapshot(), b"cdef");
    }

    #[test]
    fn oversized_write_keeps_tail() {
        let mut r = Ring::new(3);
        r.push(b"0123456789");
        assert_eq!(r.snapshot(), b"789");
        assert_eq!(r.snapshot().len(), 3);
    }

    #[test]
    fn incremental_then_overflow() {
        let mut r = Ring::new(5);
        r.push(b"ab");
        r.push(b"cd");
        r.push(b"ef");
        assert_eq!(r.snapshot(), b"bcdef");
    }
}
