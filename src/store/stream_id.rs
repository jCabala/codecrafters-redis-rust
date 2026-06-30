//! A stream entry ID: `<millisecondsTime>-<sequenceNumber>`, with support
//! for the explicit, partial-auto, and fully-auto formats `XADD` accepts.

use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

/// A concrete, resolved stream entry ID. Deriving `Ord` gives exactly
/// Redis's comparison rule, since it compares `ms` first and `seq` second,
/// matching field declaration order.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct StreamId {
    ms: u64,
    seq: u64,
}

impl StreamId {
    pub(super) const ZERO: StreamId = StreamId { ms: 0, seq: 0 };

    /// Picks the next sequence number for `ms`: one past the last entry's
    /// sequence number if it shares the same `ms`, otherwise 0.
    fn next_seq(ms: u64, last_id: StreamId) -> StreamId {
        let seq = if ms == last_id.ms { last_id.seq + 1 } else { 0 };
        StreamId { ms, seq }
    }
}

impl fmt::Display for StreamId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.ms, self.seq)
    }
}

/// The three ID formats `XADD` accepts, before being resolved against the
/// stream's last entry.
pub(super) enum StreamIdSpec {
    /// `<ms>-<seq>`: both parts given explicitly.
    Explicit(StreamId),
    /// `<ms>-*`: caller picks the millisecond time, we pick the sequence.
    AutoSeq(u64),
    /// `*`: we pick both the millisecond time and the sequence.
    AutoFull,
}

impl StreamIdSpec {
    pub(super) fn parse(value: &str) -> Option<StreamIdSpec> {
        if value == "*" {
            return Some(StreamIdSpec::AutoFull);
        }

        let (ms, seq) = value.split_once('-')?;
        let ms: u64 = ms.parse().ok()?;

        if seq == "*" {
            return Some(StreamIdSpec::AutoSeq(ms));
        }

        Some(StreamIdSpec::Explicit(StreamId {
            ms,
            seq: seq.parse().ok()?,
        }))
    }

    /// Resolves this spec into a concrete `StreamId`, given the id of the
    /// stream's last entry (or `StreamId::ZERO` if it's empty).
    pub(super) fn resolve(self, last_id: StreamId) -> StreamId {
        match self {
            StreamIdSpec::Explicit(id) => id,
            StreamIdSpec::AutoSeq(ms) => StreamId::next_seq(ms, last_id),
            StreamIdSpec::AutoFull => StreamId::next_seq(current_millis(), last_id),
        }
    }
}

fn current_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
