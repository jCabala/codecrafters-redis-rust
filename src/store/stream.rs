//! A stream value: an append-only, ordered sequence of entries.

use super::stream_id::{invalid_id_error, StreamId, StreamIdSpec};
use crate::resp::RespMessage;

struct StreamEntry {
    id: StreamId,
    fields: Vec<(String, String)>,
}

/// A Redis stream: entries are appended in order via `XADD` and never
/// removed. Entry IDs must always strictly increase.
#[derive(Default)]
pub(super) struct Stream {
    entries: Vec<StreamEntry>,
}

impl Stream {
    pub(super) fn new() -> Self {
        Self::default()
    }

    /// Validates and appends an entry with the given `id` spec (explicit,
    /// `ms-*`, or `*`) and `fields`, returning the resolved id. The
    /// resolved id must be strictly greater than the last entry's id (or
    /// `0-0` if the stream is empty so far), and can never be `0-0` itself.
    pub(super) fn xadd(
        &mut self,
        id: &str,
        fields: Vec<(String, String)>,
    ) -> Result<String, RespMessage> {
        let spec = StreamIdSpec::parse(id).ok_or_else(invalid_id_error)?;

        let last_id = self.entries.last().map_or(StreamId::ZERO, |entry| entry.id);
        let id = spec.resolve(last_id);

        if id == StreamId::ZERO {
            return Err(RespMessage::Error(
                "ERR The ID specified in XADD must be greater than 0-0".to_string(),
            ));
        }
        if id <= last_id {
            return Err(RespMessage::Error(
                "ERR The ID specified in XADD is equal or smaller than the target stream top item"
                    .to_string(),
            ));
        }

        self.entries.push(StreamEntry { id, fields });
        Ok(id.to_string())
    }

    /// Returns the entries with ids between `start` and `end`, inclusive.
    pub(super) fn range(&self, start: StreamId, end: StreamId) -> Vec<(String, Vec<(String, String)>)> {
        self.entries
            .iter()
            .filter(|entry| entry.id >= start && entry.id <= end)
            .map(|entry| (entry.id.to_string(), entry.fields.clone()))
            .collect()
    }
}
