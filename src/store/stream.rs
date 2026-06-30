//! A stream value: an append-only, ordered sequence of entries.

#[allow(dead_code)]
struct StreamEntry {
    id: String,
    fields: Vec<(String, String)>,
}

/// A Redis stream: entries are appended in order via `XADD` and never
/// removed.
#[derive(Default)]
pub(super) struct Stream {
    entries: Vec<StreamEntry>,
}

impl Stream {
    pub(super) fn new() -> Self {
        Self::default()
    }

    /// Appends an entry with the given `id` and `fields`, returning the id.
    pub(super) fn xadd(&mut self, id: String, fields: Vec<(String, String)>) -> String {
        self.entries.push(StreamEntry {
            id: id.clone(),
            fields,
        });
        id
    }
}
