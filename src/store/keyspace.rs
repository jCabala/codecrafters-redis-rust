//! The keyspace: a map of keys to typed, expiring values.

use super::list::BlockingList;
use crate::resp::RespMessage;
use rand::seq::IteratorRandom;
use std::collections::HashMap;
use std::collections::hash_map::{Entry as MapEntry, VacantEntry};
use std::sync::Arc;
use std::time::Instant;

/// Number of keys to randomly sample on each expiry sweep, mirroring Redis's
/// active expiry cycle (rather than scanning the whole keyspace).
const EXPIRY_SAMPLE_SIZE: usize = 20;

pub(super) fn wrong_type_error() -> RespMessage {
    RespMessage::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string())
}

/// A key's value, typed per Redis's data model (a key holds exactly one type
/// at a time).
pub(super) enum Value {
    String(String),
    List(Arc<BlockingList>),
}

pub(super) struct Entry {
    pub(super) value: Value,
    pub(super) expires_at: Option<Instant>,
}

impl Entry {
    fn is_expired(&self) -> bool {
        self.expires_at.is_some_and(|t| Instant::now() >= t)
    }
}

/// Creates a brand-new, empty list for a key that didn't exist yet, inserts
/// it, and returns the shared handle to it.
pub(super) fn create_list(vacant: VacantEntry<'_, String, Entry>) -> Arc<BlockingList> {
    let list = Arc::new(BlockingList::new());
    vacant.insert(Entry {
        value: Value::List(Arc::clone(&list)),
        expires_at: None,
    });
    list
}

/// A `HashMap` of keys to entries that transparently evicts an entry the
/// moment it's found to be expired, so callers never have to remember to
/// check expiry themselves before reading or writing a key.
#[derive(Default)]
pub(super) struct Keyspace(HashMap<String, Entry>);

impl Keyspace {
    pub(super) fn get(&mut self, key: &str) -> Option<&Entry> {
        self.evict_if_expired(key);
        self.0.get(key)
    }

    pub(super) fn entry(&mut self, key: String) -> MapEntry<'_, String, Entry> {
        self.evict_if_expired(&key);
        self.0.entry(key)
    }

    pub(super) fn insert(&mut self, key: String, entry: Entry) {
        self.0.insert(key, entry);
    }

    fn keys(&self) -> impl Iterator<Item = &String> {
        self.0.keys()
    }

    fn is_expired(&self, key: &str) -> bool {
        self.0.get(key).is_some_and(|entry| entry.is_expired())
    }

    fn remove(&mut self, key: &str) {
        self.0.remove(key);
    }

    fn evict_if_expired(&mut self, key: &str) {
        if self.is_expired(key) {
            self.0.remove(key);
        }
    }

    /// Removes any expired keys among a random sample, so that keys with a
    /// TTL that are never accessed again don't linger in memory forever.
    pub(super) fn remove_expired(&mut self) {
        let expired: Vec<String> = self
            .keys()
            .choose_multiple(&mut rand::thread_rng(), EXPIRY_SAMPLE_SIZE)
            .into_iter()
            .filter(|key| self.is_expired(key))
            .cloned()
            .collect();

        for key in expired {
            self.remove(&key);
        }
    }
}
