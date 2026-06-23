//! Shared in-memory key-value store.

use crate::resp::RespMessage;
use rand::seq::IteratorRandom;
use std::collections::HashMap;
use std::collections::hash_map::{Entry as MapEntry, VacantEntry};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Number of keys to randomly sample on each expiry sweep, mirroring Redis's
/// active expiry cycle (rather than scanning the whole keyspace).
const EXPIRY_SAMPLE_SIZE: usize = 20;

/// How often to run the active expiry sweep.
const EXPIRY_SWEEP_INTERVAL: Duration = Duration::from_millis(1000);

/// A key's value, typed per Redis's data model (a key holds exactly one type
/// at a time).
enum Value {
    String(String),
    List(Vec<String>),
}

struct Entry {
    value: Value,
    expires_at: Option<Instant>,
}

impl Entry {
    fn is_expired(&self) -> bool {
        self.expires_at.is_some_and(|t| Instant::now() >= t)
    }
}

fn wrong_type_error() -> RespMessage {
    RespMessage::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string())
}

/// Inserts a brand-new list entry for a key that didn't exist yet, returning
/// the list's length.
fn insert_new_list(vacant: VacantEntry<'_, String, Entry>, values: Vec<String>) -> usize {
    let len = values.len();
    vacant.insert(Entry {
        value: Value::List(values),
        expires_at: None,
    });
    len
}

/// A `HashMap` of keys to entries that transparently evicts an entry the
/// moment it's found to be expired, so callers never have to remember to
/// check expiry themselves before reading or writing a key.
#[derive(Default)]
struct Keyspace(HashMap<String, Entry>);

impl Keyspace {
    fn get(&mut self, key: &str) -> Option<&Entry> {
        self.evict_if_expired(key);
        self.0.get(key)
    }

    fn entry(&mut self, key: String) -> MapEntry<'_, String, Entry> {
        self.evict_if_expired(&key);
        self.0.entry(key)
    }

    fn insert(&mut self, key: String, entry: Entry) {
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
    fn remove_expired(&mut self) {
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

#[derive(Clone, Default)]
pub struct Store {
    data: Arc<Mutex<Keyspace>>,
}

impl Store {
    pub fn new() -> Self {
        let store = Self::default();
        tokio::spawn(store.clone().run_expiry_sweeps());
        store
    }

    /// Runs forever, periodically removing expired keys in the background.
    async fn run_expiry_sweeps(self) {
        let mut interval = tokio::time::interval(EXPIRY_SWEEP_INTERVAL);
        loop {
            interval.tick().await;
            self.data.lock().unwrap().remove_expired();
        }
    }

    pub fn get(&self, key: &str) -> Result<Option<String>, RespMessage> {
        let mut data = self.data.lock().unwrap();
        match data.get(key) {
            Some(Entry { value: Value::String(s), .. }) => Ok(Some(s.clone())),
            Some(Entry { value: Value::List(_), .. }) => Err(wrong_type_error()),
            None => Ok(None),
        }
    }

    pub fn set(&self, key: String, value: String, ttl: Option<Duration>) {
        let expires_at = ttl.map(|d| Instant::now() + d);
        self.data.lock().unwrap().insert(
            key,
            Entry {
                value: Value::String(value),
                expires_at,
            },
        );
    }

    /// Appends `values` to the list at `key`, creating it if it doesn't
    /// exist, and returns the list's length after appending.
    pub fn rpush(&self, key: String, values: Vec<String>) -> Result<usize, RespMessage> {
        let mut data = self.data.lock().unwrap();

        match data.entry(key) {
            MapEntry::Occupied(mut occupied) => match &mut occupied.get_mut().value {
                Value::List(list) => {
                    list.extend(values);
                    Ok(list.len())
                }
                Value::String(_) => Err(wrong_type_error()),
            },
            MapEntry::Vacant(vacant) => Ok(insert_new_list(vacant, values)),
        }
    }

    /// Prepends `values` to the list at `key`, one at a time (so the last
    /// value in `values` ends up first), creating the list if it doesn't
    /// exist, and returns the list's length after prepending.
    pub fn lpush(&self, key: String, values: Vec<String>) -> Result<usize, RespMessage> {
        let mut data = self.data.lock().unwrap();
        let mut values = values;
        values.reverse();

        match data.entry(key) {
            MapEntry::Occupied(mut occupied) => match &mut occupied.get_mut().value {
                Value::List(list) => {
                    values.append(list);
                    *list = values;
                    Ok(list.len())
                }
                Value::String(_) => Err(wrong_type_error()),
            },
            MapEntry::Vacant(vacant) => Ok(insert_new_list(vacant, values)),
        }
    }

    /// Returns the elements of the list at `key` between `start` and `stop`
    /// (inclusive, zero-based, negative indexes count from the end), using
    /// the same out-of-range clamping rules as Redis's `LRANGE`.
    pub fn lrange(&self, key: &str, start: i64, stop: i64) -> Result<Vec<String>, RespMessage> {
        let mut data = self.data.lock().unwrap();

        let list = match data.get(key) {
            Some(Entry {
                value: Value::List(list),
                ..
            }) => list,
            Some(Entry {
                value: Value::String(_),
                ..
            }) => return Err(wrong_type_error()),
            None => return Ok(Vec::new()),
        };

        let len = list.len() as i64;
        let start = if start < 0 { (len + start).max(0) } else { start };
        let mut stop: i64 = if stop < 0 { len + stop } else { stop };

        if start > stop || start >= len {
            return Ok(Vec::new());
        }
        if stop >= len {
            stop = len - 1;
        }

        Ok(list[start as usize..=stop as usize].to_vec())
    }

    /// Returns the length of the list at `key`, or 0 if it doesn't exist.
    pub fn llen(&self, key: &str) -> Result<usize, RespMessage> {
        let mut data = self.data.lock().unwrap();
        match data.get(key) {
            Some(Entry { value: Value::List(list), .. }) => Ok(list.len()),
            Some(Entry { value: Value::String(_), .. }) => Err(wrong_type_error()),
            None => Ok(0),
        }
    }

    /// Removes and returns up to `count` elements from the front of the list
    /// at `key`. Returns `None` if the key doesn't exist (distinct from `Some(vec![])`,
    /// which means the key exists but `count` was 0). The key is removed
    /// entirely once its list becomes empty.
    pub fn lpop(&self, key: &str, count: usize) -> Result<Option<Vec<String>>, RespMessage> {
        let mut data = self.data.lock().unwrap();

        match data.entry(key.to_string()) {
            MapEntry::Occupied(mut occupied) => match &mut occupied.get_mut().value {
                Value::List(list) => {
                    let count = count.min(list.len());
                    let popped: Vec<String> = list.drain(..count).collect();
                    if list.is_empty() {
                        occupied.remove();
                    }
                    Ok(Some(popped))
                }
                Value::String(_) => Err(wrong_type_error()),
            },
            MapEntry::Vacant(_) => Ok(None),
        }
    }
}
