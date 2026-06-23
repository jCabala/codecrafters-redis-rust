//! Shared in-memory key-value store.

use crate::resp::RespMessage;
use rand::seq::IteratorRandom;
use std::collections::HashMap;
use std::collections::hash_map::Entry as MapEntry;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Number of keys to randomly sample on each expiry sweep, mirroring Redis's
/// active expiry cycle (rather than scanning the whole keyspace).
const EXPIRY_SAMPLE_SIZE: usize = 20;

/// How often to run the active expiry sweep.
const EXPIRY_SWEEP_INTERVAL: Duration = Duration::from_millis(100);

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

#[derive(Clone, Default)]
pub struct Store {
    data: Arc<Mutex<HashMap<String, Entry>>>,
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
            self.remove_expired();
        }
    }

    pub fn get(&self, key: &str) -> Result<Option<String>, RespMessage> {
        let mut data = self.data.lock().unwrap();
        Self::evict_if_expired(&mut data, key);
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
        Self::evict_if_expired(&mut data, &key);

        match data.entry(key) {
            MapEntry::Occupied(mut occupied) => match &mut occupied.get_mut().value {
                Value::List(list) => {
                    list.extend(values);
                    Ok(list.len())
                }
                Value::String(_) => Err(wrong_type_error()),
            },
            MapEntry::Vacant(vacant) => {
                let len = values.len();
                vacant.insert(Entry {
                    value: Value::List(values),
                    expires_at: None,
                });
                Ok(len)
            }
        }
    }

    fn evict_if_expired(data: &mut HashMap<String, Entry>, key: &str) {
        if data.get(key).is_some_and(|entry| entry.is_expired()) {
            data.remove(key);
        }
    }

    /// Removes any expired keys among a random sample, so that keys with a
    /// TTL that are never accessed again don't linger in memory forever.
    fn remove_expired(&self) {
        let mut data = self.data.lock().unwrap();

        let expired: Vec<String> = data
            .keys()
            .choose_multiple(&mut rand::thread_rng(), EXPIRY_SAMPLE_SIZE)
            .into_iter()
            .filter(|key| data[*key].is_expired())
            .cloned()
            .collect();

        for key in expired {
            data.remove(&key);
        }
    }
}
