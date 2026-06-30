//! Shared in-memory key-value store.

mod keyspace;
mod list;
mod stream;
mod stream_id;

use crate::resp::RespMessage;
use keyspace::{create_list, wrong_type_error, Entry, Keyspace, Value};
use std::collections::hash_map::Entry as MapEntry;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use stream::Stream;

/// How often to run the active expiry sweep.
const EXPIRY_SWEEP_INTERVAL: Duration = Duration::from_millis(1000);

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
            Some(Entry { value: Value::List(_) | Value::Stream(_), .. }) => Err(wrong_type_error()),
            None => Ok(None),
        }
    }

    /// Returns the Redis type name of the value at `key`, or `"none"` if it
    /// doesn't exist.
    pub fn key_type(&self, key: &str) -> &'static str {
        let mut data = self.data.lock().unwrap();
        match data.get(key) {
            Some(Entry { value: Value::String(_), .. }) => "string",
            Some(Entry { value: Value::List(_), .. }) => "list",
            Some(Entry { value: Value::Stream(_), .. }) => "stream",
            None => "none",
        }
    }

    pub fn set(&self, key: String, value: String, ttl: Option<Duration>) {
        let expires_at = ttl.map(|d| Instant::now() + d);
        let mut data = self.data.lock().unwrap();

        if let Some(Entry { value: Value::List(list), .. }) = data.get(&key) {
            list.cancel_waiters();
        }

        data.insert(
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
            MapEntry::Occupied(occupied) => match &occupied.get().value {
                Value::List(list) => Ok(list.push_back(values)),
                Value::String(_) | Value::Stream(_) => Err(wrong_type_error()),
            },
            MapEntry::Vacant(vacant) => Ok(create_list(vacant).push_back(values)),
        }
    }

    /// Prepends `values` to the list at `key`, one at a time (so the last
    /// value in `values` ends up first), creating the list if it doesn't
    /// exist, and returns the list's length after prepending.
    pub fn lpush(&self, key: String, values: Vec<String>) -> Result<usize, RespMessage> {
        let mut data = self.data.lock().unwrap();

        match data.entry(key) {
            MapEntry::Occupied(occupied) => match &occupied.get().value {
                Value::List(list) => Ok(list.push_front(values)),
                Value::String(_) | Value::Stream(_) => Err(wrong_type_error()),
            },
            MapEntry::Vacant(vacant) => Ok(create_list(vacant).push_front(values)),
        }
    }

    /// Returns the elements of the list at `key` between `start` and `stop`
    /// (inclusive, zero-based, negative indexes count from the end), using
    /// the same out-of-range clamping rules as Redis's `LRANGE`.
    pub fn lrange(&self, key: &str, start: i64, stop: i64) -> Result<Vec<String>, RespMessage> {
        let mut data = self.data.lock().unwrap();
        match data.get(key) {
            Some(Entry { value: Value::List(list), .. }) => Ok(list.range(start, stop)),
            Some(Entry { value: Value::String(_) | Value::Stream(_), .. }) => Err(wrong_type_error()),
            None => Ok(Vec::new()),
        }
    }

    /// Returns the length of the list at `key`, or 0 if it doesn't exist.
    pub fn llen(&self, key: &str) -> Result<usize, RespMessage> {
        let mut data = self.data.lock().unwrap();
        match data.get(key) {
            Some(Entry { value: Value::List(list), .. }) => Ok(list.len()),
            Some(Entry { value: Value::String(_) | Value::Stream(_), .. }) => Err(wrong_type_error()),
            None => Ok(0),
        }
    }

    /// Removes and returns up to `count` elements from the front of the list
    /// at `key`, without blocking. Returns `None` if the key doesn't exist
    /// (distinct from `Some(vec![])`, which means the key exists but `count`
    /// was 0). The key is removed entirely once its list becomes idle.
    pub fn lpop(&self, key: &str, count: usize) -> Result<Option<Vec<String>>, RespMessage> {
        let mut data = self.data.lock().unwrap();

        match data.entry(key.to_string()) {
            MapEntry::Occupied(occupied) => match &occupied.get().value {
                Value::List(list) => {
                    let popped = list.pop_front(count);
                    if list.is_idle() {
                        occupied.remove();
                    }
                    Ok(Some(popped))
                }
                Value::String(_) | Value::Stream(_) => Err(wrong_type_error()),
            },
            MapEntry::Vacant(_) => Ok(None),
        }
    }

    /// Removes and returns the front element of the list at `key`, blocking
    /// until one becomes available or `timeout` elapses (waiting forever if
    /// `timeout` is `None`). Multiple clients blocked on the same key are
    /// served in the order they started waiting.
    pub async fn blpop(&self, key: String, timeout: Option<Duration>) -> Result<Option<String>, RespMessage> {
        let list = {
            let mut data = self.data.lock().unwrap();
            match data.entry(key) {
                MapEntry::Occupied(occupied) => match &occupied.get().value {
                    Value::List(list) => Arc::clone(list),
                    Value::String(_) | Value::Stream(_) => return Err(wrong_type_error()),
                },
                MapEntry::Vacant(vacant) => create_list(vacant),
            }
        };

        Ok(list.pop_blocking(timeout).await)
    }

    /// Validates and appends an entry with the given `id` and `fields` to
    /// the stream at `key`, creating it if it doesn't exist, and returns
    /// the entry's id. A key is never created if the id fails validation.
    pub fn xadd(
        &self,
        key: String,
        id: &str,
        fields: Vec<(String, String)>,
    ) -> Result<String, RespMessage> {
        let mut data = self.data.lock().unwrap();

        match data.entry(key) {
            MapEntry::Occupied(mut occupied) => match &mut occupied.get_mut().value {
                Value::Stream(stream) => stream.xadd(id, fields),
                Value::String(_) | Value::List(_) => Err(wrong_type_error()),
            },
            MapEntry::Vacant(vacant) => {
                let mut stream = Stream::new();
                let result = stream.xadd(id, fields);
                if result.is_ok() {
                    vacant.insert(Entry {
                        value: Value::Stream(stream),
                        expires_at: None,
                    });
                }
                result
            }
        }
    }
}
