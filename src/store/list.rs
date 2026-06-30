//! A list value with blocking pop support.

use std::collections::VecDeque;
use std::sync::Mutex;
use std::time::Duration;
use tokio::sync::oneshot;

/// A list value with its own internal locking, so a client blocked in
/// `pop_blocking` never has to hold the outer `Keyspace` lock while waiting.
///
/// Every push appends/prepends to `items` first (so its reported length is
/// always accurate, matching real Redis even when waiters are present), then
/// drains matched waiter/item pairs from the front. After every push or pop
/// completes, `waiters` is only ever non-empty while `items` is empty.
pub(super) struct BlockingList {
    state: Mutex<ListState>,
}

struct ListState {
    items: Vec<String>,
    waiters: VecDeque<oneshot::Sender<String>>,
}

impl BlockingList {
    pub(super) fn new() -> Self {
        BlockingList {
            state: Mutex::new(ListState {
                items: Vec::new(),
                waiters: VecDeque::new(),
            }),
        }
    }

    /// Appends `values` to the back of the list and returns its length
    /// immediately afterward, then wakes any waiters with the front
    /// elements they're owed.
    pub(super) fn push_back(&self, values: Vec<String>) -> usize {
        let mut state = self.state.lock().unwrap();
        state.items.extend(values);
        let len = state.items.len();
        Self::wake_waiters(&mut state);
        len
    }

    /// Prepends `values` to the front of the list, one at a time (so the
    /// last value ends up first), and returns its length immediately
    /// afterward, then wakes any waiters with the front elements they're
    /// owed.
    pub(super) fn push_front(&self, values: Vec<String>) -> usize {
        let mut state = self.state.lock().unwrap();
        let mut new_items: Vec<String> = values.into_iter().rev().collect();
        new_items.append(&mut state.items);
        state.items = new_items;

        let len = state.items.len();
        Self::wake_waiters(&mut state);
        len
    }

    /// Hands front elements to waiters one at a time (oldest waiter first)
    /// until either runs out. A waiter whose receiver has already been
    /// dropped is skipped and its would-be element is left in place for the
    /// next waiter.
    fn wake_waiters(state: &mut ListState) {
        while !state.waiters.is_empty() && !state.items.is_empty() {
            let sender = state.waiters.pop_front().unwrap();
            let value = state.items.remove(0);
            if let Err(value) = sender.send(value) {
                state.items.insert(0, value);
            }
        }
    }

    /// Removes and returns up to `count` elements from the front of the
    /// list, without blocking.
    pub(super) fn pop_front(&self, count: usize) -> Vec<String> {
        let mut state = self.state.lock().unwrap();
        let count = count.min(state.items.len());
        state.items.drain(..count).collect()
    }

    /// Removes and returns the front element, waiting for one to become
    /// available if the list is currently empty. Waits forever if `timeout`
    /// is `None`, otherwise returns `None` once `timeout` elapses.
    pub(super) async fn pop_blocking(&self, timeout: Option<Duration>) -> Option<String> {
        let receiver = {
            let mut state = self.state.lock().unwrap();
            if !state.items.is_empty() {
                return Some(state.items.remove(0));
            }
            let (sender, receiver) = oneshot::channel();
            state.waiters.push_back(sender);
            receiver
        };

        match timeout {
            Some(duration) => tokio::time::timeout(duration, receiver).await.ok()?.ok(),
            None => receiver.await.ok(),
        }
    }

    pub(super) fn len(&self) -> usize {
        self.state.lock().unwrap().items.len()
    }

    /// Wakes any blocked waiters with no value, causing their `BLPOP` to
    /// resolve as if it had timed out. Used when the key is about to be
    /// overwritten with a non-list value out from under them.
    pub(super) fn cancel_waiters(&self) {
        self.state.lock().unwrap().waiters.clear();
    }

    /// True once the list has no elements and nobody is waiting on it,
    /// meaning its keyspace entry can be safely removed.
    pub(super) fn is_idle(&self) -> bool {
        let state = self.state.lock().unwrap();
        state.items.is_empty() && state.waiters.is_empty()
    }

    /// Returns the elements between `start` and `stop` (inclusive,
    /// zero-based, negative indexes count from the end), using the same
    /// out-of-range clamping rules as Redis's `LRANGE`.
    pub(super) fn range(&self, start: i64, stop: i64) -> Vec<String> {
        let state = self.state.lock().unwrap();
        let items = &state.items;

        let len = items.len() as i64;
        let start = if start < 0 { (len + start).max(0) } else { start };
        let mut stop: i64 = if stop < 0 { len + stop } else { stop };

        if start > stop || start >= len {
            return Vec::new();
        }
        if stop >= len {
            stop = len - 1;
        }

        items[start as usize..=stop as usize].to_vec()
    }
}
