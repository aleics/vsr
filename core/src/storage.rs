use std::collections::HashMap;

use crate::replica::LogEntry;

pub trait LogStorage {
    fn read(&self, key: usize) -> Option<LogEntry>;

    fn read_all(&self) -> impl Iterator<Item = LogEntry>;

    fn write(&mut self, key: usize, value: LogEntry) -> bool;

    fn write_all(&mut self, values: impl IntoIterator<Item = (usize, LogEntry)>) -> bool {
        for (key, value) in values {
            if !self.write(key, value) {
                return false;
            }
        }
        true
    }
}

#[derive(Debug, Clone, Default)]
pub struct InMemoryStorage {
    logs: HashMap<usize, LogEntry>,
}

impl InMemoryStorage {
    pub fn new() -> Self {
        InMemoryStorage::default()
    }

    fn keys(&self) -> impl Iterator<Item = usize> {
        self.logs.keys().copied()
    }
}

impl LogStorage for InMemoryStorage {
    fn read(&self, key: usize) -> Option<LogEntry> {
        self.logs.get(&key).cloned()
    }

    fn write(&mut self, key: usize, value: LogEntry) -> bool {
        self.logs.insert(key, value);
        true
    }

    fn read_all(&self) -> impl Iterator<Item = LogEntry> {
        self.keys().flat_map(|key| self.read(key))
    }
}
