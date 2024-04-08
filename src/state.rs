use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::resp::{BulkString, RedisData};

#[derive(Debug, Clone)]
pub struct State {
    map: Arc<Mutex<HashMap<BulkString, BulkString>>>,
}

impl State {
    fn new() -> Self {
        Self {
            map: Arc::new(Mutex::new(HashMap::new())),
        }
    }
    pub fn handle_response(&mut self, request: &str) -> anyhow::Result<String> {
        let redis_data = RedisData::parse(request)?;
        let response = match redis_data {
            RedisData::Ping => "+PONG\r\n".to_owned(),
            RedisData::Set(key, value) => {
                let mut map = self.map.lock().unwrap();
                map.insert(key, value);
                "+OK\r\n".to_owned()
            }
            RedisData::Get(key) => {
                let map = self.map.lock().unwrap();
                match map.get(&key) {
                    Some(value) => value.decode(),
                    None => "$-1\r\n".to_owned(),
                }
            }
            RedisData::Echo(data) => data.decode(),
        };
        Ok(response)
    }
}

impl Default for State {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_get_not_found_get() {
        let mut state = State::new();
        let result = state.handle_response("*2\r\n$3\r\nget\r\n$3\r\nfoo\r\n");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "$-1\r\n".to_owned())
    }

    #[test]
    fn test_set_and_get() {
        let mut state = State::new();
        let result = state.handle_response("*3\r\n$3\r\nset\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "+OK\r\n".to_owned());
        let result = state.handle_response("*2\r\n$3\r\nget\r\n$3\r\nfoo\r\n");
        assert!(result.is_ok());
        let result = result.unwrap();
        assert_eq!(result, "$3\r\nbar\r\n")
    }
}
