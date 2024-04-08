use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::resp::{BulkString, InfoArg, RedisData, SetConfig};

#[derive(Debug)]
struct HashValue {
    value: BulkString,
    config: SetConfig,
}

#[derive(Debug, Clone)]
pub struct State {
    map: Arc<Mutex<HashMap<BulkString, HashValue>>>,
    replicaof: bool,
}

impl State {
    pub fn new(replicaof: bool) -> Self {
        Self {
            map: Arc::new(Mutex::new(HashMap::new())),
            replicaof,
        }
    }
    pub fn handle_response(&mut self, request: &str) -> anyhow::Result<String> {
        let redis_data = RedisData::parse(request)?;
        let response = match redis_data {
            RedisData::Ping => "+PONG\r\n".to_owned(),
            RedisData::Info(info_arg) => {
                let mut infos = Vec::new();
                let replication_info = match self.replicaof {
                    true => "slave",
                    false => "master",
                };
                let replication_info = format!("role:{replication_info}");
                match info_arg {
                    InfoArg::All => {
                        let replication_info = BulkString::encode(&replication_info).decode();
                        infos.push(replication_info);
                    }
                    InfoArg::Replication => {
                        let replication_info = BulkString::encode(&replication_info).decode();
                        infos.push(replication_info);
                    }
                };
                infos.join("\r\n")
            }
            RedisData::Set(key, value, config) => {
                let mut map = self.map.lock().unwrap();
                map.insert(key, HashValue { value, config });
                "+OK\r\n".to_owned()
            }
            RedisData::Get(key) => {
                let mut map = self.map.lock().unwrap();
                match map.get(&key) {
                    Some(hash_value) => match hash_value.config.has_expired() {
                        true => {
                            map.remove(&key);
                            "$-1\r\n".to_owned()
                        }
                        false => hash_value.value.decode(),
                    },
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
        Self::new(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_get_not_found_get() {
        let mut state = State::default();
        let result = state.handle_response("*2\r\n$3\r\nget\r\n$3\r\nfoo\r\n");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "$-1\r\n".to_owned())
    }

    #[test]
    fn test_set_and_get() {
        let mut state = State::default();
        let result = state.handle_response("*3\r\n$3\r\nset\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "+OK\r\n".to_owned());
        let result = state.handle_response("*2\r\n$3\r\nget\r\n$3\r\nfoo\r\n");
        assert!(result.is_ok());
        let result = result.unwrap();
        assert_eq!(result, "$3\r\nbar\r\n")
    }
}
