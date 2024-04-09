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
pub struct ReplicaConfig {
    replid: String,
    repl_offset: usize,
    role: Role,
}

#[derive(Debug, Clone)]
pub struct MasterConfig {
    pub host: String,
    pub port: usize,
}

#[derive(Debug, Clone)]
pub enum Role {
    Master,
    Slave,
}

impl ReplicaConfig {
    fn generate_response(&self) -> String {
        match self.role {
            Role::Slave => {
                format!(
                    "# Replication\r\nrole:slave\r\nmaster_replid:{}\r\nmaster_repl_offset:{}\r\n",
                    self.replid, self.repl_offset
                )
            }
            Role::Master => {
                format!(
                    "# Replication\r\nrole:master\r\nmaster_replid:{}\r\nmaster_repl_offset:{}\r\n",
                    self.replid, self.repl_offset
                )
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct State {
    map: Arc<Mutex<HashMap<BulkString, HashValue>>>,
    replica_config: ReplicaConfig,
}

impl State {
    pub fn new(replicaof: bool, master_config: Option<MasterConfig>) -> Self {
        let replica_config = ReplicaConfig {
            replid: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_owned(),
            repl_offset: 0,
            role: match replicaof {
                true => Role::Slave,
                false => Role::Master,
            },
        };

        Self {
            map: Arc::new(Mutex::new(HashMap::new())),
            replica_config,
        }
    }
    pub fn handle_response(&mut self, request: &str) -> anyhow::Result<String> {
        let redis_data = RedisData::parse(request)?;
        let response = match redis_data {
            RedisData::Ping => "+PONG\r\n".to_owned(),
            RedisData::Info(info_arg) => match info_arg {
                InfoArg::All => {
                    let mut infos = Vec::new();
                    infos.push(self.replica_config.generate_response());
                    format!(
                        "*{}\r\n{}",
                        infos.len(),
                        infos
                            .iter()
                            .map(|r| BulkString::encode(r).decode())
                            .collect::<Vec<String>>()
                            .join("")
                    )
                }
                InfoArg::Replication => {
                    BulkString::encode(&self.replica_config.generate_response()).decode()
                }
            },
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
        dbg!(&response);
        Ok(response)
    }
}

impl Default for State {
    fn default() -> Self {
        Self::new(false, None)
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
