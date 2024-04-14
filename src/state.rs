use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
};

use bytes::BytesMut;

use crate::resp::{bulk_string::BulkString, rdb::Rdb, InfoArg, RedisData, SetConfig};

#[derive(Debug)]
struct HashValue {
    value: BulkString,
    config: SetConfig,
}

#[derive(Debug)]
pub struct ReplicaConfig {
    replid: String,
    repl_offset: AtomicUsize,
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
                    self.replid,
                    self.repl_offset.load(Ordering::Relaxed)
                )
            }
            Role::Master => {
                format!(
                    "# Replication\r\nrole:master\r\nmaster_replid:{}\r\nmaster_repl_offset:{}\r\n",
                    self.replid,
                    self.repl_offset.load(Ordering::Relaxed)
                )
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct State {
    map: Arc<Mutex<HashMap<BulkString, HashValue>>>,
    replica_config: Arc<Mutex<ReplicaConfig>>,
}

impl State {
    pub fn new(replicaof: bool, _master_config: Option<MasterConfig>) -> Self {
        let replica_config = ReplicaConfig {
            replid: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_owned(),
            repl_offset: AtomicUsize::new(0),
            role: match replicaof {
                true => Role::Slave,
                false => Role::Master,
            },
        };

        Self {
            map: Arc::new(Mutex::new(HashMap::new())),
            replica_config: Arc::new(Mutex::new(replica_config)),
        }
    }

    pub fn increment_offset(&self, increment: usize) {
        self.replica_config
            .lock()
            .unwrap()
            .repl_offset
            .fetch_add(increment, Ordering::Acquire);
    }

    pub fn handle_response(&mut self, redis_data: &RedisData) -> anyhow::Result<String> {
        let response = match redis_data {
            RedisData::Ping => "+PONG\r\n".to_owned(),
            RedisData::Info(info_arg) => match info_arg {
                InfoArg::All => {
                    let mut infos = Vec::new();
                    infos.push(self.replica_config.lock().unwrap().generate_response());
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
                    BulkString::encode(&self.replica_config.lock().unwrap().generate_response())
                        .decode()
                }
            },
            RedisData::Set(key, value, config) => {
                let mut map = self.map.lock().unwrap();
                map.insert(
                    key.to_owned(),
                    HashValue {
                        value: value.to_owned(),
                        config: config.to_owned(),
                    },
                );
                "+OK\r\n".to_owned()
            }
            RedisData::Psync(repl_id, _repl_offset) => match repl_id.data.as_str() {
                "?" => {
                    let resp = format!(
                        "+FULLRESYNC {} 0\r\n",
                        self.replica_config.lock().unwrap().replid
                    );
                    resp.to_owned()
                }
                _ => anyhow::bail!("not supported"),
            },

            RedisData::Get(key) => {
                let mut map = self.map.lock().unwrap();
                match map.get(key) {
                    Some(hash_value) => match hash_value.config.has_expired() {
                        true => {
                            map.remove(key);
                            "$-1\r\n".to_owned()
                        }
                        false => hash_value.value.decode(),
                    },
                    None => "$-1\r\n".to_owned(),
                }
            }
            RedisData::Echo(data) => data.decode(),
            RedisData::ReplConf(cmd, _arg) => match cmd.data.to_lowercase().as_str() {
                "listening-port" => "+OK\r\n".to_owned(),
                "capa" => "+OK\r\n".to_owned(),
                "getack" => format!(
                    "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n{}",
                    BulkString::encode(
                        &self
                            .replica_config
                            .lock()
                            .unwrap()
                            .repl_offset
                            .load(Ordering::Relaxed)
                            .to_string()
                    )
                    .decode()
                ),
                cmd => anyhow::bail!("invalid cmd {cmd}"),
            },
        };
        Ok(response)
    }

    pub fn replica_request(&self) -> anyhow::Result<BytesMut> {
        let hex_file = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2".to_string();
        let rdb_file = Rdb {
            length: hex_file.len(),
            hex_content: hex_file,
        };
        rdb_file.decode()
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
        let redis_data = RedisData::parse("*2\r\n$3\r\nget\r\n$3\r\nfoo\r\n").unwrap();
        let result = state.handle_response(&redis_data);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "$-1\r\n".to_owned())
    }

    #[test]
    fn test_set_and_get() {
        let mut state = State::default();
        let redis_data = RedisData::parse("*3\r\n$3\r\nset\r\n$3\r\nfoo\r\n$3\r\nbar\r\n").unwrap();
        let result = state.handle_response(&redis_data);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "+OK\r\n".to_owned());
        let redis_data = RedisData::parse("*2\r\n$3\r\nget\r\n$3\r\nfoo\r\n").unwrap();
        let result = state.handle_response(&redis_data);
        assert!(result.is_ok());
        let result = result.unwrap();
        assert_eq!(result, "$3\r\nbar\r\n")
    }
}
