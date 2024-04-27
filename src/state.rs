use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use bytes::BytesMut;
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use tokio::{sync::broadcast::Receiver, time::timeout};

use crate::{
    db::{Database, InputData, StreamData},
    resp::{bulk_string::BulkString, InfoArg, RedisData},
};

#[derive(Debug)]
pub struct ReplicaConfig {
    replid: String,
    repl_offset: AtomicUsize,
    num_replicas: AtomicUsize,
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
    replica_config: Arc<Mutex<ReplicaConfig>>,
    db: Arc<Mutex<Database>>,
}

impl State {
    pub fn new(replicaof: bool, db: Database) -> Self {
        let replica_config = ReplicaConfig {
            replid: thread_rng()
                .sample_iter(&Alphanumeric)
                .map(char::from)
                .take(40)
                .collect(),
            repl_offset: AtomicUsize::new(0),
            num_replicas: AtomicUsize::new(0),
            role: match replicaof {
                true => Role::Slave,
                false => Role::Master,
            },
        };

        Self {
            replica_config: Arc::new(Mutex::new(replica_config)),
            db: Arc::new(Mutex::new(db)),
        }
    }

    pub fn increment_offset(&self, increment: usize) {
        self.replica_config
            .lock()
            .unwrap()
            .repl_offset
            .fetch_add(increment, Ordering::Acquire);
    }

    pub fn increment_num_replicas(&self) {
        self.replica_config
            .lock()
            .unwrap()
            .num_replicas
            .fetch_add(1, Ordering::Acquire);
    }

    pub fn replica_count(&self) -> usize {
        self.replica_config
            .lock()
            .unwrap()
            .num_replicas
            .load(Ordering::Relaxed)
    }

    pub fn offset(&self) -> usize {
        self.replica_config
            .lock()
            .unwrap()
            .repl_offset
            .load(Ordering::Relaxed)
    }

    pub async fn count_synced_replicas(
        &self,
        target_num_replicas: BulkString,
        timeout_duration: BulkString,
        mut rx: Receiver<(usize, SocketAddr)>,
    ) -> anyhow::Result<usize> {
        let connected_replicas = self.replica_count();
        let primary_offset = self.offset();
        println!("connected replicas: {connected_replicas}");
        println!("primary offset: {primary_offset}");
        if primary_offset == 0 {
            return Ok(connected_replicas);
        }
        let target_num_replicas: usize = target_num_replicas.data.parse()?;
        let target_count = target_num_replicas.min(connected_replicas);
        let timeout_duration = {
            let timeout_duration: u64 = timeout_duration.data.parse()?;
            Duration::from_millis(timeout_duration)
        };
        let mut synced_replicas = 0;
        while let Ok(res) = timeout(timeout_duration, rx.recv()).await {
            match res {
                Ok((offset, socket_addr)) => {
                    println!("replica_offset: {offset} from {socket_addr}");
                    if offset >= primary_offset {
                        synced_replicas += 1;
                    }
                    if synced_replicas >= target_count {
                        break;
                    }
                }
                Err(e) => {
                    eprintln!("error getting offset from replica due to {e:?}")
                }
            }
        }
        Ok(synced_replicas)
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
                self.db.lock().unwrap().set(
                    key.to_owned(),
                    InputData::String(value.to_owned()),
                    Some(config.to_owned()),
                )?;
                "+OK\r\n".to_owned()
            }

            RedisData::Xadd(key, id, map) => {
                let result = self.db.lock().unwrap().set(
                    key.to_owned(),
                    InputData::Stream(StreamData {
                        id: id.to_owned(),
                        map: map.to_owned(),
                    }),
                    None,
                );
                match result {
                    Ok(id) => id.decode(),
                    Err(e) => e.to_string(),
                }
            }
            RedisData::Xrange(key, start, end) => {
                match self.db.lock().unwrap().xrange(key, start, end) {
                    Ok(res) => res,
                    Err(e) => panic!("Failed to retrieve values due to {e}"),
                }
            }
            RedisData::Xread(_streams, key, start) => {
                match self.db.lock().unwrap().xread(key, start) {
                    Ok(res) => res,
                    Err(e) => panic!("Failed to retrieve values due to {e}"),
                }
            }

            RedisData::Keys(_value) => self.db.lock().unwrap().keys(),
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
            RedisData::Wait(_, _) => {
                let num_replica = self
                    .replica_config
                    .lock()
                    .unwrap()
                    .num_replicas
                    .load(Ordering::Relaxed);
                format!(":{}\r\n", num_replica)
            }
            RedisData::Get(key) => self.db.lock().unwrap().get(key),
            RedisData::Type(key) => self.db.lock().unwrap().ty(key),
            RedisData::Echo(data) => data.decode(),
            RedisData::Config(cmd, arg) => match cmd.data.to_lowercase().as_str() {
                "get" => match arg.data.to_lowercase().as_str() {
                    "dir" => self.db.lock().unwrap().dir()?,
                    "dbfilename" => self.db.lock().unwrap().dbfilename()?,
                    arg => anyhow::bail!("invalid cmd {arg}"),
                },
                cmd => anyhow::bail!("invalid cmd {cmd}"),
            },
            RedisData::ReplConf(cmd, _arg) => match cmd.data.to_lowercase().as_str() {
                "listening-port" => "+OK\r\n".to_owned(),
                "capa" => "+OK\r\n".to_owned(),
                "ack" => "".to_owned(),
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
        let hex_bytes = hex::decode(hex_file.clone())?;
        let msg = format!("${}\r\n", hex_bytes.len());
        let mut response = BytesMut::from(msg.as_bytes());
        response.extend(hex_bytes);
        Ok(response)
    }
}

impl Default for State {
    fn default() -> Self {
        let db = Database::initialize(None);
        Self::new(false, db)
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
