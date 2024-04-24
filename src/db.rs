use std::{
    collections::{HashMap, VecDeque},
    fs::File,
    path::Path,
    time::{Duration, SystemTime},
};

use crate::{
    config::DatabaseConfig,
    resp::{bulk_string::BulkString, rdb::Rdb},
};
use thiserror::Error;

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct SetConfig {
    expiration: Option<SystemTime>,
}

impl SetConfig {
    pub fn new(px: Option<&BulkString>, time: Option<&BulkString>) -> anyhow::Result<Self> {
        let mut config = SetConfig { expiration: None };
        if let (Some(px), Some(time)) = (px, time) {
            if px.data.to_lowercase().as_str() == "px" {
                let expiry_duration: u64 = time.data.parse()?;
                config.expiration =
                    Some(SystemTime::now() + Duration::from_millis(expiry_duration));
            }
        };
        Ok(config)
    }

    pub fn from_expiration(expiration: SystemTime) -> Self {
        Self {
            expiration: Some(expiration),
        }
    }

    pub fn has_expired(&self) -> bool {
        println!("checking for expiry, {:?}", self,);
        match self.expiration {
            Some(expiration) => expiration <= SystemTime::now(),
            None => false,
        }
    }
}

#[derive(Debug)]
pub struct StreamData {
    pub id: BulkString,
    pub map: HashMap<BulkString, BulkString>,
}

#[derive(Debug)]
pub enum DataType {
    String(BulkString),
    Stream(VecDeque<StreamData>),
}

#[derive(Debug)]
pub enum InputData {
    String(BulkString),
    Stream(StreamData),
}

#[derive(Error, Debug)]
pub enum EntryIdError {
    #[error(
        "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"
    )]
    NotAscendingError,
    #[error("-ERR The ID specified in XADD must be greater than 0-0\r\n")]
    InvalidStartError,
    #[error("Parsing stream data")]
    ParsingError,
}

impl StreamData {
    pub fn valid_entry_id(&self, other: Option<&StreamData>) -> Result<bool, EntryIdError> {
        let (ms_time, seq_num) = self.get_time_and_seq_num(&self.id.data)?;
        if ms_time <= 0 && seq_num <= 0 {
            return Err(EntryIdError::InvalidStartError);
        }

        let res = match other {
            Some(other) => {
                let (other_ms_time, other_seq_num) = self.get_time_and_seq_num(&other.id.data)?;
                if ms_time > other_ms_time || (ms_time == other_ms_time && seq_num > other_seq_num)
                {
                    true
                } else {
                    return Err(EntryIdError::NotAscendingError);
                }
            }
            None => true,
        };
        Ok(res)
    }

    fn get_time_and_seq_num(&self, data: &str) -> Result<(i32, i32), EntryIdError> {
        let (time, num) = data.split_once('-').expect("invalid explicit entry id");
        let time: i32 = time.parse().map_err(|_| EntryIdError::ParsingError)?;
        let num: i32 = num.parse().map_err(|_| EntryIdError::ParsingError)?;
        Ok((time, num))
    }
}

#[derive(Debug)]
pub struct Database {
    config: Option<DatabaseConfig>,
    value_map: HashMap<BulkString, DataType>,
    expiry_map: HashMap<BulkString, SetConfig>,
}

impl Database {
    pub fn initialize(config: Option<DatabaseConfig>) -> Self {
        let mut value_map = HashMap::new();
        let mut expiry_map = HashMap::new();
        let Some(config) = config else {
            return Self {
                config,
                value_map,
                expiry_map,
            };
        };

        let path = Path::new(&config.dir).join(&config.dbfilename);
        if let Ok(f) = File::open(&path) {
            let mut rdb = Rdb::new(f);
            rdb.read_rdb_to_map(&mut value_map, &mut expiry_map)
                .expect("Failed to read rdb dump");
        } else {
            println!("The {path:?} file was not found");
        }

        Self {
            config: Some(config),
            value_map,
            expiry_map,
        }
    }

    pub fn set(
        &mut self,
        key: BulkString,
        value: InputData,
        config: Option<SetConfig>,
    ) -> Result<(), EntryIdError> {
        let stored_value = self.value_map.get_mut(&key);
        match value {
            InputData::String(val) => {
                self.value_map.insert(key.clone(), DataType::String(val));
            }
            InputData::Stream(val) => match stored_value {
                Some(stored_steam_values) => match stored_steam_values {
                    DataType::Stream(stored_steam_values) => {
                        let last_value = stored_steam_values.back();
                        val.valid_entry_id(last_value)?;
                        stored_steam_values.push_back(val);
                    }
                    DataType::String(_) => panic!("previous stored value was a string"),
                },
                None => {
                    let mut v = VecDeque::new();
                    v.push_back(val);
                    self.value_map.insert(key.clone(), DataType::Stream(v));
                }
            },
        };
        if let Some(config) = config {
            self.expiry_map.insert(key.clone(), config);
        };
        Ok(())
    }

    pub fn keys(&self) -> String {
        let keys: Vec<String> = self.value_map.keys().map(|x| x.decode()).collect();
        format!("*{}\r\n{}\r\n", keys.len(), keys.join(""))
    }

    pub fn get(&mut self, key: &BulkString) -> String {
        match (self.expiry_map.get(key), self.value_map.get(key)) {
            (Some(config), Some(value)) => match config.has_expired() {
                true => {
                    println!("{key:?} value expired {config:?}");
                    self.value_map.remove(key);
                    self.expiry_map.remove(key);
                    "$-1\r\n".to_owned()
                }
                false => match value {
                    DataType::String(v) => v.decode(),
                    DataType::Stream(_) => unimplemented!(),
                },
            },
            (None, Some(value)) => match value {
                DataType::String(v) => v.decode(),
                DataType::Stream(_) => unimplemented!(),
            },
            (_config, _value) => {
                dbg!(_config, _value);
                "$-1\r\n".to_owned()
            }
        }
    }

    pub fn ty(&mut self, key: &BulkString) -> String {
        match (self.expiry_map.get(key), self.value_map.get(key)) {
            (Some(config), Some(value)) => match config.has_expired() {
                true => {
                    println!("{key:?} value expired {config:?}");
                    self.value_map.remove(key);
                    self.expiry_map.remove(key);
                    "+none\r\n".to_owned()
                }
                false => match value {
                    DataType::String(_) => "+string\r\n".to_owned(),
                    DataType::Stream(_) => "+stream\r\n".to_owned(),
                },
            },
            (None, Some(value)) => match value {
                DataType::String(_) => "+string\r\n".to_owned(),
                DataType::Stream(_) => "+stream\r\n".to_owned(),
            },

            (_config, _value) => {
                dbg!(_config, _value);
                "+none\r\n".to_owned()
            }
        }
    }

    pub fn dir(&self) -> anyhow::Result<String> {
        match &self.config {
            Some(config) => {
                let dir = format!(
                    "*2\r\n$3\r\ndir\r\n${}\r\n{}\r\n",
                    config.dir.len(),
                    config.dir
                );
                Ok(dir)
            }
            None => anyhow::bail!("No db config available"),
        }
    }

    pub fn dbfilename(&self) -> anyhow::Result<String> {
        match &self.config {
            Some(config) => {
                let dbfilename = format!(
                    "*2\r\n$10\r\ndbfilename\r\n${}\r\n{}\r\n",
                    config.dbfilename.len(),
                    config.dbfilename
                );
                Ok(dbfilename)
            }
            None => anyhow::bail!("No db config available"),
        }
    }
}
