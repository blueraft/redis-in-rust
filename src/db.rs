use std::{
    collections::HashMap,
    fs::File,
    path::Path,
    time::{Duration, SystemTime},
};

use crate::{
    config::DatabaseConfig,
    resp::{bulk_string::BulkString, rdb::Rdb},
};

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
    Stream(StreamData),
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

    pub fn set(&mut self, key: BulkString, value: DataType, config: Option<SetConfig>) {
        self.value_map.insert(key.clone(), value);
        if let Some(config) = config {
            self.expiry_map.insert(key, config);
        }
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
