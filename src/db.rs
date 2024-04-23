use std::{
    collections::HashMap,
    fs::File,
    path::Path,
    time::{Duration, Instant},
};

use crate::{
    config::DatabaseConfig,
    resp::{bulk_string::BulkString, rdb::Rdb},
};

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct SetConfig {
    old_time: Instant,
    expiry_duration: Option<Duration>,
}

impl SetConfig {
    pub fn new(px: Option<&BulkString>, time: Option<&BulkString>) -> anyhow::Result<Self> {
        let mut config = SetConfig {
            old_time: Instant::now(),
            expiry_duration: None,
        };
        if let (Some(px), Some(time)) = (px, time) {
            if px.data.to_lowercase().as_str() == "px" {
                let expiry_duration: u64 = time.data.parse()?;
                config.expiry_duration = Some(Duration::from_millis(expiry_duration));
            }
        };
        Ok(config)
    }
    pub fn has_expired(&self) -> bool {
        match self.expiry_duration {
            Some(expiry_duration) => self.old_time.elapsed() > expiry_duration,
            None => false,
        }
    }
}

#[derive(Debug)]
pub(crate) struct HashValue {
    pub(crate) value: BulkString,
    pub(crate) config: SetConfig,
}

#[derive(Debug)]
pub struct Database {
    config: Option<DatabaseConfig>,
    map: HashMap<BulkString, HashValue>,
}

impl Database {
    pub fn initialize(config: Option<DatabaseConfig>) -> Self {
        let mut map = HashMap::new();
        let Some(config) = config else {
            return Self { config, map };
        };

        let path = Path::new(&config.dir).join(&config.dbfilename);
        if let Ok(f) = File::open(&path) {
            let mut rdb = Rdb::new(f);
            rdb.read_rdb_to_map(&mut map)
                .expect("Failed to read rdb dump");
        } else {
            println!("The {path:?} file was not found");
        }

        Self {
            config: Some(config),
            map,
        }
    }

    pub fn set(&mut self, key: BulkString, value: BulkString, config: SetConfig) {
        self.map.insert(key, HashValue { value, config });
    }

    pub fn get(&mut self, key: &BulkString) -> String {
        match self.map.get(key) {
            Some(hash_value) => match hash_value.config.has_expired() {
                true => {
                    self.map.remove(key);
                    "$-1\r\n".to_owned()
                }
                false => hash_value.value.decode(),
            },
            None => "$-1\r\n".to_owned(),
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
