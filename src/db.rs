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
use indexmap::IndexMap;
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
        match self.expiration {
            Some(expiration) => expiration <= SystemTime::now(),
            None => false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct StreamData {
    pub id: BulkString,
    pub map: IndexMap<BulkString, BulkString>,
}

#[derive(Debug)]
pub enum DataType {
    String(BulkString),
    Stream(VecDeque<StreamData>),
}

enum SequencePosition {
    Start,
    End,
}

impl DataType {
    fn xrange_split_values(
        &self,
        data: &str,
        position: SequencePosition,
    ) -> anyhow::Result<(i64, i64)> {
        let values: Vec<i64> = data
            .split_terminator('-')
            .filter_map(|x| x.parse().ok())
            .collect();
        let empty_seq_value = match position {
            SequencePosition::Start => 0,
            SequencePosition::End => i64::MAX,
        };
        match values.len() {
            1 => Ok((values[0], empty_seq_value)),
            2 => Ok((values[0], values[1])),
            _ => anyhow::bail!("Invalid x_range value: {data}"),
        }
    }

    fn within_time_bounds(&self, value: &StreamData, start: (i64, i64), end: (i64, i64)) -> bool {
        let (time, seq) = value
            .get_time_and_seq_num(&value.id.data, None)
            .expect("Invalid Id set for {value:?}");

        (start.0 <= time && time <= end.0) && (start.1 <= seq && seq <= end.1)
    }

    fn within_exclusive_time_bounds(
        &self,
        value: &StreamData,
        start: (i64, i64),
        end: (i64, i64),
    ) -> bool {
        let (time, seq) = value
            .get_time_and_seq_num(&value.id.data, None)
            .expect("Invalid Id set for {value:?}");

        ((start.0 < time && time <= end.0) && (start.1 <= seq && seq <= end.1))
            || ((start.0 <= time && time <= end.0) && (start.1 < seq && seq <= end.1))
    }

    fn stream_value_to_resp(&self, value: &StreamData) -> String {
        let mut map_values = Vec::with_capacity(value.map.len());
        for (k, v) in value.map.clone() {
            map_values.push(k.decode());
            map_values.push(v.decode());
        }
        format!(
            "*2\r\n{}*{}\r\n{}",
            value.id.decode(),
            map_values.len(),
            map_values.join("")
        )
    }

    fn xrange(&self, start: &BulkString, end: &BulkString) -> anyhow::Result<String> {
        let stream_data = match self {
            DataType::String(_) => anyhow::bail!("Only use for stream data"),
            DataType::Stream(val) => val,
        };
        let start = match start.data.as_str() {
            "-" => (0, 0),
            data => self.xrange_split_values(data, SequencePosition::Start)?,
        };
        let end = match end.data.as_str() {
            "+" => (i64::MAX, i64::MAX),
            data => self.xrange_split_values(data, SequencePosition::End)?,
        };

        let result: Vec<String> = stream_data
            .iter()
            .filter(|x| self.within_time_bounds(x, start, end))
            .map(|x| self.stream_value_to_resp(x))
            .collect();
        let resp_value = format!("*{}\r\n{}", result.len(), result.join(""));
        Ok(resp_value)
    }

    fn xread(&self, key: &BulkString, start: &BulkString) -> anyhow::Result<String> {
        let stream_data = match self {
            DataType::String(_) => anyhow::bail!("Only use for stream data"),
            DataType::Stream(val) => val,
        };

        let start = match start.data.as_str() {
            "-" => (0, 0),
            data => self.xrange_split_values(data, SequencePosition::Start)?,
        };

        let end = (i64::MAX, i64::MAX);

        let result: Vec<String> = stream_data
            .iter()
            .filter(|x| self.within_exclusive_time_bounds(x, start, end))
            .map(|x| self.stream_value_to_resp(x))
            .collect();

        if result.is_empty() {
            anyhow::bail!("No response")
        }

        let resp_value = format!(
            "*2\r\n{}*{}\r\n{}",
            key.decode(),
            result.len(),
            result.join("")
        );
        Ok(resp_value)
    }

    fn max_entry_timestamp(&self) -> anyhow::Result<BulkString> {
        let stream_data = match self {
            DataType::String(_) => anyhow::bail!("Only use for stream data"),
            DataType::Stream(val) => val,
        };

        let res = match stream_data.back() {
            Some(v) => v.id.clone(),
            None => BulkString::encode("0-0"),
        };

        Ok(res)
    }
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
    fn valid_entry_id(&mut self, other: Option<&StreamData>) -> Result<bool, EntryIdError> {
        let data_contains_star = self.id.data.contains('*');
        let (ms_time, seq_num) = self.get_time_and_seq_num(&self.id.data, other)?;
        if data_contains_star {
            // the seq number returned by get_time_and_seq_num would be updated to reflect a new
            // seq
            self.id = BulkString::encode(&format!("{ms_time}-{seq_num}"));
        }
        if ms_time <= 0 && seq_num <= 0 {
            return Err(EntryIdError::InvalidStartError);
        }

        let res = match other {
            Some(other) => {
                let (other_ms_time, other_seq_num) =
                    self.get_time_and_seq_num(&other.id.data, None)?;
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

    fn get_time_and_seq_num(
        &self,
        data: &str,
        other: Option<&StreamData>,
    ) -> Result<(i64, i64), EntryIdError> {
        let (time, num) = data.split_once('-').expect("invalid explicit entry id");
        let time: i64 = time.parse().map_err(|_| EntryIdError::ParsingError)?;
        let num = match num {
            "*" => match other {
                Some(other) => {
                    let (other_time, other_num) = other
                        .id
                        .data
                        .split_once('-')
                        .expect("invalid explicit entry id");
                    let other_num: i64 =
                        other_num.parse().map_err(|_| EntryIdError::ParsingError)?;
                    let other_time: i64 =
                        other_time.parse().map_err(|_| EntryIdError::ParsingError)?;
                    if other_time == time {
                        other_num + 1
                    } else {
                        0
                    }
                }
                None => 1,
            },
            n => n.parse().map_err(|_| EntryIdError::ParsingError)?,
        };
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
    ) -> Result<BulkString, EntryIdError> {
        let stored_value = self.value_map.get_mut(&key);
        let val = match value {
            InputData::String(val) => {
                self.value_map
                    .insert(key.clone(), DataType::String(val.clone()));
                val
            }
            InputData::Stream(mut val) => match stored_value {
                Some(stored_steam_values) => match stored_steam_values {
                    DataType::Stream(stored_steam_values) => {
                        let last_value = stored_steam_values.back();
                        val.valid_entry_id(last_value)?;
                        let id = val.id.clone();
                        stored_steam_values.push_back(val);
                        id
                    }
                    DataType::String(_) => panic!("previous stored value was a string"),
                },
                None => {
                    let mut v = VecDeque::new();
                    let key_id = val.id.data.replace('*', "1");
                    val.id = BulkString::encode(&key_id);
                    let id = val.id.clone();
                    v.push_back(val);
                    self.value_map.insert(key.clone(), DataType::Stream(v));
                    id
                }
            },
        };
        if let Some(config) = config {
            self.expiry_map.insert(key.clone(), config);
        };

        Ok(val)
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

    pub fn xrange(
        &self,
        key: &BulkString,
        start: &BulkString,
        end: &BulkString,
    ) -> anyhow::Result<String> {
        match self.value_map.get(key) {
            Some(value) => value.xrange(start, end),
            None => anyhow::bail!("Stream value not set"),
        }
    }

    pub fn swap_and_fetch_max_id(
        &self,
        key_id_pairs: &[(BulkString, BulkString)],
    ) -> Vec<(BulkString, BulkString)> {
        key_id_pairs
            .iter()
            .flat_map(|(key, start)| {
                dbg!(&start.data);
                let start = if start.data.as_str() == "$" {
                    match self.value_map.get(key) {
                        Some(v) => v
                            .max_entry_timestamp()
                            .expect("Failed to find last entry time stamp"),
                        None => BulkString::encode("0-0"),
                    }
                } else {
                    start.to_owned()
                };
                dbg!(&key, &start);
                Some((key.clone(), start))
            })
            .collect()
    }

    pub fn xread(&self, key_id_pairs: &[(BulkString, BulkString)]) -> String {
        let resp_values: Vec<String> = key_id_pairs
            .iter()
            .flat_map(|(key, start)| {
                self.value_map
                    .get(key)
                    .and_then(|v| v.xread(key, start).ok())
            })
            .collect();

        match resp_values.len() {
            0 => "$-1\r\n".to_string(),
            n => format!("*{n}\r\n{}", resp_values.join("")),
        }
    }
}
