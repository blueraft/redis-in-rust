use bulk_string::BulkString;
use std::time::{Duration, Instant};

use command::Command;

pub(crate) mod bulk_string;
pub(crate) mod command;
pub(crate) mod rdb;
pub(crate) mod simple_string;

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct SetConfig {
    old_time: Instant,
    expiry_duration: Option<Duration>,
}

impl SetConfig {
    pub fn has_expired(&self) -> bool {
        match self.expiry_duration {
            Some(expiry_duration) => self.old_time.elapsed() > expiry_duration,
            None => false,
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum RedisData {
    Get(BulkString),
    Echo(BulkString),
    Set(BulkString, BulkString, SetConfig),
    Info(InfoArg),
    Ping,
    ReplConf(BulkString, BulkString),
    Psync(BulkString, BulkString),
    Wait(BulkString, BulkString),
    Config(BulkString, BulkString),
}

#[derive(Debug, PartialEq, Eq)]
pub enum InfoArg {
    All,
    Replication,
}

impl RedisData {
    pub fn parse(data: &str) -> anyhow::Result<Self> {
        let values: Vec<BulkString> = data
            .split_terminator('$')
            .skip(1)
            .filter_map(|x| BulkString::parse(x).ok())
            .collect();
        anyhow::ensure!(values.len() >= 1);
        let command = Command::try_from(values[0].data.as_str())?;
        let redis_data = match command {
            Command::Echo if values.len() == 2 => Self::Echo(values[1].clone()),
            Command::Get if values.len() == 2 => Self::Get(values[1].clone()),
            Command::Ping => Self::Ping,
            Command::Info => {
                if values.len() > 1 {
                    // TODO: filter for the argument
                }
                Self::Info(InfoArg::Replication)
            }
            Command::Replconf if values.len() >= 3 => {
                Self::ReplConf(values[1].clone(), values[2].clone())
            }
            Command::Config if values.len() >= 3 => {
                Self::Config(values[1].clone(), values[2].clone())
            }
            Command::Wait if values.len() >= 3 => Self::Wait(values[1].clone(), values[2].clone()),
            Command::Psync if values.len() >= 3 => {
                Self::Psync(values[1].clone(), values[2].clone())
            }
            Command::Set if values.len() >= 3 => {
                let mut config = SetConfig {
                    old_time: Instant::now(),
                    expiry_duration: None,
                };
                if let (Some(px), Some(time)) = (values.get(3), values.get(4)) {
                    if px.data.to_lowercase().as_str() == "px" {
                        let expiry_duration: u64 = time.data.parse()?;
                        config.expiry_duration = Some(Duration::from_millis(expiry_duration));
                    }
                };
                Self::Set(values[1].clone(), values[2].clone(), config)
            }
            _ => anyhow::bail!("incorrect {values:?} for {command:?}",),
        };
        Ok(redis_data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn parse_echo_redis_data() {
        let result = RedisData::parse("*2\r\n$4\r\necho\r\n$3\r\nhey\r\n");
        assert!(result.is_ok());
        let result = result.unwrap();
        let data = RedisData::Echo(BulkString {
            length: 3,
            data: "hey".into(),
        });
        assert_eq!(result, data);
    }

    #[test]
    fn parse_get_data() {
        let result = RedisData::parse("*2\r\n$3\r\nget\r\n$3\r\nfoo\r\n");
        assert!(result.is_ok());
        let result = result.unwrap();
        let data = RedisData::Get(BulkString {
            length: 3,
            data: "foo".into(),
        });
        assert_eq!(result, data);
    }

    #[test]
    fn parse_set_data() {
        let result = RedisData::parse("*3\r\n$3\r\nset\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
        assert!(result.is_ok());
        let result = result.unwrap();
        let data = RedisData::Set(
            BulkString {
                length: 3,
                data: "foo".into(),
            },
            BulkString {
                length: 3,
                data: "bar".into(),
            },
            SetConfig {
                old_time: Instant::now(),
                expiry_duration: None,
            },
        );
        assert_eq!(result, data);
    }

    #[test]
    fn parse_ping_redis_data() {
        let result = RedisData::parse("*1\r\n$4\r\nping\r\n");
        assert!(result.is_ok());
        let result = result.unwrap();
        assert_eq!(result, RedisData::Ping);
    }

    #[test]
    fn parse_hey() {
        let result = BulkString::parse("3\r\nhey\r\n");
        assert!(result.is_ok());
        let result = result.unwrap();
        assert_eq!(result.data, "hey".into());
        assert_eq!(result.length, 3);
    }

    #[test]
    fn parse_longer_string() {
        let result = BulkString::parse("17\r\nheyhellohowareyou\r\n");
        assert!(result.is_ok());
        let result = result.unwrap();
        assert_eq!(result.data, "heyhellohowareyou".into());
        assert_eq!(result.length, 17);
    }

    #[test]
    fn parse_empty_string() {
        let result = BulkString::parse("0\r\n\r\n");
        assert!(result.is_ok());
        let result = result.unwrap();
        assert_eq!(result.data, "".into());
        assert_eq!(result.length, 0);
    }

    #[test]
    fn decode_bulk_string() {
        let input_string = "3\r\nhey\r\n";
        let result = BulkString::parse(input_string).unwrap();
        assert_eq!(result.data, "hey".into());
        assert_eq!(result.length, 3);
        assert_eq!(result.decode(), format!("${input_string}"));
    }
}
