use std::{
    ops::Deref,
    time::{Duration, Instant},
};

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct BulkString {
    pub length: usize,
    pub data: SimpleString,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct SimpleString(String);

impl Deref for SimpleString {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum Command {
    Echo,
    Ping,
    Set,
    Get,
}

#[derive(Debug, PartialEq, Eq)]
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
    Ping,
}

impl RedisData {
    pub fn parse(data: &str) -> anyhow::Result<Self> {
        let values: Vec<BulkString> = data
            .split_terminator('$')
            .skip(1)
            .filter_map(|x| BulkString::parse(x).ok())
            .collect();
        let command = Command::try_from(values[0].data.as_str())?;
        let redis_data = match command {
            Command::Echo if values.len() == 2 => Self::Echo(values[1].clone()),
            Command::Get if values.len() == 2 => Self::Get(values[1].clone()),
            Command::Ping => Self::Ping,
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
            _ => anyhow::bail!(
                "incorrect number of values {} for {command:?}",
                values.len()
            ),
        };
        Ok(redis_data)
    }
}

impl TryFrom<&str> for Command {
    type Error = anyhow::Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value.to_lowercase().as_str() {
            "echo" => Ok(Command::Echo),
            "ping" => Ok(Command::Ping),
            "set" => Ok(Command::Set),
            "get" => Ok(Command::Get),
            _ => Err(anyhow::anyhow!("Invalid command {value}")),
        }
    }
}

impl From<&str> for SimpleString {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

impl SimpleString {
    fn decode(&self) -> String {
        format!("{}\r\n", self.0)
    }
}

impl BulkString {
    pub fn parse(value: &str) -> anyhow::Result<Self> {
        let values = value.split_terminator("\r\n").collect::<Vec<&str>>();
        if values.len() != 2 {
            anyhow::bail!("Invalid num values")
        };
        let bulk_string = Self {
            length: values[0].parse()?,
            data: values[1].into(),
        };
        Ok(bulk_string)
    }

    pub fn decode(&self) -> String {
        format!("${}\r\n{}", self.length, self.data.decode())
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
