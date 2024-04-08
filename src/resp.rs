use std::ops::Deref;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct BulkString {
    pub length: usize,
    pub data: SimpleString,
}

#[derive(Debug, PartialEq, Eq, Hash)]
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
pub enum RedisData {
    Get(BulkString),
    Echo(BulkString),
    Set(BulkString, BulkString),
    Ping,
}

impl RedisData {
    pub fn parse(data: &str) -> anyhow::Result<Self> {
        dbg!(&data);
        let values = data.split_terminator('$').collect::<Vec<&str>>();
        let command = BulkString::parse(values[1])?;
        let command = Command::try_from(command.data.as_str())?;
        let redis_data = match command {
            Command::Echo if values.len() == 3 => Self::Echo(BulkString::parse(values[2])?),
            Command::Get if values.len() == 3 => Self::Get(BulkString::parse(values[2])?),
            Command::Ping => Self::Ping,
            Command::Set if values.len() == 4 => {
                Self::Set(BulkString::parse(values[2])?, BulkString::parse(values[3])?)
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
