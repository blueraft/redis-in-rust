#[derive(Debug, PartialEq, Eq)]
pub struct BulkString {
    pub length: usize,
    pub data: String,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Command {
    Echo,
    Ping,
}

#[derive(Debug, PartialEq, Eq)]
pub struct RedisData {
    pub command: Command,
    pub data: Option<BulkString>,
}

impl RedisData {
    pub fn parse(data: &str) -> anyhow::Result<Self> {
        dbg!(&data);
        let values = data.split_terminator('$').collect::<Vec<&str>>();
        if values.len() == 3 {
            let command = BulkString::parse(values[1])?;
            Ok(Self {
                command: Command::try_from(command.data.as_str())?,
                data: Some(BulkString::parse(values[2])?),
            })
        } else if values.len() == 2 {
            let command = BulkString::parse(values[1])?;
            Ok(Self {
                command: Command::try_from(command.data.as_str())?,
                data: None,
            })
        } else {
            anyhow::bail!("Invalid number of values for redis-data")
        }
    }
}

impl TryFrom<&str> for Command {
    type Error = anyhow::Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value.to_lowercase().as_str() {
            "echo" => Ok(Command::Echo),
            "ping" => Ok(Command::Ping),
            _ => Err(anyhow::anyhow!("Invalid command {value}")),
        }
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
        format!("${}\r\n{}\r\n", self.length, self.data)
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
        assert_eq!(result.command, Command::Echo);
        let data = BulkString {
            length: 3,
            data: "hey".into(),
        };
        assert_eq!(result.data.unwrap(), data);
    }

    #[test]
    fn parse_ping_redis_data() {
        let result = RedisData::parse("*1\r\n$4\r\nping\r\n");
        assert!(result.is_ok());
        let result = result.unwrap();
        assert_eq!(result.command, Command::Ping);
        assert!(result.data.is_none());
    }

    #[test]
    fn parse_hey() {
        let result = BulkString::parse("3\r\nhey\r\n");
        assert!(result.is_ok());
        let result = result.unwrap();
        assert_eq!(result.data, "hey");
        assert_eq!(result.length, 3);
    }

    #[test]
    fn parse_longer_string() {
        let result = BulkString::parse("17\r\nheyhellohowareyou\r\n");
        assert!(result.is_ok());
        let result = result.unwrap();
        assert_eq!(result.data, "heyhellohowareyou");
        assert_eq!(result.length, 17);
    }

    #[test]
    fn parse_empty_string() {
        let result = BulkString::parse("0\r\n\r\n");
        assert!(result.is_ok());
        let result = result.unwrap();
        assert_eq!(result.data, "");
        assert_eq!(result.length, 0);
    }

    #[test]
    fn decode_bulk_string() {
        let input_string = "3\r\nhey\r\n";
        let result = BulkString::parse(input_string).unwrap();
        assert_eq!(result.data, "hey");
        assert_eq!(result.length, 3);
        assert_eq!(result.decode(), format!("${input_string}"));
    }
}
