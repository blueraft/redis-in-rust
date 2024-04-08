#[derive(Debug, PartialEq, Eq)]
pub struct BulkString {
    pub length: usize,
    pub data: String,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Command {
    Echo,
}

#[derive(Debug, PartialEq, Eq)]
pub struct RedisData {
    pub command: Command,
    pub data: BulkString,
}

impl RedisData {
    pub fn parse(data: &str) -> anyhow::Result<Self> {
        let values = data.split_terminator('$').collect::<Vec<&str>>();
        dbg!(&values[1]);
        if values.len() != 3 {
            anyhow::bail!("Invalid num values")
        };
        let command = BulkString::parse(values[1])?;
        Ok(Self {
            command: Command::try_from(command.data.as_str())?,
            data: BulkString::parse(values[2])?,
        })
    }
}

impl TryFrom<&str> for Command {
    type Error = anyhow::Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value.to_lowercase().as_str() {
            "echo" => Ok(Command::Echo),
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
    fn parse_redis_data() {
        let result = RedisData::parse("*2\r\n$4\r\necho\r\n$3\r\nhey\r\n");
        assert!(result.is_ok());
        let result = result.unwrap();
        assert_eq!(result.command, Command::Echo);
        let data = BulkString {
            length: 3,
            data: "hey".into(),
        };
        assert_eq!(result.data, data);
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
}
