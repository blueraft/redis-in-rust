use crate::resp::simple_string::SimpleString;

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct BulkString {
    pub length: usize,
    pub data: SimpleString,
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

    pub fn encode(data: &str) -> Self {
        Self {
            data: data.into(),
            length: data.len(),
        }
    }
}
