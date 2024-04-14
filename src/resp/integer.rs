#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct Integer {
    pub data: usize,
}

impl Integer {
    pub fn parse(value: &str) -> anyhow::Result<Self> {
        let data: usize = value.parse()?;
        Ok(Integer { data })
    }

    pub fn decode(&self) -> String {
        format!(":{}\r\n", self.data)
    }
}
