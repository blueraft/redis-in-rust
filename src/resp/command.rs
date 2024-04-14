#[derive(Debug, PartialEq, Eq)]
pub enum Command {
    Echo,
    Ping,
    Set,
    Get,
    Info,
    Replconf,
    Psync,
}

impl TryFrom<&str> for Command {
    type Error = anyhow::Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value.to_lowercase().as_str() {
            "echo" => Ok(Command::Echo),
            "ping" => Ok(Command::Ping),
            "set" => Ok(Command::Set),
            "get" => Ok(Command::Get),
            "info" => Ok(Command::Info),
            "replconf" => Ok(Command::Replconf),
            "psync" => Ok(Command::Psync),
            _ => Err(anyhow::anyhow!("Invalid command {value}")),
        }
    }
}