#[derive(Debug, PartialEq, Eq)]
pub enum Command {
    Echo,
    Ping,
    Set,
    Get,
    Info,
    Replconf,
    Psync,
    Wait,
    Config,
    Keys,
    Xadd,
    Xrange,
    Xread,
    Type,
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
            "wait" => Ok(Command::Wait),
            "config" => Ok(Command::Config),
            "keys" => Ok(Command::Keys),
            "type" => Ok(Command::Type),
            "xadd" => Ok(Command::Xadd),
            "xrange" => Ok(Command::Xrange),
            "xread" => Ok(Command::Xread),
            _ => Err(anyhow::anyhow!("Invalid command {value}")),
        }
    }
}
