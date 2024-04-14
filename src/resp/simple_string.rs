use std::ops::Deref;

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct SimpleString(String);

impl Deref for SimpleString {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<&str> for SimpleString {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

impl SimpleString {
    pub(crate) fn decode(&self) -> String {
        format!("{}\r\n", self.0)
    }
}
