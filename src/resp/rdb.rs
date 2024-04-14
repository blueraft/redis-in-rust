use bytes::BytesMut;

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Rdb {
    pub length: usize,
    pub hex_content: String,
}

impl Rdb {
    pub(crate) fn decode(&self) -> anyhow::Result<BytesMut> {
        let hex_bytes = hex::decode(self.hex_content.clone())?;
        let msg = format!("${}\r\n", hex_bytes.len());
        let mut response = BytesMut::from(msg.as_bytes());
        response.extend(hex_bytes);
        Ok(response)
    }
}
