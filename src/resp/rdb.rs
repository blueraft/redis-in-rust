// RDB reader - some amount of code here is adopted and modified from https://github.com/badboy/rdb-rs/blob/master/src/parser.rs
use std::{
    collections::HashMap,
    io::{BufReader, Read},
};

use bytes::{Buf, BytesMut};

use crate::{
    db::{HashValue, SetConfig},
    resp::bulk_string::BulkString,
};

#[derive(Debug)]
pub(crate) struct Rdb<R> {
    inner: BufReader<R>,
    buffer: BytesMut,
}
pub mod op_code {
    pub const AUX: u8 = 250;
    pub const RESIZEDB: u8 = 251;
    pub const _EXPIRETIME_MS: u8 = 252;
    pub const _EXPIRETIME: u8 = 253;
    pub const SELECTDB: u8 = 254;
    pub const EOF: u8 = 255;
}

pub mod constant {
    pub const RDB_6BITLEN: u8 = 0;
    pub const RDB_14BITLEN: u8 = 1;
    pub const RDB_ENCVAL: u8 = 3;
}

pub mod encoding {
    pub const INT8: u32 = 0;
    pub const INT16: u32 = 1;
    pub const INT32: u32 = 2;
    pub const _LZF: u32 = 3;
}

#[derive(Debug)]
enum StringEncoding {
    Int32(u32),
    StringValue(String),
}

impl From<StringEncoding> for BulkString {
    fn from(value: StringEncoding) -> Self {
        match value {
            StringEncoding::Int32(_val) => unimplemented!(),
            StringEncoding::StringValue(val) => BulkString {
                length: val.len(),
                data: val.as_str().into(),
            },
        }
    }
}

impl<R: Read> Rdb<R> {
    pub fn new(reader: R) -> Self {
        Self {
            inner: BufReader::new(reader),
            buffer: BytesMut::new(),
        }
    }

    pub fn read_rdb_to_map(
        &mut self,
        map: &mut HashMap<BulkString, HashValue>,
    ) -> anyhow::Result<()> {
        self.read_header()?;
        loop {
            let op = self.next()?;
            match op {
                op_code::AUX => {
                    let _key = self.read_blob()?;
                    let _value = self.read_blob()?;
                }
                op_code::SELECTDB => {
                    let (_length, _) = self.read_length_with_encoding()?;
                }
                op_code::RESIZEDB => {
                    let (_db_size, _) = self.read_length_with_encoding()?;
                    let (_expires_size, _) = self.read_length_with_encoding()?;
                }
                op_code::EOF => {
                    break;
                }
                _ => {
                    let key = self.read_blob()?;
                    let value = self.read_blob()?;
                    println!("Saved key {key:?} and value {value:?}");
                    map.insert(
                        key.into(),
                        HashValue {
                            value: value.into(),
                            config: SetConfig::new(None, None)?,
                        },
                    );
                }
            }
        }
        Ok(())
    }

    fn read_header(&mut self) -> anyhow::Result<()> {
        self.buffer.resize(9, 0);
        self.inner.read_exact(&mut self.buffer)?;
        anyhow::ensure!(&self.buffer[..5] == b"REDIS", "Invalid header");
        Ok(())
    }

    fn next(&mut self) -> anyhow::Result<u8> {
        self.buffer.resize(1, 0);
        self.inner.read_exact(&mut self.buffer)?;
        Ok(self.buffer[0])
    }

    fn read_blob(&mut self) -> anyhow::Result<StringEncoding> {
        let (length, is_encoded) = self.read_length_with_encoding()?;

        if is_encoded {
            let result = match length {
                encoding::INT8 => self.next()? as u32,
                encoding::INT16 => {
                    self.buffer.resize(3, 0);
                    self.inner.read_exact(&mut self.buffer)?;
                    let val = self.buffer.get_u16_le();
                    val as u32
                }
                encoding::INT32 => {
                    self.buffer.resize(4, 0);
                    self.inner.read_exact(&mut self.buffer)?;
                    self.buffer.get_u32_le()
                }
                // encoding::LZF => {
                //     let compressed_length = try!(read_length(input));
                //     let real_length = try!(read_length(input));
                //     let data = try!(read_exact(input, compressed_length as usize));
                //     lzf::decompress(&data, real_length as usize).unwrap()
                // },
                _ => {
                    panic!("Unknown encoding: {}", length)
                }
            };
            Ok(StringEncoding::Int32(result))
        } else {
            self.buffer.resize(length as usize, 0);
            self.inner.read_exact(&mut self.buffer)?;
            let value = String::from_utf8_lossy(&self.buffer).to_string();
            Ok(StringEncoding::StringValue(value))
        }
    }

    fn read_length_with_encoding(&mut self) -> anyhow::Result<(u32, bool)> {
        let length;
        let mut is_encoded = false;

        let enc_type = self.next()?;

        match (enc_type & 0xC0) >> 6 {
            constant::RDB_ENCVAL => {
                is_encoded = true;
                length = (enc_type & 0x3F) as u32;
            }
            constant::RDB_6BITLEN => {
                length = (enc_type & 0x3F) as u32;
            }
            constant::RDB_14BITLEN => {
                let next_byte = self.next()?;
                length = (((enc_type & 0x3F) as u32) << 8) | next_byte as u32;
            }
            _ => {
                length = self.next()?.to_be() as u32;
            }
        }

        Ok((length, is_encoded))
    }
}
