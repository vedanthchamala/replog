//! Little-endian wire primitives. Strings are u16-length-prefixed UTF-8;
//! byte blobs are u32-length-prefixed; optional blobs use i32 with -1 = null
//! (the same null convention as the on-disk record format).

use super::{ProtoError, Result};

pub fn put_u8(out: &mut Vec<u8>, v: u8) {
    out.push(v);
}

pub fn put_u16(out: &mut Vec<u8>, v: u16) {
    out.extend_from_slice(&v.to_le_bytes());
}

pub fn put_u32(out: &mut Vec<u8>, v: u32) {
    out.extend_from_slice(&v.to_le_bytes());
}

pub fn put_u64(out: &mut Vec<u8>, v: u64) {
    out.extend_from_slice(&v.to_le_bytes());
}

pub fn put_i32(out: &mut Vec<u8>, v: i32) {
    out.extend_from_slice(&v.to_le_bytes());
}

pub fn put_i64(out: &mut Vec<u8>, v: i64) {
    out.extend_from_slice(&v.to_le_bytes());
}

pub fn put_str(out: &mut Vec<u8>, s: &str) {
    debug_assert!(s.len() <= u16::MAX as usize);
    put_u16(out, s.len() as u16);
    out.extend_from_slice(s.as_bytes());
}

pub fn put_bytes(out: &mut Vec<u8>, b: &[u8]) {
    put_u32(out, b.len() as u32);
    out.extend_from_slice(b);
}

pub fn put_opt_bytes(out: &mut Vec<u8>, b: Option<&[u8]>) {
    match b {
        None => out.extend_from_slice(&(-1i32).to_le_bytes()),
        Some(b) => {
            out.extend_from_slice(&(b.len() as i32).to_le_bytes());
            out.extend_from_slice(b);
        }
    }
}

pub struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.buf.len() - self.pos < n {
            return Err(ProtoError::Truncated);
        }
        let s = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }

    pub fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    pub fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().unwrap()))
    }

    pub fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }

    pub fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }

    pub fn i32(&mut self) -> Result<i32> {
        Ok(i32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }

    pub fn i64(&mut self) -> Result<i64> {
        Ok(i64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }

    pub fn string(&mut self) -> Result<String> {
        let len = self.u16()? as usize;
        let bytes = self.take(len)?;
        String::from_utf8(bytes.to_vec())
            .map_err(|_| ProtoError::Malformed("invalid utf-8 in string".into()))
    }

    pub fn bytes(&mut self) -> Result<Vec<u8>> {
        let len = self.u32()? as usize;
        Ok(self.take(len)?.to_vec())
    }

    pub fn opt_bytes(&mut self) -> Result<Option<Vec<u8>>> {
        let len = self.i32()?;
        if len < 0 {
            return Ok(None);
        }
        Ok(Some(self.take(len as usize)?.to_vec()))
    }

    pub fn expect_end(&self) -> Result<()> {
        if self.pos != self.buf.len() {
            return Err(ProtoError::Malformed(format!(
                "{} trailing bytes after message body",
                self.buf.len() - self.pos
            )));
        }
        Ok(())
    }
}
