use std::time::{SystemTime, UNIX_EPOCH};

pub const LEN_FIELD_BYTES: usize = 4;
pub const CRC_FIELD_BYTES: usize = 4;
/// offset(8) + timestamp(8) + key_len(4) + value_len(4)
pub const MIN_BODY_BYTES: usize = 24;

/// On-disk layout (little-endian):
///
/// ```text
/// u32  len          length of everything after this field
/// u32  crc32        over everything after this field
/// u64  offset       absolute, monotonic
/// i64  timestamp_ms
/// i32  key_len      -1 = null key
///      key bytes
/// u32  value_len
///      value bytes
/// ```
///
/// Length-first so recovery can skip records without parsing them; CRC over the
/// whole body so nothing is trusted before it is verified.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub offset: u64,
    pub timestamp_ms: i64,
    pub key: Option<Vec<u8>>,
    pub value: Vec<u8>,
}

#[derive(Debug)]
pub enum DecodeOutcome {
    /// A complete, CRC-valid record; `consumed` bytes were used from the input.
    Record { record: Record, consumed: usize },
    /// Not enough bytes for a complete record: a torn tail, or simply the end
    /// of the bytes read so far.
    Incomplete,
    Corrupt(String),
}

impl Record {
    pub fn now_ms() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0)
    }

    pub fn encoded_len(&self) -> usize {
        let key_len = self.key.as_ref().map_or(0, |k| k.len());
        LEN_FIELD_BYTES + CRC_FIELD_BYTES + MIN_BODY_BYTES + key_len + self.value.len()
    }

    pub fn encode(&self, out: &mut Vec<u8>) {
        let key_len = self.key.as_ref().map_or(0, |k| k.len());
        let body_len = MIN_BODY_BYTES + key_len + self.value.len();
        let len = (CRC_FIELD_BYTES + body_len) as u32;
        out.reserve(LEN_FIELD_BYTES + len as usize);
        out.extend_from_slice(&len.to_le_bytes());
        let crc_pos = out.len();
        out.extend_from_slice(&[0u8; CRC_FIELD_BYTES]);
        let body_start = out.len();
        out.extend_from_slice(&self.offset.to_le_bytes());
        out.extend_from_slice(&self.timestamp_ms.to_le_bytes());
        match &self.key {
            None => out.extend_from_slice(&(-1i32).to_le_bytes()),
            Some(k) => {
                out.extend_from_slice(&(k.len() as i32).to_le_bytes());
                out.extend_from_slice(k);
            }
        }
        out.extend_from_slice(&(self.value.len() as u32).to_le_bytes());
        out.extend_from_slice(&self.value);
        let crc = crc32fast::hash(&out[body_start..]);
        out[crc_pos..crc_pos + CRC_FIELD_BYTES].copy_from_slice(&crc.to_le_bytes());
    }

    pub fn decode(buf: &[u8], max_record_bytes: u32) -> DecodeOutcome {
        if buf.len() < LEN_FIELD_BYTES {
            return DecodeOutcome::Incomplete;
        }
        let len = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        if (len as usize) < CRC_FIELD_BYTES + MIN_BODY_BYTES {
            return DecodeOutcome::Corrupt(format!("implausibly small record length {len}"));
        }
        if len > max_record_bytes {
            return DecodeOutcome::Corrupt(format!(
                "record length {len} exceeds configured max {max_record_bytes}"
            ));
        }
        let total = LEN_FIELD_BYTES + len as usize;
        if buf.len() < total {
            return DecodeOutcome::Incomplete;
        }
        let crc_stored = u32::from_le_bytes(buf[4..8].try_into().unwrap());
        let body = &buf[8..total];
        let crc_actual = crc32fast::hash(body);
        if crc_stored != crc_actual {
            return DecodeOutcome::Corrupt(format!(
                "crc mismatch: stored {crc_stored:#010x}, computed {crc_actual:#010x}"
            ));
        }
        let offset = u64::from_le_bytes(body[0..8].try_into().unwrap());
        let timestamp_ms = i64::from_le_bytes(body[8..16].try_into().unwrap());
        let key_len = i32::from_le_bytes(body[16..20].try_into().unwrap());
        let mut pos = 20usize;
        let key = if key_len < 0 {
            None
        } else {
            let kl = key_len as usize;
            if body.len() < pos + kl + 4 {
                return DecodeOutcome::Corrupt(format!("key length {kl} overruns record"));
            }
            let k = body[pos..pos + kl].to_vec();
            pos += kl;
            Some(k)
        };
        if body.len() < pos + 4 {
            return DecodeOutcome::Corrupt("missing value length".into());
        }
        let value_len = u32::from_le_bytes(body[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        if body.len() != pos + value_len {
            return DecodeOutcome::Corrupt(format!(
                "value length {value_len} does not match record length"
            ));
        }
        let value = body[pos..].to_vec();
        DecodeOutcome::Record {
            record: Record {
                offset,
                timestamp_ms,
                key,
                value,
            },
            consumed: total,
        }
    }
}
