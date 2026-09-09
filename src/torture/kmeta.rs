//! A minimal hand-rolled Kafka `Metadata` (v4) client over one TCP connection.
//!
//! Why not librdkafka for this: a librdkafka client learns every broker from
//! its first metadata answer and then sends later metadata requests to
//! whichever broker it happens to be connected to. During an `isolate` that is
//! frequently the zombie itself, which still answers — with itself as leader.
//! A poller built on it reports "leadership never moved" while the other two
//! brokers elected long ago. Asking one named broker over a plain socket is
//! the only way to know *that broker's* opinion, and the majority of those
//! opinions is the cluster's.
//!
//! Wire format (big-endian, Kafka framing): `[i32 len][i16 api_key=3]
//! [i16 version=4][i32 corr][nullable string client_id][i32 n][string topic]...
//! [i8 allow_auto_create]`; the response carries brokers, cluster id,
//! controller id, and per topic the partitions with leader / replicas / isr.

use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[derive(Clone, Debug, Default)]
pub struct PartitionView {
    pub partition: i32,
    pub error_code: i16,
    pub leader: i32,
    pub replicas: Vec<i32>,
    pub isr: Vec<i32>,
}

#[derive(Clone, Debug, Default)]
pub struct MetadataView {
    pub brokers: Vec<(i32, String, i32)>,
    pub controller_id: i32,
    pub topic_error: i16,
    pub partitions: Vec<PartitionView>,
}

fn put_string(out: &mut Vec<u8>, s: &str) {
    out.extend_from_slice(&(s.len() as i16).to_be_bytes());
    out.extend_from_slice(s.as_bytes());
}

struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], String> {
        if self.pos + n > self.buf.len() {
            return Err("metadata response truncated".into());
        }
        let s = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }
    fn i8(&mut self) -> Result<i8, String> {
        Ok(self.take(1)?[0] as i8)
    }
    fn i16(&mut self) -> Result<i16, String> {
        Ok(i16::from_be_bytes(self.take(2)?.try_into().unwrap()))
    }
    fn i32(&mut self) -> Result<i32, String> {
        Ok(i32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn string(&mut self) -> Result<String, String> {
        let n = self.i16()?;
        if n < 0 {
            return Ok(String::new());
        }
        Ok(String::from_utf8_lossy(self.take(n as usize)?).to_string())
    }
    fn i32_array(&mut self) -> Result<Vec<i32>, String> {
        let n = self.i32()?;
        let mut v = Vec::with_capacity(n.max(0) as usize);
        for _ in 0..n.max(0) {
            v.push(self.i32()?);
        }
        Ok(v)
    }
}

/// One Metadata v4 round trip to exactly `addr`.
pub async fn fetch(addr: &str, topic: &str, timeout: Duration) -> Result<MetadataView, String> {
    let io = async {
        let mut s = TcpStream::connect(addr).await.map_err(|e| format!("connect {addr}: {e}"))?;
        let mut body = Vec::new();
        body.extend_from_slice(&3i16.to_be_bytes());
        body.extend_from_slice(&4i16.to_be_bytes());
        body.extend_from_slice(&7i32.to_be_bytes());
        put_string(&mut body, "replog-faults");
        body.extend_from_slice(&1i32.to_be_bytes());
        put_string(&mut body, topic);
        body.push(0);
        let mut frame = (body.len() as i32).to_be_bytes().to_vec();
        frame.extend_from_slice(&body);
        s.write_all(&frame).await.map_err(|e| e.to_string())?;
        let mut len = [0u8; 4];
        s.read_exact(&mut len).await.map_err(|e| format!("read {addr}: {e}"))?;
        let len = i32::from_be_bytes(len);
        if !(0..=(16 << 20)).contains(&len) {
            return Err(format!("bad metadata frame length {len}"));
        }
        let mut buf = vec![0u8; len as usize];
        s.read_exact(&mut buf).await.map_err(|e| format!("read {addr}: {e}"))?;
        let mut r = Reader { buf: &buf, pos: 0 };
        let _corr = r.i32()?;
        let _throttle = r.i32()?;
        let n = r.i32()?;
        let mut brokers = Vec::new();
        for _ in 0..n.max(0) {
            let id = r.i32()?;
            let host = r.string()?;
            let port = r.i32()?;
            let _rack = r.string()?;
            brokers.push((id, host, port));
        }
        let _cluster_id = r.string()?;
        let controller_id = r.i32()?;
        let n = r.i32()?;
        let mut view = MetadataView {
            brokers,
            controller_id,
            topic_error: 0,
            partitions: Vec::new(),
        };
        for _ in 0..n.max(0) {
            let error_code = r.i16()?;
            let name = r.string()?;
            let _internal = r.i8()?;
            let pn = r.i32()?;
            let mut parts = Vec::new();
            for _ in 0..pn.max(0) {
                let error_code = r.i16()?;
                let partition = r.i32()?;
                let leader = r.i32()?;
                let replicas = r.i32_array()?;
                let isr = r.i32_array()?;
                parts.push(PartitionView {
                    partition,
                    error_code,
                    leader,
                    replicas,
                    isr,
                });
            }
            if name == topic {
                view.topic_error = error_code;
                view.partitions = parts;
            }
        }
        Ok(view)
    };
    tokio::time::timeout(timeout, io)
        .await
        .map_err(|_| format!("metadata {addr}: timeout"))?
}
