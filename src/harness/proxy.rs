//! A cuttable TCP proxy for injecting network partitions.
//!
//! The torture harness puts one of these between each broker and the
//! controller: `cut()` severs the link (live connections are killed, new ones
//! are accepted and immediately dropped), `heal()` restores it. Cutting a
//! broker's controller link makes the controller declare it dead and re-elect
//! while the broker itself keeps serving clients — the zombie-leader
//! scenario, which is the sharpest live test of epoch fencing.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use tokio::net::{TcpListener, TcpStream};

pub struct TcpProxy {
    pub addr: String,
    enabled: Arc<AtomicBool>,
    /// Bumped on every cut; shuttles exit when their generation goes stale.
    generation: Arc<AtomicU64>,
    accept_task: tokio::task::JoinHandle<()>,
}

impl Drop for TcpProxy {
    fn drop(&mut self) {
        self.accept_task.abort();
    }
}

impl TcpProxy {
    pub async fn start(target: impl Into<String>) -> std::io::Result<TcpProxy> {
        let target = target.into();
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?.to_string();
        let enabled = Arc::new(AtomicBool::new(true));
        let generation = Arc::new(AtomicU64::new(0));
        let accept_task = {
            let enabled = enabled.clone();
            let generation = generation.clone();
            tokio::spawn(async move {
                loop {
                    let Ok((inbound, _)) = listener.accept().await else {
                        return;
                    };
                    if !enabled.load(Ordering::SeqCst) {
                        drop(inbound); // refuse by instant close
                        continue;
                    }
                    let my_gen = generation.load(Ordering::SeqCst);
                    let generation = generation.clone();
                    let target = target.clone();
                    tokio::spawn(shuttle(inbound, target, generation, my_gen));
                }
            })
        };
        Ok(TcpProxy {
            addr,
            enabled,
            generation,
            accept_task,
        })
    }

    /// Severs the link: kills live connections, refuses new ones.
    pub fn cut(&self) {
        self.enabled.store(false, Ordering::SeqCst);
        self.generation.fetch_add(1, Ordering::SeqCst);
    }

    pub fn heal(&self) {
        self.enabled.store(true, Ordering::SeqCst);
    }

    pub fn is_up(&self) -> bool {
        self.enabled.load(Ordering::SeqCst)
    }
}

async fn shuttle(
    mut inbound: TcpStream,
    target: String,
    generation: Arc<AtomicU64>,
    my_gen: u64,
) {
    let Ok(mut outbound) = TcpStream::connect(&target).await else {
        return;
    };
    let stale = async {
        loop {
            tokio::time::sleep(Duration::from_millis(50)).await;
            if generation.load(Ordering::SeqCst) != my_gen {
                return;
            }
        }
    };
    tokio::select! {
        _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound) => {}
        _ = stale => {} // cut: drop both halves, killing the connection
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// Echo server + proxy: bytes flow while up; cut kills the live
    /// connection and refuses new ones; heal restores service.
    #[tokio::test]
    async fn cut_kills_and_refuses_heal_restores() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let target = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            loop {
                let Ok((mut s, _)) = listener.accept().await else { return };
                tokio::spawn(async move {
                    let mut buf = [0u8; 256];
                    while let Ok(n) = s.read(&mut buf).await {
                        if n == 0 || s.write_all(&buf[..n]).await.is_err() {
                            return;
                        }
                    }
                });
            }
        });

        let proxy = TcpProxy::start(&target).await.unwrap();
        let mut conn = TcpStream::connect(&proxy.addr).await.unwrap();
        conn.write_all(b"ping").await.unwrap();
        let mut buf = [0u8; 4];
        conn.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"ping");

        proxy.cut();
        // The live connection dies within the watch interval: reads reach
        // EOF/error once in-flight bytes (racing the cut) have drained.
        let mut buf = [0u8; 4];
        let dead = tokio::time::timeout(Duration::from_secs(2), async {
            conn.write_all(b"pong").await.ok();
            loop {
                match conn.read(&mut buf).await {
                    Ok(0) | Err(_) => return true,
                    Ok(_) => {} // bytes already through the proxy when cut hit
                }
            }
        })
        .await;
        assert!(matches!(dead, Ok(true)), "live connection must die on cut");
        // New connections are accepted-and-dropped: reads see instant EOF.
        let mut conn2 = TcpStream::connect(&proxy.addr).await.unwrap();
        let n = tokio::time::timeout(Duration::from_secs(2), conn2.read(&mut buf))
            .await
            .expect("refusal must be prompt")
            .unwrap_or(0);
        assert_eq!(n, 0, "cut proxy must not carry data");

        proxy.heal();
        let mut conn3 = TcpStream::connect(&proxy.addr).await.unwrap();
        conn3.write_all(b"back").await.unwrap();
        let mut buf = [0u8; 4];
        conn3.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"back");
    }
}
