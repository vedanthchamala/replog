//! Docker fault backend: the same three levers for every containerized broker.
//!
//! Shells out to the `docker` CLI rather than speaking the Engine API — the
//! harness runs on the host next to the daemon, and the CLI is the one
//! interface every target's compose file already depends on. Each broker
//! container sits on a shared `peers` network plus its own private edge
//! network that publishes its client port to the host, so `isolate` (leave
//! the peers network) severs it from the cluster while the host-side
//! workload still reaches it.

use tokio::process::Command;

use super::cluster::Fault;

#[derive(Clone, Debug)]
pub struct Docker {
    pub containers: Vec<String>,
    pub peers_network: String,
}

impl Docker {
    pub fn new(containers: Vec<String>, peers_network: impl Into<String>) -> Docker {
        Docker {
            containers,
            peers_network: peers_network.into(),
        }
    }

    pub async fn run(args: &[&str]) -> Result<String, String> {
        let out = Command::new("docker")
            .args(args)
            .output()
            .await
            .map_err(|e| format!("docker {}: {e}", args.join(" ")))?;
        if !out.status.success() {
            return Err(format!(
                "docker {} failed: {}",
                args.join(" "),
                String::from_utf8_lossy(&out.stderr).trim()
            ));
        }
        Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
    }

    pub async fn apply(&self, i: usize, f: Fault) -> Result<(), String> {
        let c = &self.containers[i];
        match f {
            Fault::Kill => Self::run(&["kill", "-s", "KILL", c]).await?,
            Fault::Pause => Self::run(&["pause", c]).await?,
            Fault::Isolate => Self::run(&["network", "disconnect", &self.peers_network, c]).await?,
        };
        Ok(())
    }

    pub async fn heal(&self, i: usize, f: Fault) -> Result<(), String> {
        let c = &self.containers[i];
        match f {
            Fault::Kill => Self::run(&["start", c]).await?,
            Fault::Pause => Self::run(&["unpause", c]).await?,
            Fault::Isolate => Self::run(&["network", "connect", &self.peers_network, c]).await?,
        };
        Ok(())
    }

    async fn inspect(&self, i: usize, format: &str) -> Result<String, String> {
        Self::run(&["inspect", "-f", format, &self.containers[i]]).await
    }

    pub async fn is_running(&self, i: usize) -> Result<bool, String> {
        Ok(self.inspect(i, "{{.State.Running}}").await? == "true")
    }

    pub async fn is_paused(&self, i: usize) -> Result<bool, String> {
        Ok(self.inspect(i, "{{.State.Paused}}").await? == "true")
    }

    pub async fn on_peers_network(&self, i: usize) -> Result<bool, String> {
        let nets = self.inspect(i, "{{range $k, $v := .NetworkSettings.Networks}}{{$k}} {{end}}").await?;
        Ok(nets.split_whitespace().any(|n| n == self.peers_network))
    }
}
