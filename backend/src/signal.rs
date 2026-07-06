//! Transport du signal HFT (KILL) entre le Nœud Radar (Tokyo) et le Nœud
//! Exécuteur (Dublin).
//!
//! En dev local : `LoopbackTransport` (canal mpsc in-process) — les deux rôles
//! tournent sur la même machine. En production : `UdpSignalTransport` (datagramme
//! brut d'1 octet) qui transite via AWS Global Accelerator sans allocation.

use std::net::SocketAddr;

use async_trait::async_trait;
use tokio::net::UdpSocket;
use tokio::sync::mpsc;

use crate::types::{Signal, WireTick};

#[async_trait]
pub trait SignalTransport: Send + Sync {
    async fn send_signal(&self, signal: Signal) -> anyhow::Result<()>;
    async fn recv_signal(&self) -> anyhow::Result<Signal>;
}

// ───── Dev local : loopback in-process ─────
pub struct LoopbackTransport {
    sender: mpsc::Sender<Signal>,
    receiver: tokio::sync::Mutex<mpsc::Receiver<Signal>>,
}

impl LoopbackTransport {
    pub fn new(buffer_size: usize) -> Self {
        let (tx, rx) = mpsc::channel(buffer_size);
        Self {
            sender: tx,
            receiver: tokio::sync::Mutex::new(rx),
        }
    }
}

#[async_trait]
impl SignalTransport for LoopbackTransport {
    async fn send_signal(&self, signal: Signal) -> anyhow::Result<()> {
        self.sender.send(signal).await.map_err(|e| anyhow::anyhow!(e))
    }

    async fn recv_signal(&self) -> anyhow::Result<Signal> {
        let mut rx = self.receiver.lock().await;
        rx.recv()
            .await
            .ok_or_else(|| anyhow::anyhow!("Loopback channel closed"))
    }
}

// ───── Production AWS : UDP brut 1 octet ─────
pub struct UdpSignalTransport {
    socket: UdpSocket,
    target_addr: Option<SocketAddr>, // None pour le récepteur (Exécuteur)
}

impl UdpSignalTransport {
    /// Récepteur (Exécuteur) : se lie à une adresse locale et attend les datagrammes.
    pub async fn new_bind(local_addr: SocketAddr) -> anyhow::Result<Self> {
        let socket = UdpSocket::bind(local_addr).await?;
        Ok(Self {
            socket,
            target_addr: None,
        })
    }

    /// Émetteur (Radar) : se lie localement et cible l'adresse de l'Exécuteur.
    pub async fn new_connect(
        local_addr: SocketAddr,
        target_addr: SocketAddr,
    ) -> anyhow::Result<Self> {
        let socket = UdpSocket::bind(local_addr).await?;
        Ok(Self {
            socket,
            target_addr: Some(target_addr),
        })
    }
}

#[async_trait]
impl SignalTransport for UdpSignalTransport {
    async fn send_signal(&self, signal: Signal) -> anyhow::Result<()> {
        let Some(target) = self.target_addr else {
            return Err(anyhow::anyhow!("Mode récepteur uniquement : adresse cible manquante"));
        };
        match signal {
            Signal::Kill => { self.socket.send_to(&[0x4B], target).await?; }
            Signal::Heartbeat => { self.socket.send_to(&[0x48], target).await?; }
            Signal::Tick(t) => { self.socket.send_to(&t.encode(), target).await?; }
        }
        Ok(())
    }

    async fn recv_signal(&self) -> anyhow::Result<Signal> {
        let mut buf = [0u8; 128];
        let (n, _) = self.socket.recv_from(&mut buf).await?;
        match buf[0] {
            0x4B => Ok(Signal::Kill),
            0x48 => Ok(Signal::Heartbeat),
            0x54 => WireTick::decode(&buf[..n])
                .map(Signal::Tick)
                .ok_or_else(|| anyhow::anyhow!("trame Tick tronquée ({n} octets)")),
            other => Err(anyhow::anyhow!(
                "Octet de signalisation corrompu ou inconnu : 0x{:02X}",
                other
            )),
        }
    }
}
