//! Transport UDP fire-and-forget pour le signal radar → exécuteur.
//!
//! - **Radar** : `UdpSender` ouvre un socket éphémère et `send` un `WireSignal` (6 octets) vers
//!   l'IP:port de l'exécuteur. Pas d'ACK, pas de retry : on privilégie la latence (le prochain
//!   tick OBI ré-émettra si besoin).
//! - **Exécuteur** : `listen(port)` ouvre un socket bound et pousse chaque paquet valide (len==6,
//!   kind connu) dans un `mpsc::channel`, drainé par la FSM d'exécution.

use std::net::SocketAddr;

use tokio::net::UdpSocket;
use tokio::sync::mpsc;

use super::wire::{WireSignal, WIRE_LEN};

/// Émetteur radar : socket éphémère réutilisable + adresse cible pré-résolue.
pub struct UdpSender {
    socket: UdpSocket,
    target: SocketAddr,
}

impl UdpSender {
    /// Lie un socket éphémère (`0.0.0.0:0`) et mémorise la cible.
    pub async fn new(target_ip: &str, target_port: u16) -> anyhow::Result<Self> {
        let socket = UdpSocket::bind("0.0.0.0:0").await?;
        let target: SocketAddr = format!("{target_ip}:{target_port}").parse()?;
        tracing::info!(%target, "UDP sender prêt (radar → exécuteur)");
        Ok(Self { socket, target })
    }

    /// "Tire" le signal en 6 octets. Erreur loggée, jamais fatale.
    pub async fn send(&self, signal: WireSignal) {
        let payload = signal.to_bytes();
        match self.socket.send_to(&payload, &self.target).await {
            Ok(_) => tracing::info!(?signal, target = %self.target, "🚀 signal UDP envoyé"),
            Err(e) => tracing::error!(error = %e, "échec envoi UDP"),
        }
    }
}

/// Écoute UDP côté exécuteur : tâche dédiée → renvoie le `Receiver` des signaux décodés.
pub async fn listen(port: u16) -> anyhow::Result<mpsc::Receiver<WireSignal>> {
    let socket = UdpSocket::bind(format!("0.0.0.0:{port}")).await?;
    tracing::info!(port, "👂 écoute UDP (exécuteur)");
    let (tx, rx) = mpsc::channel::<WireSignal>(128);

    tokio::spawn(async move {
        let mut buf = [0u8; 64];
        loop {
            match socket.recv_from(&mut buf).await {
                Ok((len, addr)) => {
                    if len != WIRE_LEN {
                        tracing::warn!(len, %addr, "paquet UDP de taille inattendue, ignoré");
                        continue;
                    }
                    match WireSignal::from_bytes(&buf[..WIRE_LEN]) {
                        Some(sig) => {
                            tracing::info!(?sig, %addr, "🎯 signal reçu de Tokyo");
                            if tx.send(sig).await.is_err() {
                                tracing::error!("récepteur UDP fermé, arrêt de l'écoute");
                                break;
                            }
                        }
                        None => tracing::warn!(%addr, "paquet UDP invalide (kind inconnu), ignoré"),
                    }
                }
                Err(e) => tracing::error!(error = %e, "erreur recv_from UDP"),
            }
        }
    });

    Ok(rx)
}
