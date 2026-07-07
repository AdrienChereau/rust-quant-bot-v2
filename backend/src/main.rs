//! Point d'entrée du binaire `polymarket_mm_bot`.
//!
//! Le rôle est choisi par l'environnement (`BOT_ROLE`/`AWS_REGION`) :
//!   - `radar`    → Nœud Radar (Tokyo), émet les signaux KILL.
//!   - `executor` → Nœud Exécuteur (Dublin), reçoit les signaux et cote/exécute.
//!   - `combined` → mode dev local : les deux rôles dans le même process, reliés
//!                  par un transport loopback in-process (pour tester sur un seul Mac).

mod bankroll;
mod config;
mod connectors;
mod dashboard;
mod engines;
mod execution;
mod inventory;
#[cfg(feature = "live")]
mod live;
mod roles;
mod signal;
mod types;

use std::env;
use std::sync::Arc;

use anyhow::Context;

use config::{BotRole, Config};
use signal::{LoopbackTransport, SignalTransport, UdpSignalTransport};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let cfg = Config::from_env();
    tracing::info!(?cfg.role, dry_run = cfg.dry_run, "Démarrage polymarket_mm_bot");

    // État partagé + serveur de monitoring local.
    let role_str = match (env::var("BOT_ROLE").map(|r| r.eq_ignore_ascii_case("combined")).unwrap_or(false), cfg.role) {
        (true, _) => "executor", // combined : UI bot
        (_, config::BotRole::Radar) => "radar",
        _ => "executor",
    };
    let shared = dashboard::shared(cfg.dry_run, role_str);
    {
        let (port, st) = (cfg.dashboard_port, shared.clone());
        tokio::spawn(async move {
            if let Err(e) = dashboard::serve(port, st).await {
                tracing::error!(error = %e, "dashboard arrêté");
            }
        });
    }

    // Mode dev combiné : un seul process exécute Radar + Exécuteur via loopback.
    let combined = env::var("BOT_ROLE")
        .map(|r| r.eq_ignore_ascii_case("combined"))
        .unwrap_or(false);

    if combined {
        let transport: Arc<dyn SignalTransport> = Arc::new(LoopbackTransport::new(64));
        let exec_cfg = cfg.clone();
        let exec_transport = transport.clone();
        let exec_shared = shared.clone();
        let gtc = cfg.strategy.eq_ignore_ascii_case("gtc");
        let exec = tokio::spawn(async move {
            let res = if gtc {
                roles::executor_gtc::run(exec_cfg, exec_transport, exec_shared).await
            } else {
                roles::executor::run(exec_cfg, exec_transport, exec_shared).await
            };
            if let Err(e) = res {
                tracing::error!(error = %e, "executor terminé en erreur");
            }
        });
        let radar = tokio::spawn(async move {
            if let Err(e) = roles::radar::run(cfg, transport, shared).await {
                tracing::error!(error = %e, "radar terminé en erreur");
            }
        });
        let _ = tokio::try_join!(radar, exec);
        return Ok(());
    }

    // Mode mono-rôle : transport UDP (prod / dev deux terminaux).
    match cfg.role {
        BotRole::Radar => {
            let target = cfg
                .signal_target
                .context("SIGNAL_TARGET requis pour le rôle radar (adresse de l'exécuteur)")?;
            let local: std::net::SocketAddr = ([0, 0, 0, 0], 0).into();
            // Fan-out optionnel : SIGNAL_TARGET2 → le radar nourrit paper ET live.
            let transport: Arc<dyn SignalTransport> = match cfg.signal_target2 {
                Some(t2) => Arc::new(signal::FanoutTransport::new(vec![
                    Arc::new(UdpSignalTransport::new_connect(local, target).await?),
                    Arc::new(UdpSignalTransport::new_connect(([0, 0, 0, 0], 0).into(), t2).await?),
                ])),
                None => Arc::new(UdpSignalTransport::new_connect(local, target).await?),
            };
            roles::radar::run(cfg, transport, shared).await
        }
        BotRole::Executor => {
            let transport: Arc<dyn SignalTransport> =
                Arc::new(UdpSignalTransport::new_bind(cfg.signal_addr).await?);
            if cfg.strategy.eq_ignore_ascii_case("gtc") {
                roles::executor_gtc::run(cfg, transport, shared).await
            } else {
                roles::executor::run(cfg, transport, shared).await
            }
        }
    }
}
