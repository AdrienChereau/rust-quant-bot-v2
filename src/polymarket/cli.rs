//! Sous-commandes CLI Polymarket — remplace les scripts Python pour ops AWS.

use clap::Subcommand;

use crate::concurrency::bus::Side;
use crate::config::Config;
use crate::polymarket::live_executor::{self, LiveCredentials, OrderArgs};

#[derive(Subcommand)]
pub enum PolyCmd {
    /// Vérifie creds, sync balance (sig_type=3), affiche USDC CLOB.
    Verify,
    /// Dérive POLY_API_KEY/SECRET/PASSPHRASE via L1 (SDK v2). À lancer en local.
    DeriveCreds,
    /// Signe + log un ordre FAK test (jamais POSTé — dry-run forcé).
    DryOrder {
        #[arg(long)]
        token_id: String,
        #[arg(long, default_value = "0.01")]
        price: f64,
        #[arg(long, default_value = "1")]
        size: f64,
        /// Marché neg-risk → contrat de vérification EIP-712 différent (sig_type 0/1/2).
        #[arg(long, default_value = "false")]
        neg_risk: bool,
    },
}

pub async fn run(cmd: PolyCmd, _cfg: Config) -> anyhow::Result<()> {
    match cmd {
        PolyCmd::Verify => verify().await,
        PolyCmd::DeriveCreds => derive_creds().await,
        PolyCmd::DryOrder { token_id, price, size, neg_risk } => {
            dry_order(&token_id, price, size, neg_risk).await
        }
    }
}

async fn verify() -> anyhow::Result<()> {
    let creds = LiveCredentials::from_env()
        .ok_or_else(|| anyhow::anyhow!("identifiants POLY_* incomplets dans .env"))?;
    live_executor::startup_poly(&creds).await?;
    let usdc = live_executor::get_collateral_balance(&creds).await?;
    println!("OK — solde CLOB : {usdc:.2} USDC");
    Ok(())
}

async fn derive_creds() -> anyhow::Result<()> {
    #[cfg(not(feature = "live"))]
    anyhow::bail!("`poly derive-creds` requiert `cargo build --features live`");

    #[cfg(feature = "live")]
    {
        use polymarket_client_sdk_v2::auth::ExposeSecret as _;

        let partial = LiveCredentials::from_env_for_derive().ok_or_else(|| {
            anyhow::anyhow!("POLY_PRIVATE_KEY, POLY_FUNDER_ADDRESS et POLY_SIG_TYPE requis")
        })?;
        partial.log_config_check();
        let sdk = crate::polymarket::poly1271::derive_api_creds(&partial).await?;
        println!("POLY_API_KEY={}", sdk.key());
        println!("POLY_API_SECRET={}", sdk.secret().expose_secret());
        println!("POLY_PASSPHRASE={}", sdk.passphrase().expose_secret());
        Ok(())
    }
}

async fn dry_order(token_id: &str, price: f64, size: f64, neg_risk: bool) -> anyhow::Result<()> {
    let creds = LiveCredentials::from_env()
        .ok_or_else(|| anyhow::anyhow!("identifiants POLY_* incomplets"))?;
    let args = OrderArgs { side: Side::Up, price, size };
    let result = live_executor::place_order(false, Some(&creds), token_id, neg_risk, args).await?;
    match result {
        live_executor::PlaceResult::DryRun => println!("Dry-run OK — ordre signé, non POSTé"),
        live_executor::PlaceResult::Placed(id) => println!("Ordre accepté : {id}"),
    }
    Ok(())
}
