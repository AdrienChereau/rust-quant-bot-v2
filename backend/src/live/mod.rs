//! Exécution LIVE Polymarket (feature `live` uniquement — rustc ≥ 1.91).
//! Voir auth.rs (verrous), orders.rs (GTC/FAK/cancel/heartbeat), user_ws.rs
//! (fills), engine.rs (pont avec la boucle executor).
pub mod auth;
pub mod engine;
pub mod orders;
pub mod user_ws;
