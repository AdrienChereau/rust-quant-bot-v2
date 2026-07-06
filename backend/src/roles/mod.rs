//! Boucles principales par rôle. Squelettes au J0, étoffées aux jalons suivants
//! (radar: J1-J2, exécuteur: J6-J9).

pub mod executor;
pub mod executor_gtc; // stratégie pair-GTC (bot parallèle, STRATEGY=gtc)
pub mod radar;
