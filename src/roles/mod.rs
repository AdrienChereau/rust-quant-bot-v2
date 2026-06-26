//! Rôles d'exécution du binaire : `radar` (Tokyo, émetteur) et `executor` (Dublin, récepteur).
//! Le mode `mono` (radar+exécuteur in-process) reste dans `main.rs` pour le run local/cloudy.

pub mod executor;
pub mod radar;
