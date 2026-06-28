//! Rôles d'exécution du binaire :
//! - `radar` (Tokyo, émetteur) — diffuse les signaux OBI en UDP.
//! - `live`  (Dublin, récepteur) — exécution réelle uniquement, zéro code paper.
//! - `paper` (machine séparée, récepteur) — simulation pure, zéro code live.
//!
//! Le mode `mono` (radar+exécuteur in-process) reste dans `main.rs` pour le run local/cloudy.

pub mod live;
pub mod paper;
pub mod radar;
