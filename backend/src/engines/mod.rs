//! Moteurs quantitatifs : radar (HFT), pricing, volatilité, risque.
//! Les modules `pricing`/`volatility`/`risk` arrivent aux jalons J5/J4/J7.

pub mod drift; // signal : terme de drift/momentum (correctif tendance, alimente le gate ⚡)
pub mod ofi; // signal : order flow imbalance (observabilité)
pub mod pair_gtc; // stratégie utilisateur : 2 GTC → cancel → complétion <1$ (bot 8700)
pub mod spread_capture; // stratégie v5 : taker dip-accumulation (validée Phase A)
pub mod pricing; // J5
pub mod radar;
pub mod risk; // J7
pub mod volatility; // J4
