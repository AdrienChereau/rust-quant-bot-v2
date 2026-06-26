//! Gestion typée des erreurs (thiserror).

use thiserror::Error;

#[derive(Error, Debug)]
pub enum BotError {
    #[error("websocket: {0}")]
    WebSocket(String),
    #[error("parsing: {0}")]
    Parse(String),
    #[error("réseau: {0}")]
    Network(String),
    #[error("config: {0}")]
    Config(String),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

pub type BotResult<T> = Result<T, BotError>;
