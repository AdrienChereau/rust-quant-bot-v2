//! Ring buffer des logs du bot, exposé au dashboard (`GET /logs`).
//! Un `tracing::Layer` capture chaque événement ≥ INFO au format
//! `(heure, niveau, message champs=…)` — l'équivalent de `journalctl -f`
//! directement dans l'interface (indispensable pour piloter le LIVE).

use std::collections::VecDeque;
use std::fmt::Write as _;
use std::sync::Mutex;

use tracing::field::{Field, Visit};
use tracing_subscriber::layer::Context;
use tracing_subscriber::Layer;

const CAP: usize = 400;

pub static LOG_RING: Mutex<VecDeque<(String, String, String)>> = Mutex::new(VecDeque::new());

/// Dernières lignes (heure, niveau, message), la plus récente en dernier.
pub fn tail(max: usize) -> Vec<(String, String, String)> {
    let g = LOG_RING.lock().unwrap_or_else(|p| p.into_inner());
    g.iter().rev().take(max).rev().cloned().collect()
}

pub struct RingLayer;

impl<S: tracing::Subscriber> Layer<S> for RingLayer {
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        let level = *event.metadata().level();
        if level > tracing::Level::INFO {
            return; // TRACE/DEBUG : trop verbeux pour l'UI
        }
        let mut msg = String::new();
        let mut fields = String::new();
        struct V<'a> {
            msg: &'a mut String,
            fields: &'a mut String,
        }
        impl Visit for V<'_> {
            fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
                if field.name() == "message" {
                    let _ = write!(self.msg, "{value:?}");
                } else {
                    let _ = write!(self.fields, " {}={:?}", field.name(), value);
                }
            }
        }
        event.record(&mut V { msg: &mut msg, fields: &mut fields });
        // Nettoie les guillemets de Debug sur les strings.
        let msg = msg.trim_matches('"').to_string();
        let line = format!("{msg}{fields}");
        let entry = (
            chrono::Utc::now().format("%H:%M:%S").to_string(),
            level.to_string(),
            line,
        );
        let mut g = LOG_RING.lock().unwrap_or_else(|p| p.into_inner());
        g.push_back(entry);
        if g.len() > CAP {
            g.pop_front();
        }
    }
}
