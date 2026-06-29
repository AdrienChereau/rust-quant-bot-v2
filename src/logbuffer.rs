//! Ring buffer de logs en mémoire — alimente l'endpoint `/logs` du dashboard.
//!
//! **Zéro impact hot loop :** sur le chemin chaud les logs sont déjà throttlés (~1/5 s) et
//! écrivent déjà sur stdout. On tee chaque ligne vers un `VecDeque` borné (500 lignes) sous un
//! Mutex tenu quelques µs. Le dashboard lit le snapshot sur sa task séparée.
//!
//! Initialisé une seule fois dans `main()` → disponible sur **tous les nœuds** (donc la console
//! du nœud live de Dublin affiche les logs de Dublin).

use std::collections::VecDeque;
use std::io::{self, Write};
use std::sync::{Arc, Mutex, OnceLock};

use tracing_subscriber::fmt::MakeWriter;

pub type LogBuf = Arc<Mutex<VecDeque<String>>>;

static RING: OnceLock<LogBuf> = OnceLock::new();
const CAP: usize = 500;

#[derive(Clone)]
pub struct RingMaker {
    buf: LogBuf,
}

pub struct RingWriter {
    buf: LogBuf,
}

impl Write for RingWriter {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        // Tee : on garde le comportement stdout/journald intact…
        let _ = io::stdout().write_all(data);
        // …et on pousse les lignes complètes dans le ring borné.
        if let Ok(s) = std::str::from_utf8(data) {
            if let Ok(mut g) = self.buf.lock() {
                for line in s.lines() {
                    let t = line.trim_end();
                    if !t.is_empty() {
                        if g.len() >= CAP { g.pop_front(); }
                        g.push_back(t.to_string());
                    }
                }
            }
        }
        Ok(data.len())
    }
    fn flush(&mut self) -> io::Result<()> { io::stdout().flush() }
}

impl<'a> MakeWriter<'a> for RingMaker {
    type Writer = RingWriter;
    fn make_writer(&'a self) -> Self::Writer {
        RingWriter { buf: self.buf.clone() }
    }
}

/// Crée le buffer global et renvoie le `MakeWriter` à brancher sur le subscriber `fmt`.
pub fn maker() -> RingMaker {
    let buf: LogBuf = Arc::new(Mutex::new(VecDeque::with_capacity(CAP)));
    let _ = RING.set(buf.clone());
    RingMaker { buf }
}

/// Dernières lignes de log (la plus récente en dernier). Vide si non initialisé.
pub fn snapshot() -> Vec<String> {
    RING.get()
        .and_then(|b| b.lock().ok().map(|g| g.iter().cloned().collect()))
        .unwrap_or_default()
}
