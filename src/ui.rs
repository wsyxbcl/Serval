//! How Serval talks to whoever runs it: progress, warnings, produced files and summaries, either for a person in
//! a terminal (progress bars, plain lines) or, with `--progress json`, as JSON events on stderr for a program
//! such as Waxbill. One event per line, each with a "serval" key so it can be told apart from ordinary output.

use crate::utils::{configure_progress_bar, log_line};
use indicatif::ProgressBar;
use serde_json::{Value, json};
use std::fmt;
use std::io::{IsTerminal, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

/// Version of the event format; Waxbill checks it in the `hello` event.
pub const PROTOCOL: u32 = 1;

static JSON: OnceLock<bool> = OnceLock::new();

/// Switch to JSON events for this run and announce the protocol.
pub fn enable_json(command: &str) {
    let _ = JSON.set(true);
    event(json!({
        "serval": "hello",
        "protocol": PROTOCOL,
        "version": env!("CARGO_PKG_VERSION"),
        "command": command,
    }));
}

pub fn json() -> bool {
    JSON.get().copied().unwrap_or(false)
}

/// Whether a question can be asked: a person at a terminal, not a program reading events.
pub fn can_ask() -> bool {
    !json() && std::io::stdin().is_terminal()
}

/// Write one event (JSON mode only).
pub fn event(value: Value) {
    if json() {
        let mut err = std::io::stderr().lock();
        let _ = writeln!(err, "{value}");
    }
}

/// A progress bar for one phase (scan, read, plan, copy, move, write, remove, analyse). In JSON mode the bar is
/// hidden and its position is reported as `progress` events, about ten per second, until it finishes.
pub fn progress_bar(len: u64, phase: &'static str) -> ProgressBar {
    if !json() {
        let pb = ProgressBar::new(len);
        configure_progress_bar(&pb);
        return pb;
    }
    // The previous phase's final position comes before the new phase's first event.
    flush_progress();
    let pb = ProgressBar::hidden();
    pb.set_length(len);
    let watched = Arc::new(Watched {
        phase,
        pb: pb.clone(),
        last: AtomicU64::new(u64::MAX),
    });
    WATCHED.lock().unwrap().push(watched.clone());
    std::thread::spawn(move || {
        loop {
            watched.report();
            if watched.pb.is_finished() {
                break;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    });
    pb
}

struct Watched {
    phase: &'static str,
    pb: ProgressBar,
    last: AtomicU64,
}

impl Watched {
    /// Emit the position if it changed since the last event.
    fn report(&self) {
        let done = self.pb.position();
        if self.last.swap(done, Ordering::Relaxed) != done {
            event(json!({
                "serval": "progress",
                "phase": self.phase,
                "done": done,
                "total": self.pb.length(),
            }));
        }
    }
}

static WATCHED: Mutex<Vec<Arc<Watched>>> = Mutex::new(Vec::new());

/// Report the final position of every progress bar, so the last event before a result is never stale.
pub fn flush_progress() {
    if json() {
        for watched in WATCHED.lock().unwrap().iter() {
            watched.report();
        }
    }
}

/// A file or folder the run produced: "Saved to …" for people, an `output` event for programs.
pub fn output(kind: &str, path: &Path, rows: Option<usize>) {
    println!("Saved to {}", path.display());
    flush_progress();
    let mut value = json!({"serval": "output", "kind": kind, "path": path.to_string_lossy()});
    if let Some(rows) = rows {
        value["rows"] = json!(rows);
    }
    event(value);
}

/// A `summary` event with the run's counts (the plain summary line is printed by the caller).
pub fn summary(counts: Value) {
    flush_progress();
    let mut value = json!({"serval": "summary"});
    if let (Some(target), Some(fields)) = (value.as_object_mut(), counts.as_object()) {
        target.extend(fields.clone());
    }
    event(value);
}

/// The run stopped before writing anything because it needs a decision (e.g. how to handle existing targets).
/// `main` turns this into exit code 3.
#[derive(Debug)]
pub struct NeedsDecision(pub String);

impl fmt::Display for NeedsDecision {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for NeedsDecision {}

/// Report a warning: printed for people, a `warning` event for programs. Always logged.
pub fn warning(message: &str) {
    log_line(&format!("Warning: {message}"));
    if json() {
        event(json!({"serval": "warning", "message": message}));
    } else {
        eprintln!("Warning: {message}");
    }
}
