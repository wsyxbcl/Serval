//! Copy or move media files together with their sidecars to computed targets.
//!
//! Every target is checked before anything is written: copies finished by an
//! earlier run are recognized and skipped (so an interrupted run can simply be
//! repeated), and the user is asked once how to handle targets that already
//! hold a different file.

use crate::schema::resource_extension;
use crate::utils::{configure_progress_bar, log_line, pb_status, run_log_path};
use indicatif::ProgressBar;
use std::collections::HashSet;
use std::fs::{self, File, FileTimes};
use std::io::{self, IsTerminal};
use std::path::{Path, PathBuf};
use std::time::Duration;

/// Put `source` at `target`; its sidecar, if any, goes to `<target>.xmp`.
/// A sidecar handled on its own (e.g. `xmp copy`) is a transfer without one.
pub struct Transfer {
    pub source: PathBuf,
    pub sidecar: Option<PathBuf>,
    pub target: PathBuf,
}

#[derive(Clone, Copy, PartialEq)]
pub enum Mode {
    Copy,
    Move,
}

/// What to do with targets that already hold a different file.
#[derive(Clone, Copy, PartialEq)]
pub enum OnConflict {
    Skip,
    Replace,
    Rename,
}

enum Check {
    /// Files still to be written (sidecar first).
    Write(Vec<(PathBuf, PathBuf)>),
    /// Everything is already in place.
    Done,
    /// A target holds something else. Replacing writes `writes` and removes
    /// `removes` (an unrelated sidecar next to the target).
    Conflict {
        reason: String,
        writes: Vec<(PathBuf, PathBuf)>,
        removes: Vec<PathBuf>,
    },
}

struct Planned {
    target: PathBuf,
    check: Check,
}

/// Plan, confirm and carry out `transfers`. `preset` answers the conflict
/// question without asking; `dry_run` only reports the plan.
pub fn run_transfers(
    mut transfers: Vec<Transfer>,
    mode: Mode,
    preset: Option<OnConflict>,
    dry_run: bool,
) -> anyhow::Result<()> {
    // Deterministic numbering, and one transfer per (source, target).
    transfers.sort_by(|a, b| (&a.source, &a.target).cmp(&(&b.source, &b.target)));
    transfers.dedup_by(|a, b| a.source == b.source && a.target == b.target);

    let mut planned = plan(&transfers, false)?;
    let conflicts: Vec<(&Path, &str)> = planned
        .iter()
        .filter_map(|p| match &p.check {
            Check::Conflict { reason, .. } => Some((p.target.as_path(), reason.as_str())),
            _ => None,
        })
        .collect();
    let choice = if conflicts.is_empty() {
        None
    } else {
        report_conflicts(&conflicts);
        if dry_run {
            None
        } else if let Some(preset) = preset {
            Some(preset)
        } else {
            Some(ask_on_conflict()?)
        }
    };
    if choice == Some(OnConflict::Rename) {
        planned = plan(&transfers, true)?;
    }

    let to_write = planned
        .iter()
        .filter(|p| matches!(p.check, Check::Write(_)))
        .count();
    let done = planned
        .iter()
        .filter(|p| matches!(p.check, Check::Done))
        .count();
    if dry_run {
        println!(
            "DRYRUN: {to_write} to write, {done} already in place, {} conflicting",
            planned.len() - to_write - done
        );
        for p in planned.iter().take(5) {
            println!("DRYRUN sample: -> {}", p.target.display());
        }
        return Ok(());
    }

    let pb = ProgressBar::new(planned.len() as u64);
    configure_progress_bar(&pb);
    let (mut written, mut skipped, mut replaced) = (0, 0, 0);
    for p in &planned {
        match &p.check {
            Check::Done => {}
            Check::Write(writes) => {
                put_all(writes, &[], mode, &pb)?;
                written += 1;
            }
            Check::Conflict {
                writes, removes, ..
            } => match choice {
                Some(OnConflict::Replace) => {
                    put_all(writes, removes, mode, &pb)?;
                    replaced += 1;
                }
                _ => {
                    log_line(&format!("Skipping existing {}", p.target.display()));
                    skipped += 1;
                }
            },
        }
        pb.inc(1);
    }
    pb.finish_and_clear();

    let verb = if mode == Mode::Move {
        "moved"
    } else {
        "copied"
    };
    let mut summary = format!("{written} {verb}, {done} already in place");
    if skipped > 0 {
        summary.push_str(&format!(", {skipped} skipped"));
    }
    if replaced > 0 {
        summary.push_str(&format!(", {replaced} replaced"));
    }
    log_line(&summary);
    println!("{summary}");
    Ok(())
}

/// Assign every transfer a target. Clashes within the plan get "_1", "_2", ...
/// in order; with `avoid_conflicts`, so do targets holding something else.
fn plan(transfers: &[Transfer], avoid_conflicts: bool) -> anyhow::Result<Vec<Planned>> {
    let mut claimed: HashSet<PathBuf> = HashSet::new();
    let mut planned = Vec::with_capacity(transfers.len());
    for transfer in transfers {
        let mut n = 0;
        loop {
            let target = numbered(&transfer.target, n);
            let slots = slots(transfer, &target);
            if slots.iter().any(|(_, dst)| claimed.contains(dst)) {
                n += 1;
                continue;
            }
            let check = check(&slots)?;
            if avoid_conflicts && matches!(check, Check::Conflict { .. }) {
                n += 1;
                continue;
            }
            claimed.extend(slots.into_iter().map(|(_, dst)| dst));
            planned.push(Planned { target, check });
            break;
        }
    }
    Ok(planned)
}

/// (source, destination) pairs a transfer occupies, sidecar first. A media
/// file always occupies its sidecar slot, even without a sidecar, so that an
/// unrelated sidecar is never paired with it.
fn slots(transfer: &Transfer, target: &Path) -> Vec<(Option<PathBuf>, PathBuf)> {
    let mut slots = Vec::new();
    if !is_xmp(&transfer.source) {
        slots.push((transfer.sidecar.clone(), target.with_added_extension("xmp")));
    }
    slots.push((Some(transfer.source.clone()), target.to_path_buf()));
    slots
}

fn check(slots: &[(Option<PathBuf>, PathBuf)]) -> anyhow::Result<Check> {
    let mut writes = Vec::new();
    let mut all_writes = Vec::new();
    let mut removes = Vec::new();
    let mut reasons = Vec::new();
    for (source, dst) in slots {
        let exists = dst.symlink_metadata().is_ok();
        match source {
            Some(source) => {
                all_writes.push((source.clone(), dst.clone()));
                if !exists {
                    writes.push((source.clone(), dst.clone()));
                } else if !is_same(source, dst)? {
                    reasons.push(format!("{} differs", file_name(dst)));
                }
            }
            None if exists => {
                removes.push(dst.clone());
                reasons.push(format!("unrelated {}", file_name(dst)));
            }
            None => {}
        }
    }
    Ok(if !reasons.is_empty() {
        Check::Conflict {
            reason: reasons.join(", "),
            writes: all_writes,
            removes,
        }
    } else if writes.is_empty() {
        Check::Done
    } else {
        Check::Write(writes)
    })
}

/// Whether `target` already holds `source`: sidecars by content, media by
/// size and modified time (which copies keep; allow for coarse timestamps).
fn is_same(source: &Path, target: &Path) -> io::Result<bool> {
    if is_xmp(target) {
        return Ok(fs::read(source)? == fs::read(target)?);
    }
    let (source, target) = (fs::metadata(source)?, fs::metadata(target)?);
    if source.len() != target.len() {
        return Ok(false);
    }
    let (a, b) = (source.modified()?, target.modified()?);
    let diff = a.duration_since(b).or_else(|_| b.duration_since(a));
    Ok(diff.is_ok_and(|d| d <= Duration::from_secs(2)))
}

fn put_all(
    writes: &[(PathBuf, PathBuf)],
    removes: &[PathBuf],
    mode: Mode,
    pb: &ProgressBar,
) -> anyhow::Result<()> {
    for dst in removes {
        log_line(&format!("Removing unrelated {}", dst.display()));
        fs::remove_file(dst)?;
    }
    for (source, dst) in writes {
        pb_status(
            pb,
            format!(
                "{} {} -> {}",
                if mode == Mode::Move {
                    "Moving"
                } else {
                    "Copying"
                },
                source.display(),
                dst.display()
            ),
        );
        if let Some(parent) = dst.parent() {
            fs::create_dir_all(parent)?;
        }
        match mode {
            Mode::Move => fs::rename(source, dst)?,
            Mode::Copy => copy_via_temp(source, dst)?,
        }
    }
    Ok(())
}

/// Copy through a temp file renamed into place, so `dst` only ever holds a
/// complete copy. The copy keeps the source's timestamps.
fn copy_via_temp(source: &Path, dst: &Path) -> anyhow::Result<()> {
    let (mut temp, temp_path) = create_new_sibling(dst, "serval", "tmp")?;
    let result = (|| -> io::Result<()> {
        io::copy(&mut File::open(source)?, &mut temp)?;
        let metadata = fs::metadata(source)?;
        temp.set_times(
            FileTimes::new()
                .set_accessed(metadata.accessed()?)
                .set_modified(metadata.modified()?),
        )?;
        drop(temp);
        fs::rename(&temp_path, dst)
    })();
    if let Err(err) = result {
        let _ = fs::remove_file(&temp_path);
        return Err(anyhow::anyhow!(
            "Failed to copy {} to {}: {err}",
            source.display(),
            dst.display()
        ));
    }
    Ok(())
}

/// Create `<path>.<tag>.<ext>` (or `<path>.<tag>_1.<ext>`, ... if taken)
/// without ever replacing an existing file.
pub(crate) fn create_new_sibling(path: &Path, tag: &str, ext: &str) -> io::Result<(File, PathBuf)> {
    let mut i = 0;
    loop {
        let mut name = path.as_os_str().to_owned();
        if i == 0 {
            name.push(format!(".{tag}.{ext}"));
        } else {
            name.push(format!(".{tag}_{i}.{ext}"));
        }
        let candidate = PathBuf::from(name);
        match fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&candidate)
        {
            Ok(file) => return Ok((file, candidate)),
            Err(err) if err.kind() == io::ErrorKind::AlreadyExists => i += 1,
            Err(err) => return Err(err),
        }
    }
}

/// `a.jpg` -> `a_1.jpg`; a sidecar `a.jpg.xmp` -> `a_1.jpg.xmp`, so it still
/// pairs with its numbered media file.
fn numbered(target: &Path, n: usize) -> PathBuf {
    if n == 0 {
        return target.to_path_buf();
    }
    if is_xmp(target) {
        let mut name = numbered(&target.with_extension(""), n).into_os_string();
        name.push(".");
        name.push(target.extension().unwrap_or_default());
        return PathBuf::from(name);
    }
    let mut name = target.file_stem().unwrap_or_default().to_os_string();
    name.push(format!("_{n}"));
    if let Some(ext) = target.extension() {
        name.push(".");
        name.push(ext);
    }
    target.with_file_name(name)
}

fn is_xmp(path: &Path) -> bool {
    resource_extension(path).as_deref() == Some("xmp")
}

fn file_name(path: &Path) -> String {
    path.file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default()
}

fn report_conflicts(conflicts: &[(&Path, &str)]) {
    const SHOWN: usize = 10;
    eprintln!(
        "{} target(s) already exist with different content:",
        conflicts.len()
    );
    for (i, (target, reason)) in conflicts.iter().enumerate() {
        log_line(&format!("Existing target: {} ({reason})", target.display()));
        if i < SHOWN {
            eprintln!("  {} ({reason})", target.display());
        }
    }
    if conflicts.len() > SHOWN {
        match run_log_path() {
            Some(log) => eprintln!(
                "  ... and {} more, all listed in {}",
                conflicts.len() - SHOWN,
                log.display()
            ),
            None => eprintln!("  ... and {} more", conflicts.len() - SHOWN),
        }
    }
}

fn ask_on_conflict() -> anyhow::Result<OnConflict> {
    if !io::stdin().is_terminal() {
        return Err(anyhow::anyhow!(
            "Existing targets differ from their sources; run in a terminal to choose \
             whether to skip, replace or rename them. Nothing was written."
        ));
    }
    let mut rl = rustyline::DefaultEditor::new()?;
    loop {
        let answer =
            rl.readline("[s]kip them / [r]eplace them / [n]ew name (_1, _2, ...) / [a]bort: ")?;
        match answer.trim().to_lowercase().chars().next() {
            Some('s') => return Ok(OnConflict::Skip),
            Some('r') => return Ok(OnConflict::Replace),
            Some('n') => return Ok(OnConflict::Rename),
            Some('a') => return Err(anyhow::anyhow!("Aborted, nothing was written.")),
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn media_and_sidecar_stay_paired() {
        let dir = tempfile::tempdir().unwrap();
        let (src, out) = (dir.path().join("src"), dir.path().join("out"));
        for cam in ["cam_a", "cam_b"] {
            fs::create_dir_all(src.join(cam)).unwrap();
            fs::write(src.join(cam).join("a.jpg"), cam).unwrap();
            fs::write(src.join(cam).join("a.jpg.xmp"), cam).unwrap();
        }
        // An unrelated sidecar where the first copy would go (R7).
        fs::create_dir_all(&out).unwrap();
        fs::write(out.join("a.jpg.xmp"), "unrelated").unwrap();
        let transfers = || {
            ["cam_a", "cam_b"]
                .map(|cam| Transfer {
                    source: src.join(cam).join("a.jpg"),
                    sidecar: Some(src.join(cam).join("a.jpg.xmp")),
                    target: out.join("a.jpg"),
                })
                .into()
        };
        // Both sources map to out/a.jpg; out/a.jpg.xmp belongs to neither.
        run_transfers(transfers(), Mode::Copy, Some(OnConflict::Rename), false).unwrap();
        let read = |name: &str| fs::read_to_string(out.join(name)).unwrap();
        assert_eq!(read("a.jpg.xmp"), "unrelated");
        assert_eq!(
            (read("a_1.jpg"), read("a_1.jpg.xmp")),
            ("cam_a".into(), "cam_a".into())
        );
        assert_eq!(
            (read("a_2.jpg"), read("a_2.jpg.xmp")),
            ("cam_b".into(), "cam_b".into())
        );

        // A rerun recognizes the finished copies and writes nothing new.
        run_transfers(transfers(), Mode::Copy, Some(OnConflict::Rename), false).unwrap();
        assert_eq!(fs::read_dir(&out).unwrap().count(), 5);
    }
}
