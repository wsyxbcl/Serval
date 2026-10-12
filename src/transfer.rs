//! Copy or move media files together with their sidecars to computed targets.
//!
//! Every target is checked before anything is written: copies finished by an
//! earlier run are recognized and skipped (so an interrupted run can simply be
//! repeated), and the user is asked once how to handle targets that already
//! hold a different file.

use crate::schema::resource_extension;
use crate::ui;
use crate::utils::{log_line, pb_status, run_log_path};
use indicatif::ProgressBar;
use serde_json::json;
use std::collections::HashSet;
use std::fs::{self, File, FileTimes};
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::time::Duration;

/// Put `source` at `target`; its sidecar, if any, goes to `<target>.xmp`.
/// A sidecar handled on its own (e.g. `xmp copy`) is a transfer without one.
pub struct Transfer {
    pub source: PathBuf,
    pub sidecar: Option<PathBuf>,
    pub target: PathBuf,
    /// Whether the sidecar next to `target` belongs to this transfer. Then an
    /// existing sidecar there counts as a conflict even when `sidecar` is None,
    /// so that an unrelated sidecar is never paired with the new media file.
    /// Off when sidecars are handled separately (e.g. `align -t media`).
    pub sidecar_slot: bool,
}

#[derive(Clone, Copy, PartialEq)]
pub enum Mode {
    Copy,
    Move,
}

/// What to do with targets that already hold a different file.
#[derive(Clone, Copy, PartialEq, Debug, clap::ValueEnum)]
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
        /// Machine-readable reasons: file-differs, sidecar-differs, unrelated-sidecar.
        codes: Vec<&'static str>,
        writes: Vec<(PathBuf, PathBuf)>,
        removes: Vec<PathBuf>,
    },
}

struct Planned {
    source: PathBuf,
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

    let mut planned = plan(&transfers, false, mode)?;
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
        } else if ui::can_ask() {
            Some(ask_on_conflict()?)
        } else {
            report_needs_decision(&planned);
            return Err(ui::NeedsDecision(format!(
                "{} existing target(s) differ from their sources; nothing was written. \
                 Choose with --on-existing skip|replace|rename.",
                conflicts.len()
            ))
            .into());
        }
    };
    if choice == Some(OnConflict::Rename) {
        planned = plan(&transfers, true, mode)?;
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
        ui::summary(json!({
            "dry_run": true,
            "to_write": to_write,
            "already": done,
            "conflicting": planned.len() - to_write - done,
        }));
        let mut sampled = HashSet::new();
        for p in &planned {
            if sampled.insert(p.source.parent()) {
                println!(
                    "DRYRUN sample: From {} to {}",
                    p.source.display(),
                    p.target.display()
                );
            }
        }
        return Ok(());
    }

    let phase = if mode == Mode::Move { "move" } else { "copy" };
    let pb = ui::progress_bar(planned.len() as u64, phase);
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
    ui::summary(json!({
        "written": written,
        "already": done,
        "skipped": skipped,
        "replaced": replaced,
        "failed": 0,
    }));
    Ok(())
}

/// The conflict list as a `needs` event (at most 200 items; `count` is the full number), for a program to ask the
/// user and run again with `--on-existing`.
fn report_needs_decision(planned: &[Planned]) {
    let conflicts: Vec<&Planned> = planned
        .iter()
        .filter(|p| matches!(p.check, Check::Conflict { .. }))
        .collect();
    let items: Vec<_> = conflicts
        .iter()
        .take(200)
        .filter_map(|p| match &p.check {
            Check::Conflict { codes, .. } => {
                Some(json!({"target": p.target.to_string_lossy(), "reasons": codes}))
            }
            _ => None,
        })
        .collect();
    let ready = planned
        .iter()
        .filter(|p| matches!(p.check, Check::Write(_)))
        .count();
    let done = planned
        .iter()
        .filter(|p| matches!(p.check, Check::Done))
        .count();
    ui::event(json!({
        "serval": "needs",
        "kind": "conflicts",
        "count": conflicts.len(),
        "ready": ready,
        "already": done,
        "items": items,
    }));
}

/// Assign every transfer a target. Clashes within the plan get "_1", "_2", ...
/// in order; with `avoid_conflicts`, so do targets holding something else.
fn plan(transfers: &[Transfer], avoid_conflicts: bool, mode: Mode) -> anyhow::Result<Vec<Planned>> {
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
            let check = check(&slots, mode)?;
            if avoid_conflicts && matches!(check, Check::Conflict { .. }) {
                n += 1;
                continue;
            }
            claimed.extend(slots.into_iter().map(|(_, dst)| dst));
            planned.push(Planned {
                source: transfer.source.clone(),
                target,
                check,
            });
            break;
        }
    }
    Ok(planned)
}

/// (source, destination) pairs a transfer occupies, sidecar first. The
/// sidecar keeps the extension case of its source.
fn slots(transfer: &Transfer, target: &Path) -> Vec<(Option<PathBuf>, PathBuf)> {
    let mut slots = Vec::new();
    if transfer.sidecar_slot && !is_xmp(&transfer.source) {
        let ext = transfer
            .sidecar
            .as_deref()
            .and_then(Path::extension)
            .unwrap_or("xmp".as_ref());
        let selected = target.with_added_extension(ext);
        let alternate = target.with_added_extension(if ext == "XMP" { "xmp" } else { "XMP" });
        // Both supported spellings occupy the sidecar slot. On a case-insensitive
        // filesystem they can name the same file, which must not be removed twice.
        let regular_entries = [&selected, &alternate]
            .iter()
            .all(|path| fs::symlink_metadata(path).is_ok_and(|m| !m.file_type().is_symlink()));
        let same_file = regular_entries
            && fs::canonicalize(&selected)
                .ok()
                .zip(fs::canonicalize(&alternate).ok())
                .is_some_and(|(a, b)| a == b);
        if !same_file {
            slots.push((None, alternate));
        }
        slots.push((transfer.sidecar.clone(), selected));
    }
    slots.push((Some(transfer.source.clone()), target.to_path_buf()));
    slots
}

fn check(slots: &[(Option<PathBuf>, PathBuf)], mode: Mode) -> anyhow::Result<Check> {
    let mut writes = Vec::new();
    let mut all_writes = Vec::new();
    let mut removes = Vec::new();
    let mut reasons = Vec::new();
    let mut codes = Vec::new();
    for (source, dst) in slots {
        let exists = dst.symlink_metadata().is_ok();
        match source {
            Some(source) => {
                all_writes.push((source.clone(), dst.clone()));
                if !exists {
                    writes.push((source.clone(), dst.clone()));
                } else if !is_same(source, dst)? {
                    reasons.push(format!("{} differs", file_name(dst)));
                    codes.push(if is_xmp(dst) {
                        "sidecar-differs"
                    } else {
                        "file-differs"
                    });
                }
            }
            None if exists => {
                removes.push(dst.clone());
                reasons.push(format!("unrelated {}", file_name(dst)));
                codes.push("unrelated-sidecar");
            }
            None => {}
        }
    }
    Ok(if !reasons.is_empty() {
        Check::Conflict {
            reason: reasons.join(", "),
            codes,
            writes: all_writes,
            removes,
        }
    } else if mode == Mode::Move {
        // A move must consume both sources, including already copied members.
        Check::Write(all_writes)
    } else if writes.is_empty() {
        Check::Done
    } else {
        Check::Write(writes)
    })
}

/// Metadata is only a preliminary filter; matching media must also match byte for byte.
fn is_same(source: &Path, target: &Path) -> io::Result<bool> {
    if is_xmp(target) {
        return Ok(fs::read(source)? == fs::read(target)?);
    }
    let (source_meta, target_meta) = (fs::metadata(source)?, fs::metadata(target)?);
    if source_meta.len() != target_meta.len() {
        return Ok(false);
    }
    let (a, b) = (source_meta.modified()?, target_meta.modified()?);
    let diff = a.duration_since(b).or_else(|_| b.duration_since(a));
    if !diff.is_ok_and(|d| d <= Duration::from_secs(2)) {
        return Ok(false);
    }
    let (mut source, mut target) = (File::open(source)?, File::open(target)?);
    let (mut a, mut b) = ([0u8; 64 * 1024], [0u8; 64 * 1024]);
    loop {
        let n = source.read(&mut a)?;
        if n == 0 {
            return Ok(target.read(&mut b[..1])? == 0);
        }
        target.read_exact(&mut b[..n])?;
        if a[..n] != b[..n] {
            return Ok(false);
        }
    }
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
    if ui::json() {
        for (target, reason) in conflicts {
            log_line(&format!("Existing target: {} ({reason})", target.display()));
        }
        return;
    }
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
    fn move_finishes_both_members_of_a_partially_copied_pair() {
        for existing in ["media", "sidecar", "both"] {
            let dir = tempfile::tempdir().unwrap();
            let source = dir.path().join("a.jpg");
            let sidecar = source.with_added_extension("xmp");
            let target = dir.path().join("out/a.jpg");
            fs::create_dir_all(target.parent().unwrap()).unwrap();
            fs::write(&source, "media").unwrap();
            fs::write(&sidecar, "labels").unwrap();
            if existing != "sidecar" {
                copy_via_temp(&source, &target).unwrap();
            }
            if existing != "media" {
                copy_via_temp(&sidecar, &target.with_added_extension("xmp")).unwrap();
            }
            run_transfers(
                vec![Transfer {
                    source: source.clone(),
                    sidecar: Some(sidecar.clone()),
                    target: target.clone(),
                    sidecar_slot: true,
                }],
                Mode::Move,
                Some(OnConflict::Replace),
                false,
            )
            .unwrap();
            assert!(
                !source.exists(),
                "source media remains when {existing} exists"
            );
            assert!(!sidecar.exists());
            assert_eq!(fs::read_to_string(&target).unwrap(), "media");
            assert_eq!(
                fs::read_to_string(target.with_added_extension("xmp")).unwrap(),
                "labels"
            );
        }
    }

    #[test]
    fn matching_media_metadata_does_not_bypass_conflict_policy() {
        for policy in [OnConflict::Skip, OnConflict::Replace, OnConflict::Rename] {
            let dir = tempfile::tempdir().unwrap();
            let source = dir.path().join("source.jpg");
            let target = dir.path().join("target.jpg");
            fs::write(&source, "new").unwrap();
            copy_via_temp(&source, &target).unwrap();
            let time = fs::metadata(&source).unwrap().modified().unwrap();
            fs::write(&target, "old").unwrap();
            File::options()
                .write(true)
                .open(&target)
                .unwrap()
                .set_times(FileTimes::new().set_modified(time))
                .unwrap();
            run_transfers(
                vec![Transfer {
                    source,
                    sidecar: None,
                    target: target.clone(),
                    sidecar_slot: true,
                }],
                Mode::Copy,
                Some(policy),
                false,
            )
            .unwrap();
            assert_eq!(
                fs::read_to_string(&target).unwrap(),
                if policy == OnConflict::Replace {
                    "new"
                } else {
                    "old"
                }
            );
            if policy == OnConflict::Rename {
                assert_eq!(
                    fs::read_to_string(dir.path().join("target_1.jpg")).unwrap(),
                    "new"
                );
            }
        }
    }

    #[test]
    fn media_comparison_checks_beyond_the_first_buffer() {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("source.mp4");
        let target = dir.path().join("target.mp4");
        let mut data = vec![1u8; 128 * 1024 + 1];
        fs::write(&source, &data).unwrap();
        copy_via_temp(&source, &target).unwrap();
        assert!(is_same(&source, &target).unwrap());
        *data.last_mut().unwrap() = 2;
        fs::write(&target, data).unwrap();
        File::options()
            .write(true)
            .open(&target)
            .unwrap()
            .set_times(
                FileTimes::new().set_modified(fs::metadata(&source).unwrap().modified().unwrap()),
            )
            .unwrap();
        assert!(!is_same(&source, &target).unwrap());
    }

    #[test]
    fn replacement_clears_alternate_sidecar_spelling() {
        for (old_ext, new_ext) in [("xmp", "XMP"), ("XMP", "xmp")] {
            let dir = tempfile::tempdir().unwrap();
            let source = dir.path().join("source.jpg");
            let sidecar = source.with_added_extension(new_ext);
            let target = dir.path().join("target.jpg");
            fs::write(&source, "new media").unwrap();
            fs::write(&sidecar, "new labels").unwrap();
            fs::write(&target, "old").unwrap();
            fs::write(target.with_added_extension(old_ext), "old labels").unwrap();
            run_transfers(
                vec![Transfer {
                    source,
                    sidecar: Some(sidecar),
                    target: target.clone(),
                    sidecar_slot: true,
                }],
                Mode::Copy,
                Some(OnConflict::Replace),
                false,
            )
            .unwrap();
            assert_eq!(
                fs::read_to_string(target.with_added_extension(new_ext)).unwrap(),
                "new labels"
            );
            if let Ok(labels) = fs::read_to_string(target.with_added_extension(old_ext)) {
                // On case-insensitive filesystems both spellings name the new sidecar.
                assert_eq!(labels, "new labels");
            }
        }
    }

    #[cfg(unix)]
    #[test]
    fn replacement_clears_sidecar_behind_alternate_symlink() {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("source.jpg");
        let sidecar = source.with_added_extension("XMP");
        let target = dir.path().join("target.jpg");
        fs::write(&source, "new media").unwrap();
        fs::write(&sidecar, "new labels").unwrap();
        fs::write(target.with_added_extension("xmp"), "old labels").unwrap();
        std::os::unix::fs::symlink(
            target.with_added_extension("xmp"),
            target.with_added_extension("XMP"),
        )
        .unwrap();
        run_transfers(
            vec![Transfer {
                source,
                sidecar: Some(sidecar),
                target: target.clone(),
                sidecar_slot: true,
            }],
            Mode::Copy,
            Some(OnConflict::Replace),
            false,
        )
        .unwrap();
        assert!(!target.with_added_extension("xmp").exists());
        assert_eq!(
            fs::read_to_string(target.with_added_extension("XMP")).unwrap(),
            "new labels"
        );
    }

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
                    sidecar_slot: true,
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
