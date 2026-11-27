//! The contract a program such as Waxbill relies on: with every answer given as a flag, Serval runs without a
//! terminal and never prompts; `--progress json` writes JSON events on stderr; a conflict stops the run before
//! writing anything with exit code 3 and a `needs` event.

use std::fs;
use std::io::Write;
use std::path::Path;
use std::process::{Command, Output, Stdio};

fn serval(args: &[&str], dir: &Path) -> Output {
    Command::new(env!("CARGO_BIN_EXE_serval"))
        .args(args)
        .current_dir(dir)
        .stdin(Stdio::null())
        .output()
        .expect("serval runs")
}

fn events(output: &Output) -> Vec<serde_json::Value> {
    String::from_utf8_lossy(&output.stderr)
        .lines()
        .filter_map(|line| serde_json::from_str(line).ok())
        .collect()
}

#[test]
fn observe_and_init_keep_exif_midnight() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let media = dir.join("media");
    fs::create_dir(&media).unwrap();
    fs::write(
        media.join("a.jpg"),
        include_bytes!("fixtures/exif-midnight.jpg"),
    )
    .unwrap();
    let run = |args: &[&str], prefix: &str| {
        let out = serval(args, dir);
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
        let table = events(&out)
            .into_iter()
            .find(|e| {
                e["serval"] == "output"
                    && e["kind"] == "csv"
                    && Path::new(e["path"].as_str().unwrap())
                        .file_name()
                        .unwrap()
                        .to_string_lossy()
                        .starts_with(prefix)
            })
            .unwrap();
        fs::read_to_string(dir.join(table["path"].as_str().unwrap())).unwrap()
    };
    let observe = |output: &str, sidecar: bool| {
        let mut args = vec!["observe", "media", "-o", output, "--progress", "json"];
        if sidecar {
            args.push("--xmp");
        }
        run(&args, "tags_")
    };
    let init = |output: &str| {
        run(
            &["xmp", "init", "media", "-o", output, "--progress", "json"],
            "xmp_init_",
        )
    };
    let datetime = "2024-10-19 00:00:00";
    let from_media = observe("embedded", false);
    assert!(from_media.contains(datetime), "{from_media}");
    let initialized = init("new");
    assert!(initialized.contains(datetime), "{initialized}");
    let xmp = media.join("a.jpg.xmp");
    let sidecar = fs::read_to_string(&xmp).unwrap();
    let labels = serval(&["xmp", "get", "media/a.jpg.xmp"], dir);
    assert!(labels.status.success());
    let labels: serde_json::Value = serde_json::from_slice(&labels.stdout).unwrap();
    assert_eq!(labels["labels"]["datetime"], datetime);
    let from_new_sidecar = observe("new_sidecar", true);
    assert!(from_new_sidecar.contains(datetime), "{from_new_sidecar}");

    // Older versions wrote a date-only sidecar for the same embedded midnight.
    let date_only = sidecar.replace("2024-10-19T00:00:00", "2024-10-19");
    fs::write(&xmp, &date_only).unwrap();
    let from_old_sidecar = observe("old_sidecar", true);
    assert!(from_old_sidecar.contains(datetime), "{from_old_sidecar}");
    assert!(init("existing").contains(datetime));
    assert_eq!(fs::read_to_string(&xmp).unwrap(), date_only);

    for (mode, path, sidecar_mode) in [
        ("media-list", "a.jpg", false),
        ("media-as-xmp", "a.jpg", true),
        ("xmp-list", "a.jpg.xmp", true),
    ] {
        fs::write(media.join("paths.csv"), format!("path\n{path}\n")).unwrap();
        let mut args = vec![
            "observe",
            "--files-from",
            "media/paths.csv",
            "-o",
            mode,
            "--progress",
            "json",
        ];
        if sidecar_mode {
            args.push("-x");
        }
        assert!(run(&args, "tags_").contains(datetime));
    }
    let get = serval(&["xmp", "get", "media/a.jpg.xmp"], dir);
    let labels: serde_json::Value = serde_json::from_slice(&get.stdout).unwrap();
    assert_eq!(labels["labels"]["datetime"], datetime);
    let written = set_edits(
        dir,
        &[serde_json::json!({
            "path":"media/a.jpg.xmp", "expect":{"datetime":datetime}, "set":{"species":["Fox"]}
        })],
    );
    let state: serde_json::Value = serde_json::from_slice(&written.stdout).unwrap();
    assert_eq!(state["result"], "written");
    assert_eq!(state["labels"]["datetime"], datetime);
    let edited = fs::read_to_string(&xmp).unwrap();
    assert!(edited.contains("2024-10-19") && !edited.contains("2024-10-19T00:00:00"));

    // A different date in a sidecar cannot be repaired using the media's time.
    let changed = date_only.replace("2024-10-19", "2024-10-20");
    fs::write(&xmp, &changed).unwrap();
    let corrected_date = observe("different_date", true);
    assert!(!corrected_date.contains("00:00:00"), "{corrected_date}");
    assert_eq!(fs::read_to_string(&xmp).unwrap(), changed);

    let corrected_time = sidecar.replace("T00:00:00", "T08:30:45");
    fs::write(&xmp, &corrected_time).unwrap();
    let observed = observe("corrected_time", true);
    assert!(observed.contains("2024-10-19 08:30:45"), "{observed}");
    assert_eq!(fs::read_to_string(&xmp).unwrap(), corrected_time);

    let mut daytime = include_bytes!("fixtures/exif-midnight.jpg").to_vec();
    let mut replaced = 0;
    while let Some(offset) = daytime.windows(8).position(|s| s == b"00:00:00") {
        daytime[offset..offset + 8].copy_from_slice(b"12:34:56");
        replaced += 1;
    }
    assert!(replaced > 0);
    fs::write(media.join("a.jpg"), daytime).unwrap();
    fs::write(&xmp, date_only).unwrap();
    let get = serval(&["xmp", "get", "media/a.jpg.xmp"], dir);
    let labels: serde_json::Value = serde_json::from_slice(&get.stdout).unwrap();
    assert_eq!(labels["labels"]["datetime"], "2024-10-19");
}

#[test]
fn runs_without_a_terminal_and_reports_decisions() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("work");
    fs::create_dir_all(dir.join("data/dep1")).unwrap();

    // capture: all answers as flags
    let media = dir.join("data/dep1/a.jpg");
    fs::write(&media, "jpeg").unwrap();
    fs::write(dir.join("data/dep1/a.jpg.xmp"), "<x:xmpmeta/>").unwrap();
    let csv = format!(
        "path,datetime,species,individual\n{p},2024-01-01 08:00:00,Fox,\n",
        p = media.display()
    );
    fs::write(dir.join("tags.csv"), &csv).unwrap();
    let out = serval(
        &[
            "capture",
            "tags.csv",
            "--min-gap",
            "30",
            "--measure-from",
            "last-record",
            "--by",
            "species",
            "--deployment-level",
            "2",
            "--event",
            "-o",
            "cap",
            "--progress",
            "json",
        ],
        &dir,
    );
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let ev = events(&out);
    assert_eq!(ev[0]["serval"], "hello");
    assert_eq!(ev[0]["protocol"], 1);
    assert!(ev.iter().any(|e| e["serval"] == "output"));
    // The time column is `datetime`, as in tags.csv: Caracal reads these tables by that name.
    for prefix in ["temporal-independence", "events"] {
        let table = fs::read_dir(dir.join("cap"))
            .unwrap()
            .map(|e| e.unwrap().path())
            .find(|p| p.file_name().unwrap().to_string_lossy().starts_with(prefix))
            .unwrap();
        let header = fs::read_to_string(&table).unwrap();
        let header = header
            .trim_start_matches('\u{feff}')
            .lines()
            .next()
            .unwrap()
            .to_string();
        assert!(
            header.split(',').any(|c| c == "datetime"),
            "{prefix}: {header}"
        );
    }

    // extract: a differing target without --on-existing stops with exit code 3, nothing written
    let extract = |extra: &[&str]| {
        let mut args = vec![
            "extract",
            "tags.csv",
            "-f",
            "species",
            "-v",
            "Fox",
            "--keep-dirs",
            "0",
        ];
        args.extend_from_slice(&["-o", "out", "--progress", "json"]);
        args.extend_from_slice(extra);
        serval(&args, &dir)
    };
    assert!(extract(&[]).status.success());
    fs::write(dir.join("out/a.jpg.xmp"), "edited").unwrap();
    let out = extract(&[]);
    assert_eq!(out.status.code(), Some(3));
    let needs = events(&out)
        .into_iter()
        .find(|e| e["serval"] == "needs")
        .unwrap();
    assert_eq!(needs["count"], 1);
    assert_eq!(needs["items"][0]["reasons"][0], "sidecar-differs");
    assert_eq!(
        fs::read_to_string(dir.join("out/a.jpg.xmp")).unwrap(),
        "edited"
    );

    // the same run with the decision as a flag
    assert!(extract(&["--on-existing", "replace"]).status.success());
    assert_eq!(
        fs::read_to_string(dir.join("out/a.jpg.xmp")).unwrap(),
        "<x:xmpmeta/>"
    );
}

#[test]
fn set_leaves_files_that_changed_since_shown_untouched() {
    let tmp = tempfile::tempdir().unwrap();
    let xmp = tmp.path().join("a.jpg.xmp");
    fs::write(
        &xmp,
        r#"<x:xmpmeta xmlns:x="adobe:ns:meta/"><rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">
        <rdf:Description rdf:about="" xmlns:lr="http://ns.adobe.com/lightroom/1.0/">
        <lr:hierarchicalSubject><rdf:Bag><rdf:li>Species|Fox</rdf:li></rdf:Bag></lr:hierarchicalSubject>
        </rdf:Description></rdf:RDF></x:xmpmeta>"#,
    )
    .unwrap();
    let set = |values: &str, expect: &str| {
        let mut child = Command::new(env!("CARGO_BIN_EXE_serval"))
            .args(["xmp", "set", "--backup", "first"])
            .current_dir(tmp.path())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let edit = serde_json::json!({
            "path": "a.jpg.xmp",
            "expect": {"species": serde_json::from_str::<serde_json::Value>(expect).unwrap()},
            "set": {"species": serde_json::from_str::<serde_json::Value>(values).unwrap()},
        });
        writeln!(child.stdin.take().unwrap(), "{edit}").unwrap();
        let out = child.wait_with_output().unwrap();
        assert!(out.status.success());
        serde_json::from_slice::<serde_json::Value>(&out.stdout).unwrap()
    };
    let backups = || {
        fs::read_dir(tmp.path())
            .unwrap()
            .filter(|e| e.as_ref().unwrap().path().extension().unwrap() == "backup")
            .count()
    };

    let original = fs::read_to_string(&xmp).unwrap();
    let result = set(r#"["Red fox"]"#, r#"["Wolf"]"#);
    assert_eq!(result["result"], "conflict");
    assert_eq!(result["labels"]["species"][0], "Fox");
    assert_eq!(fs::read_to_string(&xmp).unwrap(), original);

    assert_eq!(set(r#"["Red fox"]"#, r#"["Fox"]"#)["result"], "written");
    assert_eq!(set(r#"["Deer"]"#, r#"["Red fox"]"#)["result"], "written");
    // --backup first: only the original is kept
    assert_eq!(backups(), 1);
}

fn set_edits(dir: &Path, edits: &[serde_json::Value]) -> Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_serval"))
        .args(["xmp", "set", "--backup", "first"])
        .current_dir(dir)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdin = child.stdin.take().unwrap();
    for edit in edits {
        writeln!(stdin, "{edit}").unwrap();
    }
    drop(stdin);
    child.wait_with_output().unwrap()
}

fn empty_xmp(path: &Path) {
    fs::write(path, r#"<x:xmpmeta xmlns:x="adobe:ns:meta/"><rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"><rdf:Description rdf:about="" xmlns:exif="http://ns.adobe.com/exif/1.0/" exif:DateTimeOriginal="2023-12-08"/></rdf:RDF></x:xmpmeta>"#).unwrap();
}

#[test]
fn set_groups_relative_and_absolute_aliases_before_writing() {
    let tmp = tempfile::tempdir().unwrap();
    let xmp = tmp.path().join("a.jpg.xmp");
    empty_xmp(&xmp);
    let out = set_edits(
        tmp.path(),
        &[
            serde_json::json!({"path": "a.jpg.xmp", "set": {"species": ["Fox"]}}),
            serde_json::json!({"path": xmp, "set": {"rating": 3}}),
        ],
    );
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let states: Vec<serde_json::Value> = String::from_utf8(out.stdout)
        .unwrap()
        .lines()
        .map(|s| serde_json::from_str(s).unwrap())
        .collect();
    assert_eq!(states.len(), 1);
    assert_eq!(states[0]["result"], "written");
    assert_eq!(states[0]["labels"]["species"], serde_json::json!(["Fox"]));
    assert_eq!(states[0]["labels"]["rating"], 3);
    let out = serval(&["xmp", "get", "a.jpg.xmp"], tmp.path());
    let state: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(state["labels"], states[0]["labels"]);
}

#[test]
fn set_accepts_date_only_expectations_returned_by_get() {
    let tmp = tempfile::tempdir().unwrap();
    empty_xmp(&tmp.path().join("a.jpg.xmp"));
    let out = serval(&["xmp", "get", "a.jpg.xmp"], tmp.path());
    assert!(out.status.success());
    let state: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(state["labels"]["datetime"], "2023-12-08");
    let out = set_edits(
        tmp.path(),
        &[serde_json::json!({
            "path": "a.jpg.xmp", "expect": state["labels"], "set": {"species": ["Fox"]}
        })],
    );
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let state: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(state["result"], "written");
    assert_eq!(state["labels"]["datetime"], "2023-12-08");
    let out = set_edits(
        tmp.path(),
        &[serde_json::json!({
            "path": "a.jpg.xmp", "set": {"datetime": "2023-12-09"}
        })],
    );
    assert!(
        !out.status.success(),
        "new assignments must still require a time"
    );
}

#[test]
fn write_accepts_equivalent_integral_decimal_ratings() {
    let tmp = tempfile::tempdir().unwrap();
    empty_xmp(&tmp.path().join("a.jpg.xmp"));
    fs::write(
        tmp.path().join("tags.csv"),
        "path,species,rating\na.jpg,Fox,3.0\na.jpg,Wolf,3\n",
    )
    .unwrap();
    let out = serval(&["xmp", "write", "tags.csv"], tmp.path());
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let out = serval(&["xmp", "get", "a.jpg.xmp"], tmp.path());
    let state: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(state["labels"]["rating"], 3);
    assert_eq!(
        state["labels"]["species"],
        serde_json::json!(["Fox", "Wolf"])
    );
}

#[test]
fn invalid_ratings_stop_the_table_before_any_write() {
    let tmp = tempfile::tempdir().unwrap();
    let xmp = tmp.path().join("a.jpg.xmp");
    empty_xmp(&xmp);
    let original = fs::read(&xmp).unwrap();
    for rating in ["3.5", "NaN", "inf", "7", "bad"] {
        fs::write(
            tmp.path().join("tags.csv"),
            format!("path,species,rating\na.jpg,Fox,{rating}\n"),
        )
        .unwrap();
        let out = serval(&["xmp", "write", "tags.csv"], tmp.path());
        assert!(!out.status.success(), "accepted {rating}");
        assert_eq!(fs::read(&xmp).unwrap(), original);
    }
}

#[cfg(unix)]
#[test]
fn set_groups_symlink_aliases_without_replacing_the_link() {
    let tmp = tempfile::tempdir().unwrap();
    let xmp = tmp.path().join("a.jpg.xmp");
    let alias = tmp.path().join("alias.xmp");
    empty_xmp(&xmp);
    std::os::unix::fs::symlink(&xmp, &alias).unwrap();
    let out = set_edits(
        tmp.path(),
        &[
            serde_json::json!({"path": alias, "set": {"species": ["Fox"]}}),
            serde_json::json!({"path": xmp, "set": {"rating": 3}}),
        ],
    );
    assert!(out.status.success());
    let state: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(state["result"], "written");
    assert_eq!(state["labels"]["species"], serde_json::json!(["Fox"]));
    assert_eq!(state["labels"]["rating"], 3);
    assert!(fs::symlink_metadata(&alias).unwrap().is_symlink());
}

#[test]
fn extract_keeps_an_uppercase_sidecar_with_its_media() {
    let tmp = tempfile::tempdir().unwrap();
    let media = tmp.path().join("a.jpg");
    fs::write(&media, "jpeg").unwrap();
    fs::write(tmp.path().join("a.jpg.XMP"), "<x:xmpmeta/>").unwrap();
    fs::write(
        tmp.path().join("tags.csv"),
        format!("path,species\n{},Fox\n", media.display()),
    )
    .unwrap();
    let out = serval(
        &[
            "extract",
            "tags.csv",
            "-f",
            "species",
            "-v",
            "Fox",
            "--keep-dirs",
            "0",
            "-o",
            "out",
        ],
        tmp.path(),
    );
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(
        fs::read_to_string(tmp.path().join("out/a.jpg.XMP")).unwrap(),
        "<x:xmpmeta/>"
    );
}

#[test]
fn observe_on_a_missing_folder_is_an_error() {
    let tmp = tempfile::tempdir().unwrap();
    let output = serval(
        &["observe", "--progress", "json", "no-such-folder"],
        tmp.path(),
    );
    assert!(!output.status.success());
    assert!(events(&output).iter().any(|e| e["serval"] == "error"));
    assert!(
        !tmp.path().join("serval_output").exists(),
        "no empty tables are written"
    );
}

#[test]
fn extracts_whole_selected_events_with_sidecars_and_kept_folders() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    fs::create_dir(dir.join("camera")).unwrap();
    for file in ["a.jpg", "b.jpg", "c.jpg"] {
        fs::write(dir.join("camera").join(file), file).unwrap();
        fs::write(dir.join("camera").join(format!("{file}.xmp")), "labels").unwrap();
    }
    let rows = "path,event_id,species\ncamera/a.jpg,1,Fox\ncamera/b.jpg,1,Cat\ncamera/a.jpg,2,Pika\ncamera/a.jpg,2,Pika\ncamera/c.jpg,3,Pika\ncamera/c.jpg,,Fox\n";
    fs::write(dir.join("events.csv"), rows).unwrap();
    let out = serval(
        &[
            "extract",
            "events.csv",
            "--unit",
            "event",
            "-f",
            "species",
            "-v",
            "Fox",
            "-v",
            "Pika",
            "--keep-dirs",
            "1",
            "-o",
            "out",
        ],
        dir,
    );
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    for path in [
        "event_1/camera/a.jpg",
        "event_1/camera/b.jpg",
        "event_2/camera/a.jpg",
    ] {
        assert!(dir.join("out").join(path).is_file());
        assert!(dir.join("out").join(format!("{path}.xmp")).is_file());
    }
    assert_eq!(
        fs::read_dir(dir.join("out"))
            .unwrap()
            .filter(|e| e.as_ref().unwrap().path().is_dir())
            .count(),
        3
    );
    assert_eq!(
        fs::read_dir(dir.join("out/event_2/camera"))
            .unwrap()
            .count(),
        2
    );
    let pika = serval(
        &[
            "extract",
            "events.csv",
            "--unit",
            "event",
            "-f",
            "species",
            "-v",
            "Pika",
            "--keep-dirs",
            "0",
            "-o",
            "pika",
        ],
        dir,
    );
    assert!(pika.status.success());
    assert!(dir.join("pika/event_2/a.jpg").is_file());
    assert!(!dir.join("pika/event_1").exists());
    let invalid = serval(
        &[
            "extract",
            "events.csv",
            "--unit",
            "event",
            "--use-subdir",
            "-f",
            "species",
            "-v",
            "Fox",
            "-o",
            "invalid",
        ],
        dir,
    );
    assert!(!invalid.status.success());
    assert!(String::from_utf8_lossy(&invalid.stderr).contains("--use-subdir"));
    assert!(!dir.join("invalid/event_1").exists());
    let by_id = serval(
        &[
            "extract",
            "events.csv",
            "--unit",
            "event",
            "-f",
            "event",
            "-v",
            "2",
            "-v",
            "3",
            "--keep-dirs",
            "0",
            "-o",
            "by_id",
        ],
        dir,
    );
    assert!(
        by_id.status.success(),
        "{}",
        String::from_utf8_lossy(&by_id.stderr)
    );
    assert!(dir.join("by_id/event_2/a.jpg").is_file());
    assert!(dir.join("by_id/event_3/c.jpg.xmp").is_file());
    assert!(!dir.join("by_id/event_1").exists());
}

#[test]
fn observe_reads_wrapped_xml_declarations_in_both_encodings() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let xml = include_str!("../assets/dummy.xmp")
        .replace("<rdf:li>Dummy</rdf:li>", "<rdf:li>Species|藏狐</rdf:li>")
        .replacen("?>", "?>\n<?xml version=\"1.0\"?>", 1);
    fs::write(dir.join("utf8.jpg.xmp"), &xml).unwrap();
    for encoding in ["UTF-16", "UTF-16LE", "UTF-16BE"] {
        let declared = xml.replace(
            "version=\"1.0\"",
            &format!("version=\"1.0\" encoding=\"{encoding}\""),
        );
        let encode = if encoding == "UTF-16BE" {
            u16::to_be_bytes
        } else {
            u16::to_le_bytes
        };
        let bytes: Vec<_> = std::iter::once(0xfeffu16)
            .chain(declared.encode_utf16())
            .flat_map(encode)
            .collect();
        fs::write(dir.join(format!("{encoding}.jpg.xmp")), bytes).unwrap();
    }
    fs::write(
        dir.join("broken.jpg.xmp"),
        xml.split("</rdf:Description>").next().unwrap(),
    )
    .unwrap();
    fs::write(
        dir.join("list.csv"),
        "path\nutf8.jpg.xmp\nUTF-16.jpg.xmp\nUTF-16LE.jpg.xmp\nUTF-16BE.jpg.xmp\nbroken.jpg.xmp\n",
    )
    .unwrap();
    for list in [false, true] {
        let mut args = vec![
            "observe",
            ".",
            "-x",
            "--progress",
            "json",
            "-o",
            if list { "files" } else { "folder" },
        ];
        if list {
            args.extend(["--files-from", "list.csv"]);
        }
        let out = serval(&args, dir);
        assert!(out.status.success());
        assert!(
            events(&out)
                .iter()
                .any(|e| e["serval"] == "summary" && e["read"] == 4 && e["failed"] == 1),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
        let tags = events(&out)
            .into_iter()
            .find(|e| {
                e["serval"] == "output" && e["path"].as_str().is_some_and(|p| p.contains("tags_"))
            })
            .unwrap();
        let mut csv = csv::Reader::from_path(dir.join(tags["path"].as_str().unwrap())).unwrap();
        let datetime = csv
            .headers()
            .unwrap()
            .iter()
            .position(|h| h == "datetime")
            .unwrap();
        let species = csv
            .headers()
            .unwrap()
            .iter()
            .position(|h| h == "species")
            .unwrap();
        let rows: Vec<_> = csv.records().collect::<Result<_, _>>().unwrap();
        assert_eq!(rows.len(), 4);
        assert!(rows.iter().all(|r| &r[species] == "藏狐"));
        assert!(rows.iter().all(|r| &r[datetime] == "2023-12-19 22:19:50"));
    }
}

#[cfg(unix)]
#[test]
fn observe_skips_unreadable_sidecar_with_non_utf8_name() {
    use std::ffi::OsStr;
    use std::os::unix::ffi::OsStrExt;
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let broken = dir.join(OsStr::from_bytes(b"bad-\xff.jpg.xmp"));
    fs::write(&broken, "truncated").unwrap();
    fs::write(
        dir.join("good.jpg.xmp"),
        include_bytes!("../assets/dummy.xmp"),
    )
    .unwrap();
    for mode in ["terminal", "json"] {
        let out = serval(&["observe", ".", "-x", "--progress", mode, "-o", mode], dir);
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
        if mode == "json" {
            let events = events(&out);
            assert!(
                events.iter().any(|e| e["serval"] == "file_error"
                    && e["path"] == broken.to_string_lossy().as_ref())
            );
            assert!(
                events
                    .iter()
                    .any(|e| e["serval"] == "summary" && e["read"] == 1 && e["failed"] == 1)
            );
        }
        let table = fs::read_dir(dir.join(mode))
            .unwrap()
            .map(Result::unwrap)
            .find(|e| e.file_name().to_string_lossy().starts_with("tags_"))
            .unwrap();
        let mut csv = csv::Reader::from_path(table.path()).unwrap();
        let rows = csv.records().collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            Path::new(&rows[0][0]).file_name(),
            Some(OsStr::new("good.jpg.xmp"))
        );
    }
}

#[test]
fn observe_list_selects_and_deduplicates_csv_paths() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let missing_list = serval(&["observe", "--files-from", "missing/list.csv"], root);
    assert!(!missing_list.status.success());
    assert!(String::from_utf8_lossy(&missing_list.stderr).contains("list.csv"));
    let media = root.join("media");
    fs::create_dir(&media).unwrap();
    let sidecar = media.join("cat,1.jpg.xmp");
    fs::write(&sidecar, include_bytes!("../assets/dummy.xmp")).unwrap();
    fs::write(media.join("unlisted.jpg.xmp"), "malformed").unwrap();
    fs::write(
        media.join("list.csv"),
        "\u{feff}species,path\r\nFox,\"cat,1.jpg\"\r\nDog,\"cat,1.jpg.xmp\"\r\n,skip.mp4\r\n",
    )
    .unwrap();
    let out = serval(
        &[
            "observe",
            "--files-from",
            "media/list.csv",
            "-x",
            "--image",
            "-o",
            "out",
        ],
        root,
    );
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let table = fs::read_dir(root.join("out"))
        .unwrap()
        .flatten()
        .find(|e| e.file_name().to_string_lossy().starts_with("tags_"))
        .unwrap();
    let mut csv = csv::Reader::from_path(table.path()).unwrap();
    let records: Vec<_> = csv.records().collect::<Result<_, _>>().unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(&records[0][0], sidecar.to_str().unwrap());

    fs::write(root.join("list.csv"), "path\nunlisted.jpg.xmp\n").unwrap();
    let invalid = serval(
        &[
            "observe",
            "media",
            "--files-from",
            "list.csv",
            "-x",
            "-o",
            "bad",
            "--progress",
            "json",
        ],
        root,
    );
    assert!(invalid.status.success());
    let diagnostics = events(&invalid);
    assert!(diagnostics.iter().any(|e| e["serval"] == "file_error"
        && e["path"] == media.join("unlisted.jpg.xmp").to_str().unwrap()));
    assert!(
        diagnostics
            .iter()
            .any(|e| e["serval"] == "summary" && e["read"] == 0 && e["failed"] == 1)
    );
    let directory = serval(
        &[
            "observe",
            "media",
            "-x",
            "-o",
            "folder",
            "--progress",
            "json",
        ],
        root,
    );
    assert!(directory.status.success());
    assert!(
        events(&directory)
            .iter()
            .any(|e| e["serval"] == "summary" && e["read"] == 1 && e["failed"] == 1)
    );

    let missing_media = serval(
        &["observe", "media", "--files-from", "list.csv", "-o", "bad"],
        root,
    );
    assert!(missing_media.status.success());
    assert!(String::from_utf8_lossy(&missing_media.stderr).contains("unlisted.jpg"));

    fs::write(root.join("empty.csv"), "path\n").unwrap();
    let empty = serval(
        &["observe", "--files-from", "empty.csv", "-o", "empty"],
        root,
    );
    assert!(
        empty.status.success(),
        "{}",
        String::from_utf8_lossy(&empty.stderr)
    );
    let path = fs::read_dir(root.join("empty"))
        .unwrap()
        .flatten()
        .find(|e| e.file_name().to_string_lossy().starts_with("tags_"))
        .unwrap()
        .path();
    assert_eq!(fs::read_to_string(path).unwrap().lines().count(), 1);
}
