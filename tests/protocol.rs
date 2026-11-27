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
    let rows = "path,event_id,species\ncamera/a.jpg,1,Fox\ncamera/b.jpg,1,Cat\ncamera/a.jpg,2,Fox\ncamera/a.jpg,2,Fox\ncamera/c.jpg,3,Pika\ncamera/c.jpg,,Fox\n";
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
        2
    );
    assert_eq!(
        fs::read_dir(dir.join("out/event_2/camera"))
            .unwrap()
            .count(),
        2
    );
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
fn observe_list_selects_and_deduplicates_csv_paths() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let media = root.join("media");
    fs::create_dir(&media).unwrap();
    let sidecar = media.join("cat,1.jpg.XMP");
    fs::write(&sidecar, include_bytes!("../assets/dummy.xmp")).unwrap();
    fs::write(media.join("unlisted.jpg.xmp"), "malformed").unwrap();
    fs::write(
        media.join("list.csv"),
        "\u{feff}species,path\r\nFox,\"cat,1.jpg\"\r\nDog,\"cat,1.jpg.XMP\"\r\n,skip.mp4\r\n",
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
        ],
        root,
    );
    assert!(!invalid.status.success());
    assert!(String::from_utf8_lossy(&invalid.stderr).contains("unlisted.jpg.xmp"));

    let missing_media = serval(
        &["observe", "media", "--files-from", "list.csv", "-o", "bad"],
        root,
    );
    assert!(!missing_media.status.success());
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
