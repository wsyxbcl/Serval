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
fn xmp_edits_read_utf16_and_preserve_untouched_labels() {
    let dir = tempfile::tempdir().unwrap();
    let xmp = dir.path().join("good.jpg.xmp");
    let xml = include_str!("../assets/dummy.xmp")
        .replace("<rdf:li>Dummy</rdf:li>", "<rdf:li>Species|藏狐</rdf:li>")
        .replacen("?>", "?>\n<?xml version=\"1.0\" encoding=\"UTF-16\"?>", 1);
    let bytes: Vec<_> = std::iter::once(0xfeffu16)
        .chain(xml.encode_utf16())
        .flat_map(u16::to_le_bytes)
        .collect();
    fs::write(&xmp, bytes).unwrap();
    let observe = serval(
        &["observe", ".", "-x", "--progress", "json", "-o", "out"],
        dir.path(),
    );
    let table = events(&observe)
        .into_iter()
        .find(|e| {
            e["serval"] == "output" && e["path"].as_str().is_some_and(|p| p.contains("tags_"))
        })
        .unwrap();
    assert!(
        fs::read_to_string(dir.path().join(table["path"].as_str().unwrap()))
            .unwrap()
            .contains("藏狐")
    );
    let labels = || {
        let out = serval(&["xmp", "get", "good.jpg.xmp"], dir.path());
        serde_json::from_slice::<serde_json::Value>(&out.stdout).unwrap()["labels"].clone()
    };
    assert_eq!(labels()["species"], serde_json::json!(["藏狐"]));
    let out = set_edits(
        dir.path(),
        &[serde_json::json!({"path":xmp,"expect":{"species":["藏狐"]},"set":{"rating":4}})],
    );
    let state: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(state["result"], "written");
    let after = labels();
    assert_eq!(after["rating"], 4);
    assert_eq!(after["species"], serde_json::json!(["藏狐"]));
}

#[test]
fn xmp_edits_leave_truncated_sidecars_intact() {
    let dir = tempfile::tempdir().unwrap();
    let xmp = dir.path().join("broken.jpg.xmp");
    let truncated = include_str!("../assets/dummy.xmp")
        .split("</rdf:Description>")
        .next()
        .unwrap();
    fs::write(&xmp, truncated).unwrap();
    let out = serval(&["xmp", "get", "broken.jpg.xmp"], dir.path());
    let state: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert!(state["message"].is_string() && state["labels"].is_null());
    let out = set_edits(
        dir.path(),
        &[serde_json::json!({"path":xmp,"set":{"rating":4}})],
    );
    let state: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(state["result"], "error");
    assert_eq!(fs::read(&xmp).unwrap(), truncated.as_bytes());
}

#[test]
fn observe_list_selects_deduplicates_and_reports_failed_paths() {
    let dir = tempfile::tempdir().unwrap();
    let media = dir.path().join("media");
    fs::create_dir(&media).unwrap();
    fs::write(
        media.join("cat,1.jpg.xmp"),
        include_bytes!("../assets/dummy.xmp"),
    )
    .unwrap();
    fs::write(
        media.join("unlisted.jpg.xmp"),
        include_bytes!("../assets/dummy.xmp"),
    )
    .unwrap();
    fs::write(
        media.join("list.csv"),
        "\u{feff}species,path\nFox,\"cat,1.jpg\"\n,\"cat,1.jpg.xmp\"\n,missing.jpg\n,ignore.mp4\n",
    )
    .unwrap();
    let out = serval(
        &[
            "observe",
            "--files-from",
            "media/list.csv",
            "-x",
            "--image",
            "--progress",
            "json",
            "-o",
            "out",
        ],
        dir.path(),
    );
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let events = events(&out);
    assert!(events.iter().any(|e| e["serval"] == "file_error"
        && e["path"] == media.join("missing.jpg.xmp").to_str().unwrap()));
    assert!(
        events
            .iter()
            .any(|e| e["serval"] == "summary" && e["read"] == 1 && e["failed"] == 1)
    );
    let table = events
        .iter()
        .find_map(|e| {
            e["path"]
                .as_str()
                .filter(|p| e["serval"] == "output" && p.contains("tags_"))
        })
        .unwrap();
    let contents = fs::read_to_string(dir.path().join(table)).unwrap();
    assert!(
        contents
            .lines()
            .next()
            .unwrap()
            .ends_with(",read_status,read_error")
    );
    let rows: Vec<_> = contents.lines().skip(1).collect();
    assert_eq!(rows.len(), 2);
    assert!(rows[0].contains("cat,1.jpg.xmp") && rows[0].ends_with(",succeeded,"));
    assert!(rows[1].contains("missing.jpg.xmp") && rows[1].contains(",failed,"));
    assert!(!rows[1].ends_with(",failed,"));
    let stats = events
        .iter()
        .find_map(|e| e["path"].as_str().filter(|p| p.contains("species_stats_")))
        .unwrap();
    let stats = fs::read_to_string(dir.path().join(stats)).unwrap();
    assert!(stats.lines().skip(1).all(|line| line.ends_with(",1")));
}

#[cfg(unix)]
#[test]
fn observe_reports_non_utf8_read_failures() {
    use std::os::unix::ffi::OsStrExt;
    let dir = tempfile::tempdir().unwrap();
    // A symlink loop reliably produces a read error even when running as root.
    let path = dir
        .path()
        .join(std::ffi::OsStr::from_bytes(b"bad-\xff.jpg.xmp"));
    std::os::unix::fs::symlink(&path, &path).unwrap();
    for mode in ["terminal", "json"] {
        let out = serval(
            &["observe", ".", "-x", "--progress", mode, "-o", mode],
            dir.path(),
        );
        assert!(out.status.success());
        if mode == "json" {
            assert!(events(&out).iter().any(
                |e| e["serval"] == "file_error" && e["path"] == path.to_string_lossy().as_ref()
            ));
        }
    }
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
