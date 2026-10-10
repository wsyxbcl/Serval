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
        let edit = format!(
            r#"{{"path":"{}","expect":{{"species":{expect}}},"set":{{"species":{values}}}}}"#,
            xmp.display()
        );
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
