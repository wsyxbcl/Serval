//! Messages of `xmp get` and `xmp set`, one JSON object per line. Waxbill uses the same types; field names and
//! meanings are the protocol (Waxbill's Design/spec/serval-protocol.md §5.2). Keep this module free of Serval's
//! other code so it can be shared as it is.

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// A file's labels as they are on disk. Readers ignore fields they do not know, so newer versions can add some.
#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq)]
pub struct Labels {
    pub species: Vec<String>,
    pub individuals: Vec<String>,
    /// -1 (rejected) to 5; 0 is no rating
    pub rating: i32,
    /// `yyyy-MM-dd HH:mm:ss`, local time without an offset (as observe reads it); left out when the file has none
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub datetime: Option<String>,
}

/// Labels in an edit: a field left out is neither checked nor changed. Unknown fields are refused, so an older
/// Serval never ignores an edit it does not understand.
#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct EditLabels {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub species: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub individuals: Option<Vec<String>>,
    /// -1 (rejected) to 5; 0 is no rating
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rating: Option<i32>,
    /// `yyyy-MM-dd HH:mm:ss`, local time without an offset (as observe reads it)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub datetime: Option<String>,
}

/// Species or individuals to add to or remove from a file's lists.
#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct TagChanges {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub species: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub individuals: Vec<String>,
}

/// One line of `xmp set` input: edits of one file, applied as `set`, then `add`, then `remove`. Several lines for
/// the same file are applied in order and written once.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct FileEdit {
    /// The sidecar, or the media file (meaning its sidecar)
    pub path: PathBuf,
    /// What Waxbill showed. If a given field no longer matches the file (lists compare as sets), nothing in the
    /// file is changed and the result is `conflict`.
    #[serde(default)]
    pub expect: EditLabels,
    /// Fields that become exactly these values
    #[serde(default)]
    pub set: EditLabels,
    #[serde(default)]
    pub add: TagChanges,
    #[serde(default)]
    pub remove: TagChanges,
}

/// What `xmp set` did with a file.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Outcome {
    Written,
    Unchanged,
    Conflict,
    Error,
}

/// One line of `xmp get` or `xmp set` output: a file's labels as they are on disk now.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct FileState {
    /// The sidecar
    pub path: PathBuf,
    /// `xmp set` only
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<Outcome>,
    pub exists: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub labels: Option<Labels>,
    /// The sidecar's modification time, in milliseconds since the Unix epoch
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub modified: Option<u64>,
    /// Why a file is a conflict or an error
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}
