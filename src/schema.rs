use anyhow::anyhow;
use polars::prelude::*;
use std::path::{Path, PathBuf};

pub const PATH_COLUMN: &str = "path";
pub const FILENAME_COLUMN: &str = "filename";
pub const MEDIA_TYPE_COLUMN: &str = "media_type";
pub const DATETIME_COLUMN: &str = "datetime";
pub const SPECIES_COLUMN: &str = "species";
pub const INDIVIDUAL_COLUMN: &str = "individual";
pub const COUNT_COLUMN: &str = "count";
pub const SEX_COLUMN: &str = "sex";
pub const BODYPART_COLUMN: &str = "bodypart";
pub const RATING_COLUMN: &str = "rating";
pub const CUSTOM_COLUMN: &str = "custom";
pub const XMP_UPDATE_COLUMN: &str = "xmp_update";
pub const XMP_UPDATE_DATETIME_COLUMN: &str = "xmp_update_datetime";
pub const SUBJECTS_COLUMN: &str = "subjects";
pub const TIME_MODIFIED_COLUMN: &str = "time_modified";
pub const EVENT_ID_COLUMN: &str = "event_id";
pub const DEPLOYMENT_ID_COLUMN: &str = "deploymentID";
pub const CANONICAL_TAGS_HEADER: &[&str] = &[
    PATH_COLUMN,
    FILENAME_COLUMN,
    MEDIA_TYPE_COLUMN,
    DATETIME_COLUMN,
    SPECIES_COLUMN,
    INDIVIDUAL_COLUMN,
    COUNT_COLUMN,
    SEX_COLUMN,
    BODYPART_COLUMN,
    RATING_COLUMN,
    CUSTOM_COLUMN,
    XMP_UPDATE_COLUMN,
    XMP_UPDATE_DATETIME_COLUMN,
];

pub const LEGACY_DATETIME_COLUMN: &str = "datetime_original";
pub const IMAGE_EXTENSIONS: &[&str] = &["jpg", "jpeg", "png"];
pub const VIDEO_EXTENSIONS: &[&str] = &["avi", "mp4", "mov"];
pub const XMP_EXTENSIONS: &[&str] = &["xmp"];
pub const MEDIA_EXTENSIONS: &[&str] = &["jpg", "jpeg", "png", "avi", "mp4", "mov"];
pub const ALL_RESOURCE_EXTENSIONS: &[&str] = &["jpg", "jpeg", "png", "avi", "mp4", "mov", "xmp"];

pub fn resource_extension(path: &Path) -> Option<String> {
    path.extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| ext.to_ascii_lowercase())
}

pub fn underlying_media_path(path: &Path) -> PathBuf {
    if resource_extension(path).is_some_and(|ext| XMP_EXTENSIONS.contains(&ext.as_str())) {
        let mut media_path = path.to_path_buf();
        media_path.set_extension("");
        media_path
    } else {
        path.to_path_buf()
    }
}

pub fn media_extension(path: &Path) -> Option<String> {
    resource_extension(&underlying_media_path(path))
}

pub fn infer_media_type(path: &Path) -> anyhow::Result<&'static str> {
    let extension = media_extension(path).ok_or_else(|| {
        anyhow!(
            "Cannot infer media_type from path without media extension: {}",
            path.display()
        )
    })?;

    match extension.as_str() {
        "jpg" | "jpeg" => Ok("image/jpeg"),
        "png" => Ok("image/png"),
        "mp4" => Ok("video/mp4"),
        "mov" => Ok("video/quicktime"),
        "avi" => Ok("video/x-msvideo"),
        _ => Err(anyhow!(
            "Unsupported media extension for media_type inference: {}",
            path.display()
        )),
    }
}

pub fn canonicalize_observe_tags_df(df: DataFrame) -> PolarsResult<DataFrame> {
    let column_names = df.get_column_names_str();
    let missing_columns = CANONICAL_TAGS_HEADER
        .iter()
        .filter(|col| !column_names.contains(col))
        .map(|col| lit("").alias(*col))
        .collect::<Vec<_>>();

    let mut df_lazy = df.lazy();
    if !missing_columns.is_empty() {
        df_lazy = df_lazy.with_columns(missing_columns);
    }

    df_lazy
        .select(
            CANONICAL_TAGS_HEADER
                .iter()
                .map(|name| col(*name))
                .collect::<Vec<_>>(),
        )
        .collect()
}
