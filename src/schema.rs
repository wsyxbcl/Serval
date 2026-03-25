use anyhow::anyhow;
use polars::prelude::*;
use std::path::{Path, PathBuf};

pub const CANONICAL_TAGS_HEADER: &[&str] = &[
    "path",
    "filename",
    "media_type",
    "datetime",
    "species",
    "individual",
    "count",
    "sex",
    "bodypart",
    "rating",
    "custom",
    "xmp_update",
    "xmp_update_datetime",
];

pub const LEGACY_DATETIME_COLUMN: &str = "datetime_original";

pub fn infer_media_type(path: &Path) -> anyhow::Result<&'static str> {
    let media_path = path_for_media_type_inference(path);
    let extension = media_path
        .extension()
        .and_then(|ext| ext.to_str())
        .ok_or_else(|| anyhow!("Cannot infer media_type from path without media extension: {}", path.display()))?
        .to_ascii_lowercase();

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

fn path_for_media_type_inference(path: &Path) -> PathBuf {
    if path
        .extension()
        .and_then(|ext| ext.to_str())
        .is_some_and(|ext| ext.eq_ignore_ascii_case("xmp"))
    {
        let mut media_path = path.to_path_buf();
        media_path.set_extension("");
        media_path
    } else {
        path.to_path_buf()
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
