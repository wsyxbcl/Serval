use polars::prelude::*;

pub const CANONICAL_TAGS_HEADER: &[&str] = &[
    "path",
    "filename",
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
