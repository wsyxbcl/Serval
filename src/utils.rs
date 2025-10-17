use chrono::NaiveDateTime;
use core::fmt;
use indicatif::ProgressStyle;
use polars::prelude::*;
use rayon::prelude::*;
use std::collections::HashSet;
use std::ffi::{OsStr, OsString};
use std::fs::{File, FileTimes};
use std::io;
use std::str::FromStr;
use std::{
    env, fs,
    path::{Path, PathBuf},
};
use walkdir::{DirEntry, WalkDir};
use xmp_toolkit::{OpenFileOptions, XmpFile, XmpMeta};

#[derive(clap::ValueEnum, Clone, Copy, Debug)]
pub enum ResourceType {
    Xmp,
    Image,
    Video,
    Media, // Image or Video
    All,   // All resources (for serval align)
}

impl fmt::Display for ResourceType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl ResourceType {
    fn extension(self) -> Vec<&'static str> {
        match self {
            ResourceType::Image => vec!["jpg", "jpeg", "png"],
            ResourceType::Video => vec!["avi", "mp4", "mov"],
            ResourceType::Xmp => vec!["xmp"],
            ResourceType::Media => vec!["jpg", "jpeg", "png", "avi", "mp4", "mov"],
            ResourceType::All => vec!["jpg", "jpeg", "png", "avi", "mp4", "mov", "xmp"],
        }
    }

    fn is_resource(self, path: &Path) -> bool {
        let ext = match path.extension() {
            None => return false,
            Some(ext) => ext,
        };

        match ext.to_str() {
            None => false,
            Some(ext_str) => {
                let ext_lower = ext_str.to_ascii_lowercase();
                self.extension().contains(&ext_lower.as_str())
            }
        }
    }
}

#[derive(clap::ValueEnum, PartialEq, Clone, Copy, Debug)]
pub enum TagType {
    Species,
    Individual,
    Count,
    Sex,
    Bodypart,
}

impl fmt::Display for TagType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl TagType {
    pub fn col_name(self) -> &'static str {
        match self {
            TagType::Individual => "individual",
            TagType::Species => "species",
            TagType::Count => "count",
            TagType::Sex => "sex",
            TagType::Bodypart => "bodypart",
        }
    }
    pub fn digikam_tag_prefix(self) -> &'static str {
        match self {
            TagType::Individual => "Individual/",
            TagType::Species => "Species/",
            TagType::Count => "Count/",
            TagType::Sex => "Sex/",
            TagType::Bodypart => "Bodypart/",
        }
    }
    pub fn adobe_tag_prefix(self) -> &'static str {
        match self {
            TagType::Individual => "Individual|",
            TagType::Species => "Species|",
            TagType::Count => "Count|",
            TagType::Sex => "Sex|",
            TagType::Bodypart => "Bodypart|",
        }
    }
}

#[derive(clap::ValueEnum, Clone, Copy, Debug, PartialEq)]
pub enum ExtractFilterType {
    Species,
    Path,
    Individual,
    Rating,
    Event,
    Custom,
    Advanced,
}

#[derive(clap::ValueEnum, Clone, Copy, Debug)]
pub enum SubdirType {
    Species,
    Individual,
    Rating,
    Custom,
}

/// Represents a parsed filter condition
#[derive(Debug, Clone)]
pub struct FilterCondition {
    pub filter_type: ExtractFilterType,
    pub operator: FilterOperator,
    pub value: String,
}

/// Supported filter operators
#[derive(Debug, Clone)]
pub enum FilterOperator {
    Equal,           // exact match
    // Contains,        // TODO: substring match
    GreaterEqual,    // >=
    LessEqual,       // <=
    Greater,         // >
    Less,            // <
    Range(f64, f64), // min-max range
    // Not,             // TODO: negation wrapper
}

/// Logical operators for combining filters
#[derive(Debug, Clone)]
pub enum LogicalOperator {
    And,
    Or,
}

/// Complete filter expression tree
#[derive(Debug, Clone)]
pub enum FilterExpr {
    Condition(FilterCondition),
    Logical {
        left: Box<FilterExpr>,
        operator: LogicalOperator,
        right: Box<FilterExpr>,
    },
    // Not(Box<FilterExpr>), // TODO, need to consider the multiple-tag case
}

impl ExtractFilterType {
    /// Parse field aliases to filter types
    pub fn from_alias(alias: &str) -> Option<Self> {
        match alias.to_lowercase().as_str() {
            "species" | "sp" | "s" => Some(Self::Species),
            "individual" | "ind" | "i" => Some(Self::Individual),
            "rating" | "rate" | "r" => Some(Self::Rating),
            "path" | "p" => Some(Self::Path),
            "event" | "e" => Some(Self::Event),
            "custom" | "c" => Some(Self::Custom),
            _ => None,
        }
    }
}

/// Parse advanced filter string into FilterExpr
pub fn parse_advanced_filter(input: &str) -> anyhow::Result<FilterExpr> {
    let tokens = tokenize_filter(input)?;
    let expr = parse_expression(&tokens)?;
    Ok(expr)
}

/// Tokenize filter string
fn tokenize_filter(input: &str) -> anyhow::Result<Vec<String>> {
    let mut tokens = Vec::new();
    let mut current_token = String::new();
    let mut in_quotes = false;
    let mut chars = input.chars().peekable();

    while let Some(ch) = chars.next() {
        match ch {
            '"' | '\'' => {
                in_quotes = !in_quotes;
                current_token.push(ch);
            }
            ' ' | '\t' if !in_quotes => {
                if !current_token.is_empty() {
                    tokens.push(current_token.trim().to_string());
                    current_token.clear();
                }
            }
            '(' | ')' if !in_quotes => {
                if !current_token.is_empty() {
                    tokens.push(current_token.trim().to_string());
                    current_token.clear();
                }
                tokens.push(ch.to_string());
            }
            _ => {
                current_token.push(ch);
            }
        }
    }

    if !current_token.is_empty() {
        tokens.push(current_token.trim().to_string());
    }

    Ok(tokens)
}

/// Parse tokens into FilterExpr
fn parse_expression(tokens: &[String]) -> anyhow::Result<FilterExpr> {
    if tokens.is_empty() {
        return Err(anyhow::anyhow!("Empty filter expression"));
    }

    // For now, handle basic patterns like "field:value" and "field:value and field2:value2"

    if tokens.len() == 1 {
        // Single condition
        return parse_single_condition(&tokens[0]);
    }

    // Look for logical operators
    let mut i = 1;
    while i < tokens.len() {
        let token = &tokens[i];
        if token.eq_ignore_ascii_case("and") || token.eq_ignore_ascii_case("or") {
            let left_tokens = &tokens[0..i];
            let right_tokens = &tokens[i+1..];

            let left = parse_expression(left_tokens)?;
            let right = parse_expression(right_tokens)?;

            let operator = if token.eq_ignore_ascii_case("and") {
                LogicalOperator::And
            } else {
                LogicalOperator::Or
            };

            return Ok(FilterExpr::Logical {
                left: Box::new(left),
                operator,
                right: Box::new(right),
            });
        }
        i += 1;
    }

    // If no logical operators found, treat as single condition
    let combined = tokens.join(" ");
    parse_single_condition(&combined)
}

/// Parse a single condition like "species:fox" or "rating:3-5"
fn parse_single_condition(condition: &str) -> anyhow::Result<FilterExpr> {
    if let Some((field, value)) = condition.split_once(':') {
        let filter_type = ExtractFilterType::from_alias(field.trim())
            .ok_or_else(|| anyhow::anyhow!("Unknown filter field: {}", field))?;

        let (operator, cleaned_value) = parse_value_and_operator(value.trim())?;

        let condition = FilterCondition {
            filter_type,
            operator,
            value: cleaned_value,
        };

        Ok(FilterExpr::Condition(condition))
    } else {
        Err(anyhow::anyhow!("Invalid condition format: {}. Expected 'field:value'", condition))
    }
}

/// Parse value and detect operator (>=, <=, range, etc.)
fn parse_value_and_operator(value: &str) -> anyhow::Result<(FilterOperator, String)> {
    // Handle range syntax first (e.g., "1-5", "0.5-4.5")
    if let Some((min_str, max_str)) = value.split_once('-') {
        if let (Ok(min), Ok(max)) = (min_str.trim().parse::<f64>(), max_str.trim().parse::<f64>()) {
            return Ok((FilterOperator::Range(min, max), value.to_string()));
        }
    }

    // Handle comparison operators
    if value.starts_with(">=") {
        return Ok((FilterOperator::GreaterEqual, value[2..].trim().to_string()));
    }
    if value.starts_with("<=") {
        return Ok((FilterOperator::LessEqual, value[2..].trim().to_string()));
    }
    if value.starts_with('>') {
        return Ok((FilterOperator::Greater, value[1..].trim().to_string()));
    }
    if value.starts_with('<') {
        return Ok((FilterOperator::Less, value[1..].trim().to_string()));
    }

    // Remove quotes if present
    let cleaned_value = if (value.starts_with('"') && value.ends_with('"')) ||
                         (value.starts_with('\'') && value.ends_with('\'')) {
        value[1..value.len()-1].to_string()
    } else {
        value.to_string()
    };

    // Default to exact match for most fields, contains for path
    Ok((FilterOperator::Equal, cleaned_value))
}

pub fn has_same_field_and_conditions(expr: &FilterExpr) -> bool {
    fn collect_and_fields(expr: &FilterExpr, fields: &mut Vec<ExtractFilterType>) {
        match expr {
            FilterExpr::Condition(cond) => {
                fields.push(cond.filter_type);
            }
            FilterExpr::Logical { left, operator, right } => {
                match operator {
                    LogicalOperator::And => {
                        collect_and_fields(left, fields);
                        collect_and_fields(right, fields);
                    }
                    LogicalOperator::Or => {
                        // OR branches are separate, don't mix them
                    }
                }
            }
        }
    }

    let mut fields = Vec::new();
    collect_and_fields(expr, &mut fields);

    // Check if any field appears more than once in AND conditions
    for i in 0..fields.len() {
        for j in (i + 1)..fields.len() {
            if fields[i] == fields[j] {
                return true;
            }
        }
    }
    false
}

/// Convert FilterExpr to Polars Expr
///
/// # Parameters
/// * `expr` - The filter expression to convert
/// * `use_aggregated` - If true, treats species/individual as list columns (for path-level filtering)
pub fn filter_expr_to_polars(expr: &FilterExpr, use_aggregated: bool) -> anyhow::Result<Expr> {
    use crate::utils::{TagType};

    match expr {
        FilterExpr::Condition(condition) => {
            let col_name = match condition.filter_type {
                ExtractFilterType::Species => TagType::Species.col_name(),
                ExtractFilterType::Individual => TagType::Individual.col_name(),
                ExtractFilterType::Rating => "rating",
                ExtractFilterType::Path => "path",
                ExtractFilterType::Event => "event_id",
                ExtractFilterType::Custom => "custom",
                ExtractFilterType::Advanced => return Err(anyhow::anyhow!("Advanced filter should not appear in conditions")),
            };

            let base_col = col(col_name);

            match &condition.operator {
                FilterOperator::Equal => {
                    if condition.filter_type == ExtractFilterType::Path {
                        // Path uses contains for substring matching
                        Ok(base_col.str().contains_literal(lit(condition.value.clone())))
                    } else if use_aggregated
                            && (condition.filter_type == ExtractFilterType::Species
                                || condition.filter_type == ExtractFilterType::Individual) {
                        // For aggregated species/individual, check if list contains the value
                        Ok(base_col.list().contains(lit(condition.value.clone()), false))
                    } else {
                        Ok(base_col.eq(lit(condition.value.clone())))
                    }
                }
                FilterOperator::Range(min, max) => {
                    // Rating stays as scalar in both modes
                    let numeric_col = base_col.cast(DataType::Float64);
                    Ok(numeric_col.clone().is_not_null()
                        .and(numeric_col.clone().gt_eq(lit(*min)))
                        .and(numeric_col.lt_eq(lit(*max))))
                }
                FilterOperator::GreaterEqual => {
                    if let Ok(value) = condition.value.parse::<f64>() {
                        let numeric_col = base_col.cast(DataType::Float64);
                        Ok(numeric_col.clone().is_not_null().and(numeric_col.gt_eq(lit(value))))
                    } else {
                        Err(anyhow::anyhow!("GreaterEqual operator requires numeric value"))
                    }
                }
                FilterOperator::LessEqual => {
                    if let Ok(value) = condition.value.parse::<f64>() {
                        let numeric_col = base_col.cast(DataType::Float64);
                        Ok(numeric_col.clone().is_not_null().and(numeric_col.lt_eq(lit(value))))
                    } else {
                        Err(anyhow::anyhow!("LessEqual operator requires numeric value"))
                    }
                }
                FilterOperator::Greater => {
                    if let Ok(value) = condition.value.parse::<f64>() {
                        let numeric_col = base_col.cast(DataType::Float64);
                        Ok(numeric_col.clone().is_not_null().and(numeric_col.gt(lit(value))))
                    } else {
                        Err(anyhow::anyhow!("Greater operator requires numeric value"))
                    }
                }
                FilterOperator::Less => {
                    if let Ok(value) = condition.value.parse::<f64>() {
                        let numeric_col = base_col.cast(DataType::Float64);
                        Ok(numeric_col.clone().is_not_null().and(numeric_col.lt(lit(value))))
                    } else {
                        Err(anyhow::anyhow!("Less operator requires numeric value"))
                    }
                }
            }
        }
        FilterExpr::Logical { left, operator, right } => {
            let left_expr = filter_expr_to_polars(left, use_aggregated)?;
            let right_expr = filter_expr_to_polars(right, use_aggregated)?;

            match operator {
                LogicalOperator::And => Ok(left_expr.and(right_expr)),
                LogicalOperator::Or => Ok(left_expr.or(right_expr)),
            }
        }
    }
}

// Serval ignores
fn is_ignored(entry: &DirEntry) -> bool {
    entry
        .file_name()
        .to_str()
        .map(|s| s.starts_with('.') || s.contains("精选")) // ignore 精选 and .dtrash
        .unwrap_or(false)
}

// Serval bar style
pub fn serval_pb_style() -> ProgressStyle {
    ProgressStyle::default_bar()
        .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta})")
        .unwrap()
        .progress_chars("=> ")
}

// workaround for https://github.com/rust-lang/rust/issues/42869
// ref. https://github.com/sharkdp/fd/pull/72/files
fn path_to_absolute(path: PathBuf) -> io::Result<PathBuf> {
    if path.is_absolute() {
        return Ok(path);
    }
    let path = path.strip_prefix(".").unwrap_or(&path);
    env::current_dir().map(|current_dir| current_dir.join(path))
}

pub fn absolute_path(path: PathBuf) -> io::Result<PathBuf> {
    let path_buf = path_to_absolute(path)?;
    #[cfg(windows)]
    let path_buf = Path::new(
        path_buf
            .as_path()
            .to_string_lossy()
            .trim_start_matches(r"\\?\"),
    )
    .to_path_buf();
    Ok(path_buf)
}

pub fn path_enumerate(root_dir: PathBuf, resource_type: ResourceType) -> Vec<PathBuf> {
    WalkDir::new(root_dir)
        .into_iter()
        .filter_entry(|e| !is_ignored(e))
        .par_bridge()
        .filter_map(Result::ok)
        .filter(|e| resource_type.is_resource(e.path()))
        .map(|e| e.into_path())
        .collect()
}

pub fn resources_flatten(
    deploy_dir: PathBuf,
    working_dir: PathBuf,
    resource_type: ResourceType,
    dry_run: bool,
    move_mode: bool,
) -> anyhow::Result<()> {
    let deploy_id = deploy_dir
        .file_name()
        .ok_or_else(|| anyhow::anyhow!("Invalid deploy directory path: no filename"))?;
    let deploy_path = deploy_dir.to_str();

    let output_dir = working_dir.join(deploy_id);
    fs::create_dir_all(output_dir.clone())?;

    let resource_paths = path_enumerate(deploy_dir.clone(), resource_type);
    let num_resource = resource_paths.len();
    println!(
        "{} {}(s) found in {}",
        num_resource,
        resource_type,
        deploy_dir.to_string_lossy()
    );

    let mut visited_path: HashSet<String> = HashSet::new();
    let pb = if !dry_run {
        Some(indicatif::ProgressBar::new(num_resource as u64))
    } else {
        None
    };
    if let Some(pb_ref) = &pb {
        pb_ref.set_style(serval_pb_style());
    }
    for resource in resource_paths {
        let mut output_path = PathBuf::new();
        let resource_parent = resource.parent().unwrap();
        // Collect parent directory names by traversing up
        let mut parent_names: Vec<OsString> = Vec::new();
        let mut current_parent = resource.parent();
        while let Some(parent) = current_parent {
            if parent.to_str() == deploy_path {
                break;
            }
            if let Some(file_name) = parent.file_name() {
                parent_names.push(file_name.to_os_string());
            }
            current_parent = parent.parent();
        }

        parent_names.reverse();
        let mut name_parts = Vec::with_capacity(parent_names.len() + 2);
        name_parts.push(deploy_id.to_os_string());
        name_parts.extend(parent_names);
        if let Some(file_name) = resource.file_name() {
            name_parts.push(file_name.to_os_string());
        } else {
            name_parts.push("unnamed_file".into());
        }
        let resource_name = name_parts.join(std::ffi::OsStr::new("-"));

        output_path.push(output_dir.join(resource_name));

        if !dry_run {
            if move_mode {
                fs::rename(resource, output_path)?;
            } else {
                fs::copy(resource, output_path)?;
            }
            if let Some(pb_ref) = &pb {
                pb_ref.inc(1);
            }
        } else if !visited_path.contains(resource_parent.to_string_lossy().as_ref()) {
            visited_path.insert(resource_parent.to_string_lossy().to_string());
            println!(
                "DRYRUN sample: From {} to {}",
                resource.display(),
                output_path.display()
            );
        }
    }
    if let Some(pb_ref) = pb {
        pb_ref.finish();
    }
    Ok(())
}

pub fn deployments_align(
    project_dir: PathBuf,
    output_dir: PathBuf,
    deploy_table: PathBuf,
    resource_type: ResourceType,
    dry_run: bool,
    move_mode: bool,
) -> anyhow::Result<()> {
    let deploy_df = CsvReadOptions::default()
        .try_into_reader_with_file_path(Some(deploy_table))?
        .finish()?;
    let deploy_array = deploy_df["deploymentID"].str()?;

    let deploy_iter = deploy_array.into_iter();
    let num_iter = deploy_iter.len();
    let pb = indicatif::ProgressBar::new(num_iter as u64);
    pb.set_style(serval_pb_style());
    for deploy_id in deploy_iter {
        let (_, collection_name) = deploy_id.unwrap().rsplit_once('_').unwrap();
        let deploy_dir = project_dir.join(collection_name).join(deploy_id.unwrap());
        let collection_output_dir = output_dir.join(collection_name);
        resources_flatten(
            deploy_dir,
            collection_output_dir.clone(),
            resource_type,
            dry_run,
            move_mode,
        )?;
        pb.inc(1);
    }
    pb.finish();
    Ok(())
}

pub fn deployments_rename(project_dir: PathBuf, dry_run: bool) -> anyhow::Result<()> {
    // rename deployment path name to <deployment_name>_<collection_name>
    let mut count = 0;
    for entry in project_dir.read_dir()? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            let mut collection_dir = path;
            let original_collection_name = collection_dir
                .file_name()
                .and_then(|name| name.to_str())
                .ok_or_else(|| anyhow::anyhow!("Invalid collection directory name"))?;
            let collection_name_lower = original_collection_name.to_lowercase();
            if original_collection_name != collection_name_lower {
                let mut new_collection_dir = collection_dir.clone();
                new_collection_dir.set_file_name(&collection_name_lower);
                if dry_run {
                    println!(
                        "Will rename collection {original_collection_name} to {collection_name_lower}"
                    );
                } else {
                    println!(
                        "Renaming collection {} to {}",
                        collection_dir.display(),
                        new_collection_dir.display()
                    );
                    fs::rename(&collection_dir, &new_collection_dir)?;
                    collection_dir = new_collection_dir;
                }
            }
            let collection_name = collection_dir
                .file_name()
                .and_then(|name| name.to_str())
                .ok_or_else(|| anyhow::anyhow!("Invalid collection directory name"))?;
            for deploy in collection_dir.read_dir()? {
                let deploy_dir = deploy?.path();
                if deploy_dir.is_file() {
                    continue;
                }
                count += 1;
                let deploy_name = deploy_dir
                    .file_name()
                    .and_then(|name| name.to_str())
                    .ok_or_else(|| anyhow::anyhow!("Invalid deploy directory name"))?;
                if !deploy_name.contains(collection_name) {
                    if dry_run {
                        println!(
                            "Will rename {} to {}_{}",
                            deploy_name,
                            deploy_name.to_lowercase(),
                            collection_name.to_lowercase()
                        );
                    } else {
                        let mut deploy_id_dir = deploy_dir.clone();
                        deploy_id_dir.set_file_name(format!(
                            "{}_{}",
                            deploy_name.to_lowercase(),
                            collection_name.to_lowercase()
                        ));
                        println!(
                            "Renaming {} to {}",
                            deploy_dir.display(),
                            deploy_id_dir.display()
                        );
                        fs::rename(deploy_dir, deploy_id_dir)?;
                    }
                }
            }
        }
    }
    println!("Total directories: {count}");
    Ok(())
}

// copy xmp files to output_dir and keep the directory structure
pub fn copy_xmp(source_dir: PathBuf, output_dir: PathBuf) -> anyhow::Result<()> {
    let xmp_paths = path_enumerate(source_dir.clone(), ResourceType::Xmp);
    let num_xmp = xmp_paths.len();
    println!("{num_xmp} xmp files found");
    let pb = indicatif::ProgressBar::new(num_xmp as u64);
    pb.set_style(serval_pb_style());

    for xmp in xmp_paths {
        let mut output_path = output_dir.clone();
        let relative_path = xmp.strip_prefix(&source_dir).unwrap();
        output_path.push(relative_path);
        fs::create_dir_all(output_path.parent().unwrap())?;
        fs::copy(xmp, output_path)?;
        pb.inc(1);
    }
    pb.finish();
    Ok(())
}

// Sync XMP metadata to corresponding media files
pub fn sync_xmp_to_media(xmp_path: &Path) -> anyhow::Result<()> {
    let media_path_str = match xmp_path.to_str() {
        Some(path_str) => path_str.trim_end_matches(".xmp"),
        None => {
            eprintln!(
                "Warning: Skipping XMP file with non-UTF-8 path: {}",
                xmp_path.display()
            );
            return Ok(());
        }
    };
    let media_path = Path::new(media_path_str);

    if !media_path.exists() {
        eprintln!(
            "Warning: Skipping,'{}' does not exist.",
            media_path.display()
        );
        return Ok(());
    }

    let xmp_content = fs::read_to_string(xmp_path)?;
    let xmp_meta = XmpMeta::from_str(&xmp_content)?;

    let mut xmp_file = XmpFile::new()?;
    let open_options = OpenFileOptions::default().for_update();
    xmp_file.open_file(media_path, open_options)?;
    xmp_file.put_xmp(&xmp_meta)?;
    xmp_file.try_close()?;

    Ok(())
}

pub fn sync_xmp_directory(source_dir: PathBuf) -> anyhow::Result<()> {
    let xmp_paths = path_enumerate(source_dir.clone(), ResourceType::Xmp);
    let num_xmp = xmp_paths.len();

    if num_xmp == 0 {
        println!("No XMP files found in {}", source_dir.display());
        return Ok(());
    }

    println!(
        "Found {} XMP files to sync in {}",
        num_xmp,
        source_dir.display()
    );

    let pb = indicatif::ProgressBar::new(num_xmp as u64);
    pb.set_style(serval_pb_style());
    pb.set_message("Syncing XMP metadata to media files...");

    let results: Vec<anyhow::Result<()>> = xmp_paths
        .par_iter()
        .map(|xmp_path| {
            let result = sync_xmp_to_media(xmp_path);
            pb.inc(1);
            result
        })
        .collect();

    pb.finish();

    let (successes, failures): (Vec<_>, Vec<_>) = results.into_iter().partition(Result::is_ok);

    let num_synced = successes.len();
    let num_skipped = failures.len();

    for result in failures {
        if let Err(e) = result {
            eprintln!("Failed to sync: {e}");
        }
    }

    println!("Successfully synced {num_synced} XMP files, skipped {num_skipped} files");

    Ok(())
}

pub fn sync_xmp_from_csv(csv_path: PathBuf) -> anyhow::Result<()> {
    let df = CsvReadOptions::default()
        .with_has_header(true)
        .with_ignore_errors(false)
        .try_into_reader_with_file_path(Some(csv_path))?
        .finish()?;

    let df_filtered = df
        .lazy()
        .filter(col("path").is_not_null())
        .filter(col("path").str().ends_with(lit(".xmp")))
        .unique(
            Some(cols(vec!["path".to_string()])),
            UniqueKeepStrategy::First,
        )
        .collect()?;

    let num_files = df_filtered.height();
    if num_files == 0 {
        println!("No XMP files found in CSV");
        return Ok(());
    }

    println!("Found {num_files} XMP files in CSV to sync");

    let pb = indicatif::ProgressBar::new(num_files as u64);
    pb.set_style(serval_pb_style());
    pb.set_message("Syncing XMP files in CSV...");

    let path_col = df_filtered.column("path")?.str()?;

    let results: Vec<anyhow::Result<()>> = path_col
        .par_iter()
        .filter_map(|path| path.map(PathBuf::from))
        .map(|xmp_path| {
            let result = sync_xmp_to_media(&xmp_path);
            pb.inc(1);
            result
        })
        .collect();

    pb.finish();

    let (successes, failures): (Vec<_>, Vec<_>) = results.into_iter().partition(Result::is_ok);

    let num_synced = successes.len();
    let num_skipped = failures.len();

    for result in failures {
        if let Err(e) = result {
            eprintln!("Failed to sync: {e}");
        }
    }

    println!("Successfully synced {num_synced} XMP files, skipped {num_skipped} files");

    Ok(())
}

// Remove all XMP files recursively from a directory
pub fn remove_xmp_files(source_dir: PathBuf) -> anyhow::Result<()> {
    let xmp_paths = path_enumerate(source_dir.clone(), ResourceType::Xmp);
    let num_xmp = xmp_paths.len();

    if num_xmp == 0 {
        println!("No XMP files found in {}", source_dir.display());
        return Ok(());
    }

    println!("Found {} XMP files in {}", num_xmp, source_dir.display());

    let pb = indicatif::ProgressBar::new(num_xmp as u64);
    pb.set_style(serval_pb_style());
    pb.set_message("Removing XMP files...");

    let results: Vec<anyhow::Result<()>> = xmp_paths
        .par_iter()
        .map(|xmp_path| {
            let result = fs::remove_file(xmp_path);
            pb.inc(1);
            result.map_err(|e| anyhow::anyhow!("Failed to remove {}: {}", xmp_path.display(), e))
        })
        .collect();

    pb.finish();

    let (successes, failures): (Vec<_>, Vec<_>) = results.into_iter().partition(Result::is_ok);

    let num_removed = successes.len();
    let num_failed = failures.len();

    for result in failures {
        if let Err(e) = result {
            eprintln!("{e}");
        }
    }

    println!("Successfully removed {num_removed} XMP files, failed to remove {num_failed} files");
    Ok(())
}

pub fn is_temporal_independent(
    time_ref: String,
    time: String,
    min_delta_time: i32,
) -> anyhow::Result<bool> {
    // TODO Timezone
    let dt_ref = NaiveDateTime::parse_from_str(time_ref.as_str(), "%Y-%m-%d %H:%M:%S")
        .map_err(|e| anyhow::anyhow!("Failed to parse reference datetime '{}': {}", time_ref, e))?;
    let dt = NaiveDateTime::parse_from_str(time.as_str(), "%Y-%m-%d %H:%M:%S")
        .map_err(|e| anyhow::anyhow!("Failed to parse datetime '{}': {}", time, e))?;
    let diff = dt - dt_ref;

    Ok(diff
        >= chrono::Duration::try_minutes(min_delta_time.into())
            .ok_or_else(|| anyhow::anyhow!("Invalid minute value: {}", min_delta_time))?)
}

pub fn get_path_levels(path: String) -> Vec<String> {
    // Abandoned for performance
    // let normalized_path = PathBuf::from(path.replace('\\', "/"));
    // let levels: Vec<String> = normalized_path
    //     .components()
    //     .filter_map(|comp| match comp {
    //         Component::Normal(part) => Some(part.to_string_lossy().into_owned()),
    //         Component::Prefix(prefix) => Some(prefix.as_os_str().to_string_lossy().into_owned()), // For windows path prefixes
    //         _ => None, // Skip root and other components
    //     })
    //     .collect();

    let normalized_path = path.replace('\\', "/");
    let levels: Vec<String> = normalized_path
        .split('/')
        .map(|comp| comp.to_string())
        .collect();
    levels[1..levels.len() - 1].to_vec()
}

pub fn ignore_timezone(time: String) -> anyhow::Result<String> {
    let time_remove_designator = time.replace('Z', "");
    let time_ignore_zone = time_remove_designator.split('+').collect::<Vec<&str>>()[0];
    Ok(time_ignore_zone.to_string())
}

pub fn append_ext(ext: impl AsRef<OsStr>, path: PathBuf) -> anyhow::Result<PathBuf> {
    let mut os_string: OsString = path.into();
    os_string.push(".");
    os_string.push(ext.as_ref());
    Ok(os_string.into())
}

pub fn sync_modified_time(source: PathBuf, target: PathBuf) -> anyhow::Result<()> {
    let src = fs::metadata(source)?;
    let dest = File::options().write(true).open(target)?;
    let times = FileTimes::new()
        .set_accessed(src.accessed()?)
        .set_modified(src.modified()?);
    dest.set_times(times)?;
    Ok(())
}

pub fn tags_csv_translate(
    source_csv: PathBuf,
    taglist_csv: PathBuf,
    output_dir: PathBuf,
    from: &str,
    to: &str,
) -> anyhow::Result<()> {
    let source_df = CsvReadOptions::default()
        .try_into_reader_with_file_path(Some(source_csv.clone()))?
        .finish()?;
    let taglist_df = CsvReadOptions::default()
        .try_into_reader_with_file_path(Some(taglist_csv))?
        .finish()?;

    let mut result = source_df
        .clone()
        .lazy()
        .join(
            taglist_df.clone().lazy(),
            [col(TagType::Species.col_name())],
            [col(from)],
            JoinArgs::new(JoinType::Left),
        )
        .drop(cols([TagType::Species.col_name()]))
        .rename(vec![to], vec![TagType::Species.col_name()], true)
        // .with_column(col(to).alias("species"))
        .collect()?;

    let output_csv = output_dir.join(format!(
        "{}_translated.csv",
        source_csv
            .file_stem()
            .and_then(|stem| stem.to_str())
            .unwrap_or("tags")
    ));
    fs::create_dir_all(output_dir.clone())?;
    let mut file = std::fs::File::create(&output_csv)?;
    CsvWriter::new(&mut file)
        .include_bom(true)
        .finish(&mut result)?;

    println!("Saved to {}", output_csv.display());
    Ok(())
}
