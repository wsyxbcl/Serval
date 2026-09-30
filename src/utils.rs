use crate::schema::{
    ALL_RESOURCE_EXTENSIONS, CUSTOM_COLUMN, DEPLOYMENT_ID_COLUMN, EVENT_ID_COLUMN,
    IMAGE_EXTENSIONS, PATH_COLUMN, RATING_COLUMN, VIDEO_EXTENSIONS, XMP_EXTENSIONS,
    resource_extension, underlying_media_path,
};
use crate::transfer::{Mode, Transfer, run_transfers};
use core::fmt;
use indicatif::{ProgressBar, ProgressStyle};
use pest_derive::Parser;
use polars::prelude::*;
use rayon::prelude::*;
use std::collections::{HashMap, HashSet};
use std::ffi::OsString;
use std::fs::File;
use std::io;
use std::str::FromStr;
use std::{
    env, fs,
    path::{Path, PathBuf},
    sync::Arc,
};
use walkdir::{DirEntry, WalkDir};
use xmp_toolkit::{OpenFileOptions, XmpFile, XmpMeta};

pub fn csv_projection_columns(names: &[&str]) -> Option<Arc<[PlSmallStr]>> {
    Some(Arc::from(
        names
            .iter()
            .map(|name| PlSmallStr::from(*name))
            .collect::<Vec<_>>()
            .into_boxed_slice(),
    ))
}

pub fn reject_duplicate_csv_columns(df: &DataFrame) -> anyhow::Result<()> {
    if df
        .get_column_names()
        .iter()
        .any(|name| name.as_str().contains("_duplicated_"))
    {
        return Err(anyhow::anyhow!(
            "Duplicated CSV columns detected. Please check the input CSV header."
        ));
    }

    Ok(())
}

#[derive(Parser)]
#[grammar = "filter.pest"]
struct FilterParser;

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
    fn extension(self) -> &'static [&'static str] {
        match self {
            ResourceType::Image => IMAGE_EXTENSIONS,
            ResourceType::Video => VIDEO_EXTENSIONS,
            ResourceType::Xmp => XMP_EXTENSIONS,
            ResourceType::Media => crate::schema::MEDIA_EXTENSIONS,
            ResourceType::All => ALL_RESOURCE_EXTENSIONS,
        }
    }

    fn is_resource(self, path: &Path) -> bool {
        resource_extension(path).is_some_and(|ext| self.extension().contains(&ext.as_str()))
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

#[derive(clap::ValueEnum, PartialEq, Clone, Copy, Debug)]
pub enum XmpUpdateType {
    Species,
    Individual,
    Rating,
}

impl fmt::Display for XmpUpdateType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl XmpUpdateType {
    pub fn col_name(self) -> &'static str {
        match self {
            Self::Species => TagType::Species.col_name(),
            Self::Individual => TagType::Individual.col_name(),
            Self::Rating => RATING_COLUMN,
        }
    }

    pub fn tag_type(self) -> Option<TagType> {
        match self {
            Self::Species => Some(TagType::Species),
            Self::Individual => Some(TagType::Individual),
            Self::Rating => None,
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
    Equal, // exact match
    // Contains,        // TODO: substring match
    GreaterEqual, // >=
    LessEqual,    // <=
    Greater,      // >
    Less,         // <
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

/// Parse advanced filter string into FilterExpr using pest
pub fn parse_advanced_filter(input: &str) -> anyhow::Result<FilterExpr> {
    use pest::Parser;

    let pairs = FilterParser::parse(Rule::filter, input)
        .map_err(|e| anyhow::anyhow!("Parse error: {e}"))?;

    // Get the or_expr inside the filter rule
    let or_expr = pairs
        .into_iter()
        .next()
        .ok_or_else(|| anyhow::anyhow!("Empty parse result"))?
        .into_inner()
        .next()
        .ok_or_else(|| anyhow::anyhow!("No expression found"))?;

    build_expr(or_expr)
}

/// Build FilterExpr from pest Pair
fn build_expr(pair: pest::iterators::Pair<Rule>) -> anyhow::Result<FilterExpr> {
    match pair.as_rule() {
        Rule::or_expr => {
            let mut inner = pair.into_inner();
            let mut expr = build_expr(inner.next().unwrap())?;

            while let Some(next) = inner.next() {
                if next.as_rule() == Rule::or_op {
                    let right = build_expr(inner.next().unwrap())?;
                    expr = FilterExpr::Logical {
                        left: Box::new(expr),
                        operator: LogicalOperator::Or,
                        right: Box::new(right),
                    };
                }
            }

            Ok(expr)
        }

        Rule::and_expr => {
            let mut inner = pair.into_inner();
            let mut expr = build_expr(inner.next().unwrap())?;

            while let Some(next) = inner.next() {
                if next.as_rule() == Rule::and_op {
                    let right = build_expr(inner.next().unwrap())?;
                    expr = FilterExpr::Logical {
                        left: Box::new(expr),
                        operator: LogicalOperator::And,
                        right: Box::new(right),
                    };
                }
            }

            Ok(expr)
        }

        Rule::primary => {
            let inner = pair.into_inner().next().unwrap();
            build_expr(inner)
        }

        Rule::paren_expr => {
            let inner = pair.into_inner().next().unwrap();
            build_expr(inner)
        }

        Rule::condition => {
            let mut inner = pair.into_inner();
            let field = inner.next().unwrap().as_str();
            let value = inner.next().unwrap().as_str().trim(); // Trim whitespace from value

            let filter_type = ExtractFilterType::from_alias(field)
                .ok_or_else(|| anyhow::anyhow!("Unknown filter field: {field}"))?;

            let (operator, cleaned_value) = parse_value_and_operator(value)?;

            Ok(FilterExpr::Condition(FilterCondition {
                filter_type,
                operator,
                value: cleaned_value,
            }))
        }

        _ => Err(anyhow::anyhow!("Unexpected rule: {:?}", pair.as_rule())),
    }
}

/// Parse value and detect operator (>=, <=, range, etc.)
fn parse_value_and_operator(value: &str) -> anyhow::Result<(FilterOperator, String)> {
    // Handle range syntax first (e.g., "1-5", "0.5-4.5")
    if let Some((min_str, max_str)) = value.split_once('-')
        && let (Ok(min), Ok(max)) = (min_str.trim().parse::<f64>(), max_str.trim().parse::<f64>())
    {
        return Ok((FilterOperator::Range(min, max), value.to_string()));
    }

    // Handle comparison operators
    if let Some(stripped) = value.strip_prefix(">=") {
        return Ok((FilterOperator::GreaterEqual, stripped.trim().to_string()));
    }
    if let Some(stripped) = value.strip_prefix("<=") {
        return Ok((FilterOperator::LessEqual, stripped.trim().to_string()));
    }
    if let Some(stripped) = value.strip_prefix('>') {
        return Ok((FilterOperator::Greater, stripped.trim().to_string()));
    }
    if let Some(stripped) = value.strip_prefix('<') {
        return Ok((FilterOperator::Less, stripped.trim().to_string()));
    }

    // Remove quotes if present
    let cleaned_value = if (value.starts_with('"') && value.ends_with('"'))
        || (value.starts_with('\'') && value.ends_with('\''))
    {
        value[1..value.len() - 1].to_string()
    } else {
        value.to_string()
    };

    // Default to exact match for most fields, contains for path
    Ok((FilterOperator::Equal, cleaned_value))
}

pub fn has_same_field_and_conditions(expr: &FilterExpr) -> bool {
    // Detects whether any AND-combination in the expression (after distributing
    // AND over OR) repeats a field, e.g. "sp:A and sp:B" but also
    // "(sp:A and sp:B) or r:5" and "sp:A and (sp:B or r:5)".
    // Returns (fields reachable in the subtree, repeated field found).
    fn check(expr: &FilterExpr) -> (Vec<ExtractFilterType>, bool) {
        match expr {
            FilterExpr::Condition(cond) => (vec![cond.filter_type], false),
            FilterExpr::Logical {
                left,
                operator,
                right,
            } => {
                let (left_fields, left_dup) = check(left);
                let (right_fields, right_dup) = check(right);
                // For AND, a field reachable on both sides ends up repeated in
                // some distributed AND-term; for OR, branches stay separate.
                let dup = left_dup
                    || right_dup
                    || (matches!(operator, LogicalOperator::And)
                        && left_fields.iter().any(|f| right_fields.contains(f)));
                let mut fields = left_fields;
                fields.extend(right_fields);
                (fields, dup)
            }
        }
    }

    check(expr).1
}

/// Convert FilterExpr to Polars Expr
///
/// # Parameters
/// * `expr` - The filter expression to convert
/// * `per_image` - If true, each condition holds when any row of the same path
///   matches it (so "sp:A and sp:B" finds images with both species); all rows of
///   a matching image are kept.
pub fn filter_expr_to_polars(expr: &FilterExpr, per_image: bool) -> anyhow::Result<Expr> {
    use crate::utils::TagType;

    match expr {
        FilterExpr::Condition(condition) => {
            let col_name = match condition.filter_type {
                ExtractFilterType::Species => TagType::Species.col_name(),
                ExtractFilterType::Individual => TagType::Individual.col_name(),
                ExtractFilterType::Rating => RATING_COLUMN,
                ExtractFilterType::Path => PATH_COLUMN,
                ExtractFilterType::Event => EVENT_ID_COLUMN,
                ExtractFilterType::Custom => CUSTOM_COLUMN,
                ExtractFilterType::Advanced => {
                    return Err(anyhow::anyhow!(
                        "Advanced filter should not appear in conditions"
                    ));
                }
            };

            let base_col = col(col_name);

            let leaf = match &condition.operator {
                FilterOperator::Equal => {
                    if condition.filter_type == ExtractFilterType::Path {
                        // Path uses contains for substring matching
                        Ok(base_col
                            .str()
                            .contains_literal(lit(condition.value.clone())))
                    } else {
                        Ok(base_col.eq(lit(condition.value.clone())))
                    }
                }
                FilterOperator::Range(min, max) => {
                    // Rating stays as scalar in both modes
                    let numeric_col = base_col.cast(DataType::Float64);
                    Ok(numeric_col
                        .clone()
                        .is_not_null()
                        .and(numeric_col.clone().gt_eq(lit(*min)))
                        .and(numeric_col.lt_eq(lit(*max))))
                }
                FilterOperator::GreaterEqual => {
                    if let Ok(value) = condition.value.parse::<f64>() {
                        let numeric_col = base_col.cast(DataType::Float64);
                        Ok(numeric_col
                            .clone()
                            .is_not_null()
                            .and(numeric_col.gt_eq(lit(value))))
                    } else {
                        Err(anyhow::anyhow!(
                            "GreaterEqual operator requires numeric value"
                        ))
                    }
                }
                FilterOperator::LessEqual => {
                    if let Ok(value) = condition.value.parse::<f64>() {
                        let numeric_col = base_col.cast(DataType::Float64);
                        Ok(numeric_col
                            .clone()
                            .is_not_null()
                            .and(numeric_col.lt_eq(lit(value))))
                    } else {
                        Err(anyhow::anyhow!("LessEqual operator requires numeric value"))
                    }
                }
                FilterOperator::Greater => {
                    if let Ok(value) = condition.value.parse::<f64>() {
                        let numeric_col = base_col.cast(DataType::Float64);
                        Ok(numeric_col
                            .clone()
                            .is_not_null()
                            .and(numeric_col.gt(lit(value))))
                    } else {
                        Err(anyhow::anyhow!("Greater operator requires numeric value"))
                    }
                }
                FilterOperator::Less => {
                    if let Ok(value) = condition.value.parse::<f64>() {
                        let numeric_col = base_col.cast(DataType::Float64);
                        Ok(numeric_col
                            .clone()
                            .is_not_null()
                            .and(numeric_col.lt(lit(value))))
                    } else {
                        Err(anyhow::anyhow!("Less operator requires numeric value"))
                    }
                }
            }?;
            Ok(if per_image {
                leaf.any(true).over([col(PATH_COLUMN)])?
            } else {
                leaf
            })
        }
        FilterExpr::Logical {
            left,
            operator,
            right,
        } => {
            let left_expr = filter_expr_to_polars(left, per_image)?;
            let right_expr = filter_expr_to_polars(right, per_image)?;

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
        .map(|s| s.starts_with('.') || s.contains("精选") || s.contains("digikamtempfile")) // ignore 精选, .dtrash and digiKam temp files
        .unwrap_or(false)
}

// Serval bar style
pub fn serval_pb_style() -> ProgressStyle {
    ProgressStyle::default_bar()
        .template(
            "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta}) {wide_msg}",
        )
        .unwrap()
        .progress_chars("=> ")
}

pub fn configure_progress_bar(pb: &ProgressBar) {
    pb.set_style(serval_pb_style());
    pb.enable_steady_tick(std::time::Duration::from_secs(1));
}

/// Name of serval's own output directory, created under the working directory.
/// Directory walkers must never treat it as camera-trap data.
pub const SERVAL_OUTPUT_DIR: &str = "serval_output";

static RUN_LOG: std::sync::OnceLock<(PathBuf, std::sync::Mutex<File>)> = std::sync::OnceLock::new();

/// Best-effort creation of the run log for file-operation commands. Written to
/// `log_dir` when the command has an output directory, otherwise to
/// ./serval_output/logs. Per-file statuses and warnings are mirrored there,
/// since transient bar messages leave no trace in the terminal.
pub fn init_run_log(command: &str, log_dir: Option<&Path>) {
    let log_dir = log_dir
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from(format!("./{SERVAL_OUTPUT_DIR}/logs")));
    let init = || -> anyhow::Result<(PathBuf, std::sync::Mutex<File>)> {
        fs::create_dir_all(&log_dir)?;
        let timestamp = chrono::Local::now().format("%Y%m%d_%H%M%S");
        let log_path = log_dir.join(format!("serval_{command}_{timestamp}.log"));
        let file = File::create(&log_path)?;
        Ok((log_path, std::sync::Mutex::new(file)))
    };
    match init() {
        Ok(entry) => {
            let _ = RUN_LOG.set(entry);
            log_line(&format!(
                "Command: {}",
                env::args().collect::<Vec<_>>().join(" ")
            ));
        }
        Err(err) => eprintln!(
            "Warning: failed to create run log in {}: {err}",
            log_dir.display()
        ),
    }
}

pub fn run_log_path() -> Option<&'static Path> {
    RUN_LOG.get().map(|(path, _)| path.as_path())
}

/// Append a timestamped line to the run log; no-op when no log is set up.
pub fn log_line(message: &str) {
    if let Some((_, log)) = RUN_LOG.get()
        && let Ok(mut file) = log.lock()
    {
        use std::io::Write;
        let timestamp = chrono::Local::now().format("%H:%M:%S");
        let _ = writeln!(file, "[{timestamp}] {message}");
    }
}

/// Show transient per-file status in the progress bar. When the bar is hidden
/// (non-TTY output), print a plain line instead so logs keep the information.
pub fn pb_status(pb: &ProgressBar, message: impl Into<String>) {
    let message = message.into();
    log_line(&message);
    if pb.is_hidden() {
        println!("{message}");
    } else {
        pb.set_message(message);
    }
}

/// Prints warnings above the progress bar as they happen and, after the bar
/// finishes, a count line so they are not overlooked.
#[derive(Default)]
pub struct WarningCollector {
    count: std::sync::atomic::AtomicUsize,
}

impl WarningCollector {
    /// Print the warning above the progress bar (or as a plain line when the
    /// bar is hidden) and count it for the final notice.
    pub fn warn(&self, pb: &ProgressBar, message: impl Into<String>) {
        let message = message.into();
        log_line(&format!("Warning: {message}"));
        if pb.is_hidden() {
            eprintln!("Warning: {message}");
        } else {
            pb.println(format!("Warning: {message}"));
        }
        self.count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// Print the warning without a progress bar and count it for the final notice.
    pub fn warn_plain(&self, message: impl Into<String>) {
        let message = message.into();
        log_line(&format!("Warning: {message}"));
        eprintln!("Warning: {message}");
        self.count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn summarize(&self) {
        let count = self.count.load(std::sync::atomic::Ordering::Relaxed);
        if count > 0 {
            log_line(&format!("{count} warning(s) occurred"));
            eprintln!("{count} warning(s) occurred, see messages above.");
        }
    }
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

/// Resource paths under `root_dir`, sorted so that runs over the same input
/// are reproducible (e.g. which of two colliding names gets the "_1" suffix).
///
/// `exclude_dir` is the command's own output directory, if it has one. When it
/// lies inside `root_dir` its subtree is skipped, otherwise a rerun would
/// process its previous output again. It must already exist to be recognized.
pub fn path_enumerate(
    root_dir: PathBuf,
    resource_type: ResourceType,
    exclude_dir: Option<&Path>,
) -> Vec<PathBuf> {
    let exclude = exclude_dir.and_then(|dir| nested_dir(&root_dir, dir));
    let mut paths: Vec<PathBuf> = WalkDir::new(root_dir)
        .into_iter()
        // Ignore rules apply below the root: a root the user passes explicitly
        // (e.g. ".backup") is walked even if its name would be ignored.
        .filter_entry(|e| {
            (e.depth() == 0 || !is_ignored(e)) && exclude.as_deref() != Some(e.path())
        })
        .filter_map(Result::ok)
        .filter(|e| resource_type.is_resource(e.path()))
        .map(|e| e.into_path())
        .collect();
    paths.sort();
    paths
}

/// `dir` expressed under `root_dir` (in the form the walk produces) if it lies
/// inside it; both are resolved first so relative paths and ".." still match.
fn nested_dir(root_dir: &Path, dir: &Path) -> Option<PathBuf> {
    let root = fs::canonicalize(root_dir).ok()?;
    let dir = fs::canonicalize(dir).ok()?;
    dir.strip_prefix(&root)
        .ok()
        .map(|relative| root_dir.join(relative))
}

/// Flatten `deploy_dir` into `working_dir/<dir name>/` (see `flatten_transfers`).
pub fn resources_flatten(
    deploy_dir: PathBuf,
    working_dir: PathBuf,
    resource_type: ResourceType,
    dry_run: bool,
    move_mode: bool,
    keep_first_subdir: bool,
) -> anyhow::Result<()> {
    let transfers = flatten_transfers(
        &deploy_dir,
        &working_dir,
        resource_type,
        false,
        keep_first_subdir,
    )?;
    run_transfers(transfers, transfer_mode(move_mode), None, dry_run)
}

fn transfer_mode(move_mode: bool) -> Mode {
    if move_mode { Mode::Move } else { Mode::Copy }
}

/// Transfers that flatten `deploy_dir` into `working_dir/<dir name>/`: each file
/// is named after its path below `deploy_dir`, parts joined by "-" (prefixed by
/// the deployment ID in align mode). With `ResourceType::All`, sidecars travel
/// with their media file.
fn flatten_transfers(
    deploy_dir: &Path,
    working_dir: &Path,
    resource_type: ResourceType,
    prefix_deploy_id_in_name: bool,
    keep_first_subdir: bool,
) -> anyhow::Result<Vec<Transfer>> {
    let deploy_id = deploy_dir
        .file_name()
        .ok_or_else(|| anyhow::anyhow!("Invalid deploy directory path: no filename"))?;
    let base_output_dir = working_dir.join(deploy_id);

    let resource_paths = path_enumerate(
        deploy_dir.to_path_buf(),
        resource_type,
        Some(&base_output_dir),
    );
    println!(
        "{} {}(s) found in {}",
        resource_paths.len(),
        resource_type,
        deploy_dir.to_string_lossy()
    );

    let flat_target = |resource: &Path| {
        let relative_path = resource.strip_prefix(deploy_dir).unwrap_or(resource);
        let mut relative_parts: Vec<OsString> = relative_path
            .iter()
            .map(|part| part.to_os_string())
            .collect();
        if relative_parts.is_empty() {
            relative_parts.push("unnamed_file".into());
        }
        let mut output_dir = base_output_dir.clone();
        if keep_first_subdir && relative_parts.len() > 1 {
            output_dir = output_dir.join(&relative_parts[0]);
        }
        let mut name_parts: Vec<OsString> = Vec::new();
        if prefix_deploy_id_in_name {
            name_parts.push(deploy_id.to_os_string());
        }
        name_parts.extend(relative_parts);
        output_dir.join(name_parts.join(std::ffi::OsStr::new("-")))
    };

    let pair = matches!(resource_type, ResourceType::All);
    let is_xmp = |path: &Path| resource_extension(path).as_deref() == Some("xmp");
    let mut sidecars: HashMap<PathBuf, PathBuf> = HashMap::new();
    let mut transfers = Vec::new();
    let mut media = Vec::new();
    for path in resource_paths {
        if pair && is_xmp(&path) {
            if let Some(other) = sidecars.insert(underlying_media_path(&path), path) {
                media.push(other); // a second sidecar for the same file travels alone
            }
        } else {
            media.push(path);
        }
    }
    for source in media {
        let sidecar = if is_xmp(&source) {
            None
        } else {
            sidecars.remove(&source)
        };
        transfers.push(Transfer {
            target: flat_target(&source),
            sidecar,
            sidecar_slot: pair && !is_xmp(&source),
            source,
        });
    }
    // Sidecars without their media file travel alone.
    for (_, source) in sidecars {
        transfers.push(Transfer {
            target: flat_target(&source),
            sidecar: None,
            sidecar_slot: false,
            source,
        });
    }
    Ok(transfers)
}

pub fn deployments_align(
    project_dir: PathBuf,
    output_dir: PathBuf,
    deploy_table: PathBuf,
    resource_type: ResourceType,
    dry_run: bool,
    move_mode: bool,
    keep_first_subdir: bool,
) -> anyhow::Result<()> {
    let deploy_df = CsvReadOptions::default()
        .with_columns(csv_projection_columns(&[DEPLOYMENT_ID_COLUMN]))
        .try_into_reader_with_file_path(Some(deploy_table))?
        .finish()?;
    reject_duplicate_csv_columns(&deploy_df)?;
    let deploy_df = deploy_df
        .lazy()
        .select([col(DEPLOYMENT_ID_COLUMN)])
        .collect()?;
    let deploy_array = deploy_df[DEPLOYMENT_ID_COLUMN].str()?;

    // Plan all deployments first, so existing targets are asked about once.
    let mut transfers = Vec::new();
    for deploy_id in deploy_array.iter() {
        let deploy_id = deploy_id
            .ok_or_else(|| anyhow::anyhow!("Empty deploymentID found in the deployments table"))?;
        let (_, collection_name) = deploy_id.rsplit_once('_').ok_or_else(|| {
            anyhow::anyhow!(
                "Invalid deploymentID '{deploy_id}': expected '<deployment_name>_<collection_name>'"
            )
        })?;
        let deploy_dir = project_dir.join(collection_name).join(deploy_id);
        let collection_output_dir = output_dir.join(collection_name);
        transfers.extend(flatten_transfers(
            &deploy_dir,
            &collection_output_dir,
            resource_type,
            true,
            keep_first_subdir,
        )?);
    }
    run_transfers(transfers, transfer_mode(move_mode), None, dry_run)
}

pub fn deployments_rename(project_dir: PathBuf, dry_run: bool) -> anyhow::Result<()> {
    // rename deployment path name to <deployment_name>_<collection_name>
    let mut count = 0;
    for entry in project_dir.read_dir()? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            // Skip serval's own output tree.
            if path.file_name().and_then(|name| name.to_str()) == Some(SERVAL_OUTPUT_DIR) {
                continue;
            }
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
                    let message = format!(
                        "Renaming collection {} to {}",
                        collection_dir.display(),
                        new_collection_dir.display()
                    );
                    log_line(&message);
                    println!("{message}");
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
                // The deployment ID is "<deployment>_<collection>" in lower case. A name
                // that already ends with "_<collection>" only needs lowercasing (a mere
                // substring match would treat "cam1" in collection "a" as renamed).
                let deploy_lower = deploy_name.to_lowercase();
                let suffix = format!("_{}", collection_name.to_lowercase());
                let deploy_id = if deploy_lower.ends_with(&suffix) {
                    deploy_lower
                } else {
                    format!("{deploy_lower}{suffix}")
                };
                if deploy_id != deploy_name {
                    if dry_run {
                        println!("Will rename {deploy_name} to {deploy_id}");
                    } else {
                        let mut deploy_id_dir = deploy_dir.clone();
                        deploy_id_dir.set_file_name(&deploy_id);
                        let message = format!(
                            "Renaming {} to {}",
                            deploy_dir.display(),
                            deploy_id_dir.display()
                        );
                        log_line(&message);
                        println!("{message}");
                        fs::rename(deploy_dir, deploy_id_dir)?;
                    }
                }
            }
        }
    }
    println!("Total directories: {count}");
    Ok(())
}

/// Copy the XMP files under `source_dir` to `output_dir`, keeping the directory
/// structure. Unchanged files are skipped; for changed ones the user is asked.
pub fn copy_xmp(source_dir: PathBuf, output_dir: PathBuf) -> anyhow::Result<()> {
    let xmp_paths = path_enumerate(source_dir.clone(), ResourceType::Xmp, Some(&output_dir));
    println!("{} xmp files found", xmp_paths.len());
    let transfers = xmp_paths
        .into_iter()
        .map(|xmp| {
            let relative_path = xmp.strip_prefix(&source_dir).unwrap_or(&xmp);
            Transfer {
                target: output_dir.join(relative_path),
                source: xmp,
                sidecar: None,
                sidecar_slot: false,
            }
        })
        .collect();
    run_transfers(transfers, Mode::Copy, None, false)
}

/// Outcome of one item in a batch operation: performed, or skipped with a reason.
pub enum BatchOutcome {
    Done,
    Skipped(String),
}

/// Print skip warnings, failure errors, and a per-outcome count summary for a
/// batch operation.
pub fn report_batch_results(results: Vec<anyhow::Result<BatchOutcome>>, action: &str) {
    let mut done = 0;
    let mut skipped = Vec::new();
    let mut failures = Vec::new();
    for result in results {
        match result {
            Ok(BatchOutcome::Done) => done += 1,
            Ok(BatchOutcome::Skipped(reason)) => skipped.push(reason),
            Err(err) => failures.push(err),
        }
    }
    for reason in &skipped {
        log_line(&format!("Warning: {reason}"));
        eprintln!("Warning: {reason}");
    }
    for err in &failures {
        log_line(&format!("Error: {err}"));
        eprintln!("Error: {err}");
    }
    let summary = format!(
        "{done} XMP file(s) {action}, {} skipped, {} failed",
        skipped.len(),
        failures.len()
    );
    log_line(&summary);
    println!("{summary}");
}

// Sync XMP metadata to corresponding media files
pub fn sync_xmp_to_media(xmp_path: &Path) -> anyhow::Result<BatchOutcome> {
    let media_path = underlying_media_path(xmp_path);
    if media_path == xmp_path {
        return Ok(BatchOutcome::Skipped(format!(
            "Skipping non-XMP file: {}",
            xmp_path.display()
        )));
    }

    if !media_path.exists() {
        return Ok(BatchOutcome::Skipped(format!(
            "Skipping {}: media file {} does not exist",
            xmp_path.display(),
            media_path.display()
        )));
    }

    let xmp_content = fs::read_to_string(xmp_path)?;
    let xmp_meta = XmpMeta::from_str(&xmp_content)?;

    let mut xmp_file = XmpFile::new()?;
    let open_options = OpenFileOptions::default().for_update();
    xmp_file.open_file(media_path, open_options)?;
    xmp_file.put_xmp(&xmp_meta)?;
    xmp_file.try_close()?;

    Ok(BatchOutcome::Done)
}

pub fn sync_xmp_directory(source_dir: PathBuf) -> anyhow::Result<()> {
    let xmp_paths = path_enumerate(source_dir.clone(), ResourceType::Xmp, None);
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
    configure_progress_bar(&pb);
    pb.set_message("Syncing XMP metadata to media files...");

    let results: Vec<anyhow::Result<BatchOutcome>> = xmp_paths
        .par_iter()
        .map(|xmp_path| {
            let result = sync_xmp_to_media(xmp_path);
            pb.inc(1);
            result
        })
        .collect();

    pb.finish();
    report_batch_results(results, "synced");

    Ok(())
}

pub fn sync_xmp_from_csv(csv_path: PathBuf) -> anyhow::Result<()> {
    let df = CsvReadOptions::default()
        .with_columns(csv_projection_columns(&[PATH_COLUMN]))
        .with_ignore_errors(false)
        .try_into_reader_with_file_path(Some(csv_path))?
        .finish()?;
    reject_duplicate_csv_columns(&df)?;

    let df_unique = df
        .lazy()
        .filter(col(PATH_COLUMN).is_not_null())
        .select([col(PATH_COLUMN)])
        .unique_stable(None, UniqueKeepStrategy::First)
        .collect()?;
    // Extension check in Rust so that ".XMP" counts too.
    let xmp_paths: Vec<PathBuf> = df_unique
        .column(PATH_COLUMN)?
        .str()?
        .iter()
        .flatten()
        .map(PathBuf::from)
        .filter(|path| resource_extension(path).as_deref() == Some("xmp"))
        .collect();

    let num_files = xmp_paths.len();
    if num_files == 0 {
        println!("No XMP files found in CSV");
        return Ok(());
    }

    println!("Found {num_files} XMP files in CSV to sync");

    let pb = indicatif::ProgressBar::new(num_files as u64);
    configure_progress_bar(&pb);
    pb.set_message("Syncing XMP files in CSV...");

    let results: Vec<anyhow::Result<BatchOutcome>> = xmp_paths
        .par_iter()
        .map(|xmp_path| {
            let result = sync_xmp_to_media(xmp_path);
            pb.inc(1);
            result
        })
        .collect();

    pb.finish();
    report_batch_results(results, "synced");

    Ok(())
}

// Remove all XMP files recursively from a directory
pub fn remove_xmp_files(source_dir: PathBuf) -> anyhow::Result<()> {
    let xmp_paths = path_enumerate(source_dir.clone(), ResourceType::Xmp, None);
    let num_xmp = xmp_paths.len();

    if num_xmp == 0 {
        println!("No XMP files found in {}", source_dir.display());
        return Ok(());
    }

    println!("Found {} XMP files in {}", num_xmp, source_dir.display());

    let pb = indicatif::ProgressBar::new(num_xmp as u64);
    configure_progress_bar(&pb);
    pb.set_message("Removing XMP files...");

    let results: Vec<anyhow::Result<BatchOutcome>> = xmp_paths
        .par_iter()
        .map(|xmp_path| {
            let result = fs::remove_file(xmp_path)
                .map(|_| BatchOutcome::Done)
                .map_err(|e| anyhow::anyhow!("Failed to remove {}: {}", xmp_path.display(), e));
            pb.inc(1);
            result
        })
        .collect();

    pb.finish();
    report_batch_results(results, "removed");
    Ok(())
}

pub fn get_path_levels(path: String) -> Vec<String> {
    // Plain string splitting instead of Path::components for performance.
    // The first component (root/prefix) and the last one (file name) are not
    // selectable as deployment levels.
    let normalized_path = normalize_path_separators(&path);
    let levels: Vec<String> = normalized_path
        .split('/')
        .map(|comp| comp.to_string())
        .collect();
    if levels.len() < 2 {
        return Vec::new();
    }
    levels[1..levels.len() - 1].to_vec()
}

fn normalize_path_separators(path: &str) -> String {
    path.replace('\\', "/")
}

// Guess which path level is the deployment, top-down: skip the levels shared by
// all paths (the common prefix), then based on assumption that:
// the first diverging level is usually the collection or the deployment,
// and #deployments is usually larger than #collections.
pub fn detect_deployment_path_index<I, S>(paths: I) -> Option<i32>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut level_names: Vec<HashSet<String>> = Vec::new();
    let mut depth = None;
    for path in paths {
        let normalized = normalize_path_separators(path.as_ref());
        let components: Vec<&str> = normalized.split('/').collect();
        // Same exclusions as get_path_levels: root/prefix and file name.
        if components.len() < 3 {
            return None;
        }
        match depth {
            None => {
                depth = Some(components.len());
                level_names = vec![HashSet::new(); components.len() - 2];
            }
            // Mixed depths make a single global index ill-defined; let the user decide.
            Some(depth) if depth != components.len() => return None,
            Some(_) => {}
        }
        for (level, name) in components[1..components.len() - 1].iter().enumerate() {
            if !level_names[level].contains(*name) {
                level_names[level].insert((*name).to_string());
            }
        }
    }
    // All levels shared by every path (e.g. a single deployment): nothing to infer.
    let diverge_level = level_names.iter().position(|names| names.len() > 1)?;
    let deploy_level = if diverge_level + 1 < level_names.len()
        && level_names[diverge_level + 1].len() > level_names[diverge_level].len()
    {
        diverge_level + 1
    } else {
        diverge_level
    };
    // +1 converts back to the split index (level_names[0] is split component 1).
    (deploy_level + 1).try_into().ok()
}

pub fn deployment_from_path_expr(path_expr: Expr, deploy_path_index: i32) -> Expr {
    path_expr
        .str()
        .replace_all(lit("\\"), lit("/"), true)
        .str()
        .split(lit("/"))
        .list()
        .get(lit(deploy_path_index), false)
}

pub fn ignore_timezone(time: String) -> anyhow::Result<String> {
    let time = time.trim_end_matches('Z');
    // Offsets (+HH:MM / -HH:MM) and fractional seconds can only appear after the
    // time-of-day part, so search after 'T'/' ' to avoid cutting at date separators.
    let time_start = time.find(['T', ' ']).map_or(0, |i| i + 1);
    let tz_start = time[time_start..]
        .find(['+', '-', '.'])
        .map_or(time.len(), |i| time_start + i);
    Ok(time[..tz_start].to_string())
}

pub fn iso_datetime_to_csv_format(time: &str) -> String {
    time.replace('T', " ")
}

pub fn tags_csv_translate(
    source_csv: PathBuf,
    taglist_csv: PathBuf,
    output_dir: PathBuf,
    from: &str,
    to: &str,
) -> anyhow::Result<()> {
    let source_df = CsvReadOptions::default()
        .with_infer_schema_length(Some(0))
        .try_into_reader_with_file_path(Some(source_csv.clone()))?
        .finish()?;
    reject_duplicate_csv_columns(&source_df)?;
    let taglist_df = CsvReadOptions::default()
        .with_columns(csv_projection_columns(&[from, to]))
        .try_into_reader_with_file_path(Some(taglist_csv))?
        .finish()?;
    reject_duplicate_csv_columns(&taglist_df)?;

    let joined = source_df.lazy().join(
        taglist_df.lazy(),
        [col(TagType::Species.col_name())],
        [col(from)],
        JoinArgs::new(JoinType::Left),
    );

    let unknown = joined
        .clone()
        .filter(
            col(to)
                .is_null()
                .and(col(TagType::Species.col_name()).is_not_null())
                .and(col(TagType::Species.col_name()).neq(lit(""))),
        )
        .select([col(TagType::Species.col_name())])
        .unique(None, UniqueKeepStrategy::Any)
        .collect()?;
    if unknown.height() > 0 {
        let mut sample = Vec::new();
        if let Ok(col) = unknown.column(TagType::Species.col_name())
            && let Ok(ca) = col.str()
        {
            for v in ca.iter().flatten().take(20) {
                sample.push(v.to_string());
            }
        }
        return Err(anyhow::anyhow!(
            "Unknown tag(s) not found in taglist: {}",
            sample.join(", ")
        ));
    }

    let mut result = joined
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ignore_timezone_strips_timezone_suffixes() {
        let strip = |s: &str| ignore_timezone(s.to_string()).unwrap();
        assert_eq!(strip("2023-12-08T10:47:39+08:00"), "2023-12-08T10:47:39");
        assert_eq!(strip("2023-12-08T10:47:39-08:00"), "2023-12-08T10:47:39");
        assert_eq!(strip("2023-12-08T10:47:39Z"), "2023-12-08T10:47:39");
        assert_eq!(strip("2023-12-08T10:47:39"), "2023-12-08T10:47:39");
        assert_eq!(
            strip("2023-12-08T10:47:39.123+08:00"),
            "2023-12-08T10:47:39"
        );
        assert_eq!(strip("2023-12-08 10:47:39-0800"), "2023-12-08 10:47:39");
    }

    #[test]
    fn detect_deployment_path_index_top_down() {
        // collection diverges first, deployments outnumber collections
        assert_eq!(
            detect_deployment_path_index([
                "project/col_a/dep1_col_a/IMG_0001.jpg",
                "project/col_a/dep2_col_a/IMG_0001.jpg",
                "project/col_b/dep3_col_b/IMG_0002.jpg",
            ]),
            Some(2)
        );
        // camera subfolders below the deployment share names -> not more distinct
        assert_eq!(
            detect_deployment_path_index([
                "project/col_a/dep1/100MEDIA/IMG_0001.jpg",
                "project/col_a/dep2/100MEDIA/IMG_0001.jpg",
            ]),
            Some(2)
        );
        // divergence at the last directory level
        assert_eq!(
            detect_deployment_path_index(["data/dep1/IMG_0001.jpg", "data/dep2/IMG_0001.jpg"]),
            Some(1)
        );
        // backslash paths are normalized
        assert_eq!(
            detect_deployment_path_index([
                r"project\col_a\dep1\IMG_0001.jpg",
                r"project\col_a\dep2\IMG_0001.jpg",
            ]),
            Some(2)
        );
        // single deployment: every level is common, nothing to infer
        assert_eq!(
            detect_deployment_path_index(["project/col_a/dep1/a.jpg", "project/col_a/dep1/b.jpg"]),
            None
        );
        // mixed depths: a single global index is ill-defined
        assert_eq!(
            detect_deployment_path_index([
                "project/col_a/dep1/a.jpg",
                "project/col_a/dep2/100MEDIA/b.jpg",
            ]),
            None
        );
        // no directory level between root and file name
        assert_eq!(detect_deployment_path_index(["dep1/a.jpg"]), None);
        assert_eq!(detect_deployment_path_index(Vec::<String>::new()), None);
    }

    #[test]
    fn flatten_collision_naming_is_deterministic() {
        // a/b-c.jpg and a-b/c.jpg both flatten to a-b-c.jpg; the first path in
        // sorted order must always keep the plain name.
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("src");
        fs::create_dir_all(src.join("a")).unwrap();
        fs::create_dir_all(src.join("a-b")).unwrap();
        for i in 0..20 {
            fs::write(src.join(format!("a/b-c{i}.jpg")), "from a").unwrap();
            fs::write(src.join(format!("a-b/c{i}.jpg")), "from a-b").unwrap();
        }
        let out = dir.path().join("out");
        resources_flatten(src, out.clone(), ResourceType::Media, false, false, false).unwrap();
        for i in 0..20 {
            let plain = fs::read_to_string(out.join(format!("src/a-b-c{i}.jpg"))).unwrap();
            assert_eq!(plain, "from a");
        }
    }

    #[test]
    fn flatten_rerun_skips_output_inside_source() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("src");
        fs::create_dir_all(src.join("d")).unwrap();
        fs::write(src.join("d/a.jpg"), "").unwrap();
        for _ in 0..2 {
            resources_flatten(
                src.clone(),
                src.join("out"),
                ResourceType::All,
                false,
                false,
                false,
            )
            .unwrap();
        }
        // The second run must not flatten the first run's output again
        // (which would produce names like "out-src-d-a.jpg").
        let reprocessed: Vec<_> = WalkDir::new(src.join("out"))
            .into_iter()
            .filter_map(Result::ok)
            .filter(|e| e.file_name().to_string_lossy().starts_with("out-"))
            .map(|e| e.into_path())
            .collect();
        assert!(reprocessed.is_empty(), "{reprocessed:?}");
    }

    #[test]
    fn advanced_filter_matches_per_image_with_all_fields() {
        let df = df!(
            PATH_COLUMN => ["a", "a", "b", "c", "c"],
            "species" => ["Fox", "Deer", "Fox", "Fox", "Deer"],
            EVENT_ID_COLUMN => ["1", "1", "1", "3", "3"],
        )
        .unwrap();
        let matching = |query: &str| {
            let expr = parse_advanced_filter(query).unwrap();
            let per_image = has_same_field_and_conditions(&expr);
            let out = df
                .clone()
                .lazy()
                .filter(filter_expr_to_polars(&expr, per_image).unwrap())
                .collect()
                .unwrap();
            let mut paths: Vec<String> = out
                .column(PATH_COLUMN)
                .unwrap()
                .str()
                .unwrap()
                .iter()
                .flatten()
                .map(str::to_string)
                .collect();
            paths.sort();
            paths
        };
        // all rows of images with both species are kept
        assert_eq!(matching("sp:Fox and sp:Deer"), ["a", "a", "c", "c"]);
        // the event column is still there when a field repeats
        assert_eq!(matching("sp:Fox and sp:Deer and e:1"), ["a", "a"]);
        assert_eq!(matching("e:>=2 and e:<=3"), ["c", "c"]);
    }

    #[test]
    fn advanced_filter_detects_same_field_and_conditions() {
        let needs_agg =
            |input: &str| has_same_field_and_conditions(&parse_advanced_filter(input).unwrap());
        assert!(needs_agg("species:A and species:B"));
        assert!(needs_agg("(species:A and species:B) or rating:5"));
        assert!(needs_agg("species:A and (species:B or rating:5)"));
        assert!(needs_agg("(species:A or rating:5) and species:B"));
        assert!(!needs_agg("species:A or species:B"));
        assert!(!needs_agg("species:A and rating:4-5"));
        assert!(!needs_agg("(species:A or rating:5) and custom:x"));
    }
}
