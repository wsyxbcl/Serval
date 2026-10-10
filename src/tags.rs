use crate::protocol::{EditLabels, FileEdit, FileState, Labels, Outcome};
use crate::schema::{
    DATETIME_COLUMN, DEPLOYMENT_ID_COLUMN, FILENAME_COLUMN, LATITUDE_COLUMN,
    LEGACY_DATETIME_COLUMN, LONGITUDE_COLUMN, MEDIA_TYPE_COLUMN, PATH_COLUMN, RATING_COLUMN,
    SUBJECTS_COLUMN, TIME_MODIFIED_COLUMN, XMP_UPDATE_COLUMN, XMP_UPDATE_DATETIME_COLUMN,
    canonicalize_observe_tags_df, infer_media_type, resource_extension, underlying_media_path,
};
use crate::transfer::{Mode, OnConflict, Transfer, create_new_sibling, run_transfers};
use crate::utils::{
    ExtractFilterType, ResourceType, SubdirType, TagType, WarningCollector, XmpUpdateType,
    absolute_path, csv_projection_columns, deployment_from_path_expr, detect_deployment_path_index,
    filter_expr_to_polars, get_path_levels, has_same_field_and_conditions, ignore_timezone,
    iso_datetime_to_csv_format, log_line, parse_advanced_filter, path_enumerate, pb_status,
    reject_duplicate_csv_columns,
};
use chrono::{DateTime, Datelike, Local, NaiveDateTime, Timelike};
use indicatif::ProgressBar;
use itertools::izip;
use polars::{lazy::dsl::StrptimeOptions, prelude::*};
use rayon::prelude::*;
use rustyline::{
    Cmd, Completer, ConditionalEventHandler, Editor, Event, EventContext, EventHandler, Helper,
    Highlighter, Hinter, KeyCode, KeyEvent, Modifiers, RepeatCount, Result,
    validate::{ValidationContext, ValidationResult, Validator},
};
use std::{
    collections::{BTreeSet, HashMap},
    fs,
    io::{self, IsTerminal, Write},
    path::{Path, PathBuf},
    str::FromStr,
};
use xmp_toolkit::{
    FromStrOptions, OpenFileOptions, ToStringOptions, XmpDate, XmpDateTime, XmpFile, XmpMeta,
    XmpTime, XmpValue, xmp_gps, xmp_ns,
};

// Namesapce for "taglists"
// Adobe
const LIGHTROOM_NS: &str = "http://ns.adobe.com/lightroom/1.0/";
const LR_HIERARCHICAL_SUBJECT: &str = "hierarchicalSubject";
// DigiKam
const DIGIKAM_NS: &str = "http://www.digikam.org/ns/1.0/";
const DIGIKAM_TAGSLIST: &str = "TagsList";

// Default species/tags to exclude from temporal independence analysis
const DEFAULT_EXCLUDE_TAGS: &[&str] = &[
    "",
    "Blank",
    "Useless data",
    "Unidentified",
    "Unknown",
    "Blur",
];

struct NumericFilteringHandler;
impl ConditionalEventHandler for NumericFilteringHandler {
    fn handle(&self, evt: &Event, _: RepeatCount, _: bool, _: &EventContext) -> Option<Cmd> {
        if let Some(KeyEvent(KeyCode::Char(c), m)) = evt.get(0) {
            if m.contains(Modifiers::CTRL) || m.contains(Modifiers::ALT) || c.is_ascii_digit() {
                None
            } else {
                Some(Cmd::Noop) // filter out invalid input
            }
        } else {
            None
        }
    }
}
#[derive(Completer, Helper, Highlighter, Hinter)]
struct NumericSelectValidator {
    min: i32,
    max: i32,
    // When true, empty input is accepted (the caller substitutes a default value).
    allow_empty: bool,
}
impl Validator for NumericSelectValidator {
    fn validate(&self, ctx: &mut ValidationContext) -> Result<ValidationResult> {
        use ValidationResult::{Invalid, Valid};
        if self.allow_empty && ctx.input().trim().is_empty() {
            return Ok(Valid(None));
        }
        let input: i32 = match ctx.input().trim().parse() {
            Ok(input) => input,
            Err(_) => {
                return Ok(Invalid(Some(" --< Expect numeric input".to_owned())));
            }
        };
        let result = if !(input >= self.min && input <= self.max) {
            Invalid(Some(format!(
                " --< Expect: number between {} and {}",
                self.min, self.max
            )))
        } else {
            Valid(None)
        };
        Ok(result)
    }
}

fn finalize_xmp_file<T>(
    file: &mut XmpFile,
    operation_result: anyhow::Result<T>,
) -> anyhow::Result<T> {
    match file.try_close().map_err(anyhow::Error::from) {
        Ok(()) => operation_result,
        Err(close_err) => match operation_result {
            Ok(_) => Err(close_err.context("Failed to close XMP file")),
            Err(err) => Err(err.context(format!("Failed to close XMP file: {close_err}"))),
        },
    }
}

fn naive_datetime_to_xmp(datetime: &str) -> anyhow::Result<XmpDateTime> {
    let datetime = NaiveDateTime::parse_from_str(datetime, "%Y-%m-%dT%H:%M:%S")?;
    Ok(XmpDateTime {
        date: Some(XmpDate {
            year: datetime.year(),
            month: datetime.month() as i32,
            day: datetime.day() as i32,
        }),
        time: Some(XmpTime {
            hour: datetime.hour() as i32,
            minute: datetime.minute() as i32,
            second: datetime.second() as i32,
            nanosecond: datetime.nanosecond() as i32,
            time_zone: None,
        }),
    })
}

fn set_xmp_datetime_without_timezone(
    xmp: &mut XmpMeta,
    namespace: &str,
    path: &str,
    datetime: &str,
) -> anyhow::Result<()> {
    let value = XmpValue::new(naive_datetime_to_xmp(datetime)?);
    xmp.set_property_date(namespace, path, &value)
        .map_err(anyhow::Error::from)
}

fn set_xmp_datetime_fields(xmp: &mut XmpMeta, datetime: &str) -> anyhow::Result<()> {
    // following the convertion table by Exiv2: https://exiv2.org/conversion.html
    set_xmp_datetime_without_timezone(xmp, xmp_ns::EXIF, "DateTimeOriginal", datetime)?;
    set_xmp_datetime_without_timezone(xmp, xmp_ns::PHOTOSHOP, "DateCreated", datetime)?;
    Ok(())
}

fn strip_xmp_datetime_timezone(
    xmp: &mut XmpMeta,
    namespace: &str,
    path: &str,
) -> anyhow::Result<()> {
    if let Some(mut value) = xmp.property_date(namespace, path)
        && let Some(time) = value.value.time.as_mut()
        && time.time_zone.is_some()
    {
        time.time_zone = None;
        xmp.set_property_date(namespace, path, &value)
            .map_err(anyhow::Error::from)?;
    }
    Ok(())
}

fn parse_xmp_gps_coordinate(raw: &str, property: &str) -> Option<f64> {
    match property {
        "GPSLatitude" => xmp_gps::exif_latitude_to_decimal(raw).or_else(|| raw.parse().ok()),
        "GPSLongitude" => xmp_gps::exif_longitude_to_decimal(raw).or_else(|| raw.parse().ok()),
        _ => None,
    }
}

fn format_coordinate(coordinate: f64) -> String {
    format!("{coordinate:.6}")
}

fn extract_xmp_gps_coordinates(xmp: &XmpMeta) -> (Option<String>, Option<String>) {
    let latitude_raw = xmp
        .property(xmp_ns::EXIF, "GPSLatitude")
        .map(|value| value.value);
    let longitude_raw = xmp
        .property(xmp_ns::EXIF, "GPSLongitude")
        .map(|value| value.value);
    let latitude = latitude_raw
        .as_deref()
        .and_then(|raw| parse_xmp_gps_coordinate(raw, "GPSLatitude"))
        .map(format_coordinate);
    let longitude = longitude_raw
        .as_deref()
        .and_then(|raw| parse_xmp_gps_coordinate(raw, "GPSLongitude"))
        .map(format_coordinate);
    (latitude, longitude)
}

/// The path level of the deployment: `level` when given (`--deployment-level`), otherwise asked on a terminal
/// with the detected level as default, otherwise the detected level.
fn prompt_deployment_path_index(
    rl: &mut Editor<NumericSelectValidator, rustyline::history::DefaultHistory>,
    path_sample: String,
    detected_index: Option<i32>,
    level: Option<i32>,
) -> anyhow::Result<i32> {
    let path_levels = get_path_levels(path_sample.clone());
    if path_levels.is_empty() {
        return Err(anyhow::anyhow!(
            "Cannot infer deployment from path: expected at least one directory level before the file name."
        ));
    }
    let max = path_levels.len().try_into()?;
    // Auto-detected level, if the guess is within the listed range.
    let default = detected_index.filter(|i| (1..=max).contains(i));
    if let Some(level) = level {
        if !(1..=max).contains(&level) {
            return Err(anyhow::anyhow!(
                "--deployment-level {level} is out of range: {path_sample} has levels 1 to {max}"
            ));
        }
        return Ok(level);
    }
    if !crate::ui::can_ask() {
        return default.ok_or_else(|| {
            anyhow::anyhow!(
                "Cannot detect which path level is the deployment; pass --deployment-level N (1 to {max})"
            )
        });
    }
    println!("\nHere is a sample of the file path ({path_sample})");
    for (i, entry) in path_levels.iter().enumerate() {
        let n = i as i32 + 1;
        if Some(n) == default {
            println!("{n}): {entry}  <- auto-detected");
        } else {
            println!("{n}): {entry}");
        }
    }
    let h = NumericSelectValidator {
        min: 1,
        max,
        allow_empty: default.is_some(),
    };
    rl.set_helper(Some(h));

    let prompt = match default {
        Some(n) => format!("Select the number corresponding to the deployment [default {n}]: "),
        None => "Select the number corresponding to the deployment: ".to_string(),
    };
    let readline = rl.readline(&prompt)?;
    let trimmed = readline.trim();
    if trimmed.is_empty()
        && let Some(n) = default
    {
        return Ok(n);
    }
    Ok(trimmed.parse::<i32>()?)
}

pub fn write_taglist(
    taglist_path: PathBuf,
    image_path: PathBuf,
    tag_type: TagType,
) -> anyhow::Result<()> {
    // Write taglist to the dummy image metadata (digiKam.TagsList)
    let mut f = XmpFile::new()?;
    let tag_df = CsvReadOptions::default()
        .with_infer_schema_length(Some(0))
        .try_into_reader_with_file_path(Some(taglist_path))?
        .finish()?;
    reject_duplicate_csv_columns(&tag_df)?;
    let tags = tag_df.column(tag_type.col_name())?.unique()?;
    XmpMeta::register_namespace(DIGIKAM_NS, "digiKam")?;
    let dummy_xmp = include_str!("../assets/dummy.xmp");
    let mut meta = XmpMeta::from_str(dummy_xmp)?;
    // flatten() skips empty (null) cells in the taglist column
    for tag in tags.str()?.iter().flatten() {
        meta.set_array_item(
            DIGIKAM_NS,
            DIGIKAM_TAGSLIST,
            xmp_toolkit::ItemPlacement::InsertBeforeIndex(1),
            &XmpValue::new(format!("{}{}", tag_type.digikam_tag_prefix(), tag)),
        )?;
    }

    f.open_file(image_path, OpenFileOptions::default().for_update())?;
    let put_result = f.put_xmp(&meta).map_err(anyhow::Error::from);
    finalize_xmp_file(&mut f, put_result)?;
    Ok(())
}

/// One row of the `xmp init` table (input for Caracal and `xmp update --datetime`).
#[derive(Default)]
struct InitRow {
    path: String,
    media_type: String,
    datetime: String,
    latitude: String,
    longitude: String,
    xmp_status: &'static str,
    embedded_datetime_original_raw: String,
    embedded_create_date_raw: String,
    file_modified_time: String,
}

fn csv_datetime(value: &XmpDateTime) -> anyhow::Result<String> {
    Ok(iso_datetime_to_csv_format(&ignore_timezone(
        value.to_string(),
    )?))
}

/// Create the sidecar of `media` if it has none. Existing sidecars are only
/// read, since they may hold corrected times or tags.
fn init_one(media: &Path) -> anyhow::Result<InitRow> {
    let existing = ["xmp", "XMP"]
        .into_iter()
        .map(|ext| media.with_added_extension(ext))
        .find(|path| path.exists());
    let xmp_path = existing
        .clone()
        .unwrap_or_else(|| media.with_added_extension("xmp"));
    let mut row = InitRow {
        path: xmp_path.to_string_lossy().into_owned(),
        media_type: infer_media_type(media)?.to_string(),
        ..Default::default()
    };
    let media_modified_time = media_modified_time(media);
    if let Some(time) = &media_modified_time {
        row.file_modified_time = iso_datetime_to_csv_format(time);
    }

    if existing.is_some() {
        let xmp = read_xmp(&xmp_path)?;
        if let Some(value) = xmp.property_date(xmp_ns::EXIF, "DateTimeOriginal") {
            row.datetime = csv_datetime(&value.value)?;
        }
        let (latitude, longitude) = extract_xmp_gps_coordinates(&xmp);
        row.latitude = latitude.unwrap_or_default();
        row.longitude = longitude.unwrap_or_default();
        row.xmp_status = "existing";
        return Ok(row);
    }

    let xmp = sidecar_from_media(media, &mut row, media_modified_time.as_deref())?;
    write_xmp_with_backup(&xmp_path, &xmp, true)?;
    log_line(&format!("Created {}", xmp_path.display()));
    row.xmp_status = "created";
    Ok(row)
}

fn media_modified_time(media: &Path) -> Option<String> {
    fs::metadata(media)
        .and_then(|metadata| metadata.modified())
        .ok()
        .map(|time| {
            DateTime::<Local>::from(time)
                .format("%Y-%m-%dT%H:%M:%S")
                .to_string()
        })
}

/// The sidecar `xmp init` would create for `media`, in memory: the media file's embedded XMP with the datetime
/// fields filled (DateTimeOriginal, else a usable CreateDate, else the file's modified time).
fn sidecar_from_media(
    media: &Path,
    row: &mut InitRow,
    media_modified_time: Option<&str>,
) -> anyhow::Result<XmpMeta> {
    let mut media_xmp = XmpFile::new()?;
    media_xmp
        .open_file(media, OpenFileOptions::default())
        .map_err(|err| anyhow::anyhow!("Failed to open file: {err}"))?;
    let xmp_result = (|| -> anyhow::Result<XmpMeta> {
        let mut xmp = media_xmp.xmp().unwrap_or_default();
        if let Some(value) = xmp.property_date(xmp_ns::EXIF, "DateTimeOriginal") {
            row.embedded_datetime_original_raw = csv_datetime(&value.value)?;
            row.datetime = row.embedded_datetime_original_raw.clone();
        }
        if let Some(value) = xmp.property_date(xmp_ns::XMP, "CreateDate") {
            row.embedded_create_date_raw = csv_datetime(&value.value)?;
        }
        let (latitude, longitude) = extract_xmp_gps_coordinates(&xmp);
        row.latitude = latitude.unwrap_or_default();
        row.longitude = longitude.unwrap_or_default();
        // Workaround for Exiv2 not recognizing this EXIF field in sidecars.
        xmp.delete_property(xmp_ns::EXIF, "DeviceSettingDescription")
            .map_err(anyhow::Error::from)?;
        Ok(xmp)
    })();
    let mut xmp = finalize_xmp_file(&mut media_xmp, xmp_result)?;

    let has_datetime_original = xmp.property(xmp_ns::EXIF, "DateTimeOriginal").is_some();
    let has_metadata_date = xmp.property(xmp_ns::XMP, "MetadataDate").is_some();
    if !has_datetime_original && !has_metadata_date {
        let create_date = xmp.property(xmp_ns::XMP, "CreateDate");
        let use_create_date = create_date.as_ref().is_some_and(|value| {
            !value.value.starts_with("1904-01-01") && !value.value.starts_with("1970-01-01")
        });
        if use_create_date {
            // Workaround for video files, as some manufacturer only write to xmp:CreateDate
            // And timezone is ignored for they write UTC-8 time but label as UTC
            // i.e. strip the timezone info in xmp:CreateDate and xmp:ModifyDate if there is
            // and skip the 0 timestamp if manufacturer write it
            row.datetime = match create_date.as_ref() {
                Some(value) if row.embedded_create_date_raw.is_empty() => {
                    iso_datetime_to_csv_format(&ignore_timezone(value.value.to_string())?)
                }
                _ => row.embedded_create_date_raw.clone(),
            };
            set_xmp_datetime_fields(&mut xmp, &row.datetime.replace(' ', "T"))?;
            strip_xmp_datetime_timezone(&mut xmp, xmp_ns::XMP, "CreateDate")?;
            strip_xmp_datetime_timezone(&mut xmp, xmp_ns::XMP, "ModifyDate")?;
        } else if let Some(time) = media_modified_time {
            // Fall back to the modified time of the file
            row.datetime = iso_datetime_to_csv_format(time);
            set_xmp_datetime_fields(&mut xmp, time)?;
        }
    }
    Ok(xmp)
}

/// Create missing sidecars for the media under `working_dir` and write a table
/// of every media file's datetime and GPS to `output_dir` (for review in
/// Caracal, and as input for `xmp update --datetime`).
pub fn init_xmp(working_dir: PathBuf, output_dir: PathBuf) -> anyhow::Result<()> {
    let media_paths = path_enumerate(working_dir.clone(), ResourceType::Media, None);
    let pb = crate::ui::progress_bar(media_paths.len() as u64, "write");
    let warnings = WarningCollector::default();
    // Files are independent; parallel reads pay off on NAS.
    let rows: Vec<InitRow> = media_paths
        .par_iter()
        .map(|media| {
            let row = init_one(media).unwrap_or_else(|err| {
                warnings.warn(&pb, format!("{}: {err}", media.display()));
                InitRow {
                    path: media
                        .with_added_extension("xmp")
                        .to_string_lossy()
                        .into_owned(),
                    media_type: infer_media_type(media).unwrap_or_default().to_string(),
                    xmp_status: "failed",
                    ..Default::default()
                }
            });
            pb.inc(1);
            row
        })
        .collect();
    pb.finish_and_clear();
    warnings.summarize();

    let count = |status: &str| rows.iter().filter(|row| row.xmp_status == status).count();
    let summary = format!(
        "{} XMP file(s) created, {} already existed, {} failed",
        count("created"),
        count("existing"),
        count("failed")
    );
    log_line(&summary);
    println!("{summary}");
    crate::ui::summary(serde_json::json!({
        "created": count("created"),
        "existing": count("existing"),
        "failed": count("failed"),
    }));
    write_init_table(&working_dir, &output_dir, &rows)?;
    let failed = count("failed");
    if failed > 0 {
        return Err(anyhow::anyhow!(
            "{failed} media file(s) could not be initialized, see the warnings above \
             (listed as failed in the table)"
        ));
    }
    Ok(())
}

fn write_init_table(working_dir: &Path, output_dir: &Path, rows: &[InitRow]) -> anyhow::Result<()> {
    let column = |name: &str, value: fn(&InitRow) -> &str| {
        Column::new(name.into(), rows.iter().map(value).collect::<Vec<_>>())
    };
    let mut df = DataFrame::new(
        rows.len(),
        vec![
            column(PATH_COLUMN, |row| &row.path),
            column(MEDIA_TYPE_COLUMN, |row| &row.media_type),
            column(DATETIME_COLUMN, |row| &row.datetime),
            column(LATITUDE_COLUMN, |row| &row.latitude),
            column(LONGITUDE_COLUMN, |row| &row.longitude),
            column(XMP_UPDATE_DATETIME_COLUMN, |_| ""),
            column("xmp_status", |row| row.xmp_status),
            column("embedded_datetime_original_raw", |row| {
                &row.embedded_datetime_original_raw
            }),
            column("embedded_create_date_raw", |row| {
                &row.embedded_create_date_raw
            }),
            column("file_modified_time", |row| &row.file_modified_time),
        ],
    )?;
    fs::create_dir_all(output_dir)?;
    let dir_name = working_dir
        .file_name()
        .map_or("unk".into(), |name| name.to_string_lossy());
    let timestamp = Local::now().format("%Y%m%d%H%M%S");
    let csv_path = output_dir.join(format!("xmp_init_{dir_name}_{timestamp}.csv"));
    let mut file = std::fs::File::create(&csv_path)?;
    CsvWriter::new(&mut file)
        .include_bom(true)
        .finish(&mut df)?;
    crate::ui::output("csv", &csv_path, Some(df.height()));
    Ok(())
}

type Metadata = (
    Vec<String>, // species
    Vec<String>, // individuals
    Vec<String>, // count
    Vec<String>, // sex
    Vec<String>, // bodyparts
    Vec<String>, // subjects
    String,      // datetime
    String,      // latitude
    String,      // longitude
    // String,      // datetime_digitized
    String, // time_modified
    String, // rating
);

fn retrieve_metadata(file_path: &Path, debug_mode: bool) -> anyhow::Result<Metadata> {
    // Retrieve metadata from given file
    // species, individual, bodypart, sex, count in digikam taglist / adobe hierarchicalsubject (species only), subject (for debugging),
    // datetime, datetime_digitized, rating and file modified time

    let mut f = XmpFile::new()?;
    f.open_file(file_path, OpenFileOptions::default())?;

    let mut species: Vec<String> = Vec::new();
    let mut individuals: Vec<String> = Vec::new();
    let mut count: Vec<String> = Vec::new();
    let mut sex: Vec<String> = Vec::new();
    let mut bodyparts: Vec<String> = Vec::new();
    let mut subjects: Vec<String> = Vec::new(); // for old digikam vesrion?
    let mut datetime = String::new();
    let mut latitude = String::new();
    let mut longitude = String::new();
    // let mut datetime_digitized = String::new();
    let mut time_modified = String::new();
    let mut rating = String::new();

    if debug_mode {
        // Sidecar mtimes are rewritten by serval itself; use the underlying
        // media file's mtime, falling back to the resource for orphan sidecars.
        let media_path = underlying_media_path(file_path);
        let file_metadata = if media_path.exists() {
            fs::metadata(&media_path)?
        } else {
            fs::metadata(file_path)?
        };
        let file_modified_time: DateTime<Local> = file_metadata.modified()?.into();
        time_modified = file_modified_time.format("%Y-%m-%dT%H:%M:%S").to_string();
    }
    let metadata_result = (|| -> anyhow::Result<Metadata> {
        if let Some(xmp) = f.xmp() {
            if let Some(value) = xmp.property_date(xmp_ns::EXIF, "DateTimeOriginal") {
                datetime = ignore_timezone(value.value.to_string())?;
            } else if let Some(value) = xmp.property_date(xmp_ns::XMP, "CreateDate") {
                // Workaround for video files, as some manufacturer only write to xmp:CreateDate
                // And timezone is ignored for they write UTC-8 time but label as UTC
                // i.e. we follow time shown in the picture without considering timezone in metadata
                // Ignore 0 timestamp in QuickTime:CreateDate, i.e. not start with 1904 and 1970
                if !value.value.to_string().starts_with("1904")
                    && !value.value.to_string().starts_with("1970")
                {
                    datetime = ignore_timezone(value.value.to_string())?;
                }
            }
            // if let Some(value) = xmp.property_date(xmp_ns::EXIF, "DateTimeDigitized") {
            //     datetime_digitized = ignore_timezone(value.value.to_string())?;
            // }
            if let Some(value) = xmp.property(xmp_ns::XMP, "Rating") {
                rating = value.value.to_string();
            }
            let (gps_latitude, gps_longitude) = extract_xmp_gps_coordinates(&xmp);
            latitude = gps_latitude.unwrap_or_default();
            longitude = gps_longitude.unwrap_or_default();
            if debug_mode {
                for property in xmp.property_array(xmp_ns::DC, "subject") {
                    subjects.push(property.value.to_string());
                }
            }

            // use adobe hierarchicalSubject if available (digikam also writes to this field)
            for property in xmp.property_array(LIGHTROOM_NS, LR_HIERARCHICAL_SUBJECT) {
                let tag = property.value;
                if tag.starts_with(TagType::Species.adobe_tag_prefix()) {
                    species.push(
                        tag.strip_prefix(TagType::Species.adobe_tag_prefix())
                            .unwrap()
                            .to_string(),
                    );
                } else if tag.starts_with(TagType::Individual.adobe_tag_prefix()) {
                    individuals.push(
                        tag.strip_prefix(TagType::Individual.adobe_tag_prefix())
                            .unwrap()
                            .to_string(),
                    );
                } else if tag.starts_with(TagType::Count.adobe_tag_prefix()) {
                    count.push(
                        tag.strip_prefix(TagType::Count.adobe_tag_prefix())
                            .unwrap()
                            .to_string(),
                    );
                } else if tag.starts_with(TagType::Sex.adobe_tag_prefix()) {
                    sex.push(
                        tag.strip_prefix(TagType::Sex.adobe_tag_prefix())
                            .unwrap()
                            .to_string(),
                    );
                } else if tag.starts_with(TagType::Bodypart.adobe_tag_prefix()) {
                    bodyparts.push(
                        tag.strip_prefix(TagType::Bodypart.adobe_tag_prefix())
                            .unwrap()
                            .to_string(),
                    );
                }
            }
        }
        Ok((
            species,
            individuals,
            count,
            sex,
            bodyparts,
            subjects,
            datetime,
            latitude,
            longitude,
            // datetime_digitized,
            time_modified,
            rating,
        ))
    })();
    finalize_xmp_file(&mut f, metadata_result)
}

pub fn get_classifications(
    file_dir: PathBuf,
    output_dir: PathBuf,
    resource_type: ResourceType,
    debug_mode: bool,
    volunteer_mode: bool, //TODO: make a mode argument
    deployment_level: Option<i32>,
    id_species: Option<Vec<String>>,
) -> anyhow::Result<()> {
    // Get tag info from the old digikam workflow in shanshui
    // by enumerating file_dir and read xmp metadata from resources

    let file_paths = path_enumerate(file_dir.clone(), resource_type, None);
    fs::create_dir_all(output_dir.clone())?;
    // Debug mode doubles as the info-table workflow (cf. xmp init --info):
    // ask which path level is the deployment so raw.csv gains a deployment column.
    let deploy_path_index = if debug_mode && !file_paths.is_empty() {
        let mut rl = Editor::new()?;
        rl.bind_sequence(
            Event::Any,
            EventHandler::Conditional(Box::new(NumericFilteringHandler)),
        );
        Some(prompt_deployment_path_index(
            &mut rl,
            file_paths[0].to_string_lossy().into_owned(),
            detect_deployment_path_index(file_paths.iter().map(|p| p.to_string_lossy())),
            deployment_level,
        )?)
    } else {
        None
    };
    // Determine output filename based on parameters
    let output_suffix = if volunteer_mode {
        String::new()
    } else {
        let file_name = file_dir
            .file_name()
            .map(|s| s.to_string_lossy())
            .unwrap_or_else(|| std::borrow::Cow::Borrowed("unk")); // For root dir

        let suffix = format!(
            "_{}_{}_{}.csv",
            file_name,
            resource_type.to_string().to_lowercase(),
            Local::now().format("%Y%m%d%H%M%S"),
        );
        suffix
    };

    let image_paths: Vec<String> = file_paths
        .iter()
        .map(|x| x.to_string_lossy().into_owned())
        .collect();
    let image_filenames: Vec<String> = file_paths
        .iter()
        .map(|x| x.file_name().unwrap().to_string_lossy().into_owned())
        .collect();
    let warnings = WarningCollector::default();
    // Keep resources whose media_type cannot be inferred (e.g. orphan sidecars
    // like orphan.xmp) in the output, with an empty media_type.
    let media_types: Vec<String> = file_paths
        .iter()
        .map(|path| match infer_media_type(path) {
            Ok(media_type) => media_type.to_string(),
            Err(_) => {
                warnings.warn_plain(format!(
                    "cannot infer media_type of {}, leaving it empty",
                    path.display()
                ));
                String::new()
            }
        })
        .collect();
    let num_images = file_paths.len();
    println!("Total {resource_type}: {num_images}.");
    let pb = crate::ui::progress_bar(num_images as u64, "read");

    let mut species_tags: Vec<String> = Vec::new();
    let mut individual_tags: Vec<String> = Vec::new();
    let mut count_tags: Vec<String> = Vec::new();
    let mut sex_tags: Vec<String> = Vec::new();
    let mut bodypart_tags: Vec<String> = Vec::new();
    let mut subjects: Vec<String> = Vec::new();
    let mut datetimes: Vec<String> = Vec::new();
    let mut latitudes: Vec<String> = Vec::new();
    let mut longitudes: Vec<String> = Vec::new();
    // let mut datetime_digitizeds: Vec<String> = Vec::new();
    let mut time_modifieds: Vec<String> = Vec::new();
    let mut ratings: Vec<String> = Vec::new();

    let result: Vec<_> = (0..num_images)
        .into_par_iter()
        .map(|i| {
            match retrieve_metadata(&file_paths[i], debug_mode) {
                Ok((
                    species,
                    individuals,
                    count,
                    sex,
                    bodyparts,
                    subjects,
                    datetime,
                    latitude,
                    longitude,
                    // datetime_digitized,
                    time_modified,
                    rating,
                )) => {
                    pb.inc(1);
                    (
                        species.join("|"),
                        individuals.join("|"),
                        count.join("|"),
                        sex.join("|"),
                        bodyparts.join("|"),
                        subjects.join("|"), // subject just for reviewing
                        datetime,
                        latitude,
                        longitude,
                        // datetime_digitized,
                        time_modified,
                        rating,
                    )
                }
                Err(error) => {
                    warnings.warn(&pb, format!("{} in {}", error, file_paths[i].display()));
                    pb.inc(1);
                    (
                        "".to_string(),
                        "".to_string(),
                        "".to_string(),
                        "".to_string(),
                        "".to_string(),
                        "".to_string(),
                        "".to_string(),
                        "".to_string(),
                        "".to_string(),
                        "".to_string(),
                        "".to_string(),
                    )
                }
            }
        })
        .collect();
    for tag in result {
        species_tags.push(tag.0);
        individual_tags.push(tag.1);
        count_tags.push(tag.2);
        sex_tags.push(tag.3);
        bodypart_tags.push(tag.4);
        subjects.push(tag.5);
        datetimes.push(tag.6);
        latitudes.push(tag.7);
        longitudes.push(tag.8);
        // datetime_digitizeds.push(tag.7);
        time_modifieds.push(tag.9);
        ratings.push(tag.10);
    }
    pb.finish();
    warnings.summarize();
    // Analysis
    let s_species = Column::new("species_tags".into(), species_tags);
    let s_individuals = Column::new("individual_tags".into(), individual_tags);
    let s_count = Column::new("count_tags".into(), count_tags);
    let s_sex = Column::new("sex_tags".into(), sex_tags);
    let s_bodyparts = Column::new("bodypart_tags".into(), bodypart_tags);
    let s_subjects = Column::new(SUBJECTS_COLUMN.into(), subjects);
    let s_datetime = Column::new(DATETIME_COLUMN.into(), datetimes);
    let s_latitude = Column::new(LATITUDE_COLUMN.into(), latitudes);
    let s_longitude = Column::new(LONGITUDE_COLUMN.into(), longitudes);
    // let s_datetime_digitized = Column::new("datetime_digitized".into(), datetime_digitizeds);
    let s_time_modified = Column::new(TIME_MODIFIED_COLUMN.into(), time_modifieds);
    let s_rating = Column::new(RATING_COLUMN.into(), ratings);

    let df_raw_height = image_paths.len();
    let mut df_raw = DataFrame::new(
        df_raw_height,
        vec![
            Column::new(PATH_COLUMN.into(), image_paths),
            Column::new(FILENAME_COLUMN.into(), image_filenames),
            Column::new(MEDIA_TYPE_COLUMN.into(), media_types),
            s_species,
            s_individuals,
            s_count,
            s_sex,
            s_bodyparts,
            s_subjects,
            s_datetime,
            s_latitude,
            s_longitude,
            // s_datetime_digitized,
            s_time_modified,
            s_rating,
        ],
    )?;
    if volunteer_mode {
        // println!("{:?}", df_raw);
        let mut df_empty_species = df_raw
            .clone()
            .lazy()
            .filter(col("species_tags").eq(lit("")))
            .collect()?;
        let num_xmp = df_raw.height();
        let num_tagged_sp = num_xmp - df_empty_species.height();
        let progress = if num_xmp > 0 {
            (num_tagged_sp as f64 / num_xmp as f64) * 100.0
        } else {
            0.0
        };

        println!("Species Labeling Progress: {progress:.2}%");

        let pb = ProgressBar::new(num_xmp as u64);
        pb.set_prefix("Species Labeling Progress:");
        pb.set_position(num_tagged_sp as u64);

        println!("Untagged xmp: {}", df_empty_species.height());

        let mut rl = rustyline::DefaultEditor::new()?;
        let input = rl.readline("Save CSV of files with missing tags for review? (y/n): ")?;

        if input.trim().eq_ignore_ascii_case("y") {
            let mut file = std::fs::File::create("serval_check_empty.csv")?;
            CsvWriter::new(&mut file)
                .include_bom(true)
                .finish(&mut df_empty_species)?;
        } else {
            println!("Skipping save.");
        }

        return Ok(());
    }
    let datetime_options = StrptimeOptions {
        // TODO: Serval does not include timezone info now
        format: Some("%Y-%m-%dT%H:%M:%S".into()),
        strict: false,
        ..Default::default()
    };
    let df_split = df_raw
        .clone()
        .lazy()
        .select([
            col(PATH_COLUMN),
            col(FILENAME_COLUMN),
            col(MEDIA_TYPE_COLUMN),
            col(DATETIME_COLUMN).str().strptime(
                DataType::Datetime(TimeUnit::Milliseconds, None),
                datetime_options.clone(),
                lit("raise"),
            ),
            col(LATITUDE_COLUMN),
            col(LONGITUDE_COLUMN),
            // col("datetime_digitized").str().strptime(
            //     DataType::Datetime(TimeUnit::Milliseconds, None),
            //     datetime_options.clone(),
            //     lit("raise"),
            // ),
            col(TIME_MODIFIED_COLUMN)
                .str()
                .to_datetime(
                    Some(TimeUnit::Milliseconds),
                    None,
                    datetime_options,
                    lit("raise"),
                )
                .dt()
                .replace_time_zone(None, lit("raise"), NonExistent::Raise),
            col("count_tags").alias(TagType::Count.col_name()),
            col("sex_tags").alias(TagType::Sex.col_name()),
            col("bodypart_tags").alias(TagType::Bodypart.col_name()),
            col(SUBJECTS_COLUMN),
            col(RATING_COLUMN),
        ])
        .collect()?;
    let df_pairs = species_individual_pairs(
        df_raw.column(PATH_COLUMN)?.str()?,
        df_raw.column("species_tags")?.str()?,
        df_raw.column("individual_tags")?.str()?,
        id_species.as_deref(),
    )?;

    if debug_mode {
        println!("{df_split:?}");
        if let Some(deploy_path_index) = deploy_path_index {
            df_raw = df_raw
                .lazy()
                .with_columns([
                    deployment_from_path_expr(col(PATH_COLUMN), deploy_path_index)
                        .alias("deployment"),
                    lit("").alias(XMP_UPDATE_DATETIME_COLUMN),
                ])
                .collect()?;
        }
        println!("{df_raw}");
        let debug_csv_path = output_dir.join(format!("raw{output_suffix}"));
        let mut file = std::fs::File::create(debug_csv_path.clone())?;
        CsvWriter::new(&mut file)
            .include_bom(true)
            .with_datetime_format(Some("%Y-%m-%d %H:%M:%S".into()))
            .finish(&mut df_raw)?;
        crate::ui::output("csv", &debug_csv_path, Some(df_raw.height()));
    }
    // One row per (species, individual) pair of an image.
    let df_flatten = df_pairs
        .lazy()
        .join(
            df_split.with_row_index("image".into(), None)?.lazy(),
            [col("image")],
            [col("image")],
            JoinArgs {
                maintain_order: MaintainOrderJoin::Left,
                ..JoinArgs::new(JoinType::Left)
            },
        )
        .drop(cols(["image"]))
        .sort(
            [PATH_COLUMN],
            SortMultipleOptions::default().with_maintain_order(true),
        )
        .collect()?;
    let mut df_flatten = canonicalize_observe_tags_df(df_flatten)?;
    println!("{df_flatten}");

    let tags_csv_path = output_dir.join(format!("tags{output_suffix}"));
    let mut file = std::fs::File::create(tags_csv_path.clone())?;
    CsvWriter::new(&mut file)
        .with_datetime_format(Some("%Y-%m-%d %H:%M:%S".into()))
        .include_bom(true)
        .finish(&mut df_flatten)?;
    crate::ui::output("csv", &tags_csv_path, Some(df_flatten.height()));

    // Number of images per species (an image with three foxes counts once).
    let mut df_count_species = df_flatten
        .clone()
        .lazy()
        .group_by([col(TagType::Species.col_name())])
        .agg([col(PATH_COLUMN).n_unique().alias("count")])
        .sort_by_exprs(
            [col("count"), col(TagType::Species.col_name())],
            SortMultipleOptions::default().with_order_descending_multi([true, false]),
        )
        .collect()?;
    println!("{df_count_species:?}");

    let species_stats_path = output_dir.join(format!("species_stats{output_suffix}"));
    let mut file = std::fs::File::create(species_stats_path.clone())?;
    CsvWriter::new(&mut file)
        .include_bom(true)
        .finish(&mut df_count_species)?;
    crate::ui::output("csv", &species_stats_path, Some(df_count_species.height()));
    Ok(())
}

/// The (species, individual) rows of each image, as columns image (index),
/// species, individual. XMP does not record which individual belongs to which
/// species, so an image with several species and individuals is ambiguous:
/// the user is asked which species are individually identified, and in an image
/// with exactly one of them the individuals go to that species. Otherwise every
/// individual is paired with every species, as before, with a warning.
fn species_individual_pairs(
    paths: &StringChunked,
    species_tags: &StringChunked,
    individual_tags: &StringChunked,
    given_id_species: Option<&[String]>,
) -> anyhow::Result<DataFrame> {
    let images: Vec<(&str, Vec<&str>, Vec<&str>)> =
        izip!(paths.iter(), species_tags.iter(), individual_tags.iter())
            .map(|(path, species, individuals)| {
                (
                    path.unwrap_or_default(),
                    species.unwrap_or_default().split('|').collect(),
                    individuals.unwrap_or_default().split('|').collect(),
                )
            })
            .collect();
    let is_ambiguous = |species: &[&str], individuals: &[&str]| {
        species.len() > 1 && individuals.iter().any(|i| !i.is_empty())
    };
    let ambiguous: Vec<&(&str, Vec<&str>, Vec<&str>)> = images
        .iter()
        .filter(|(_, species, individuals)| is_ambiguous(species, individuals))
        .collect();
    // `--id-species` answers the question; otherwise it is asked only on a terminal.
    let id_species: BTreeSet<String> = match given_id_species {
        Some(given) => given.iter().cloned().collect(),
        None if !ambiguous.is_empty() && crate::ui::can_ask() => ask_id_species(&ambiguous)?,
        None => BTreeSet::new(),
    };

    let (mut image_col, mut species_col, mut individual_col) = (Vec::new(), Vec::new(), Vec::new());
    let mut unresolved = Vec::new();
    for (index, (path, species, individuals)) in images.iter().enumerate() {
        let mut push = |s: &str, i: &str| {
            image_col.push(index as IdxSize);
            species_col.push(s.to_string());
            individual_col.push(i.to_string());
        };
        let identified: Vec<&&str> = species
            .iter()
            .filter(|s| id_species.contains(**s))
            .collect();
        if is_ambiguous(species, individuals) && identified.len() == 1 {
            for s in species {
                if s == identified[0] {
                    individuals.iter().for_each(|i| push(s, i));
                } else {
                    push(s, "");
                }
            }
        } else {
            if is_ambiguous(species, individuals) {
                unresolved.push((*path, species, individuals));
            }
            for i in individuals {
                species.iter().for_each(|s| push(s, i));
            }
        }
    }
    if !unresolved.is_empty() {
        report_unresolved(&unresolved);
    }
    Ok(DataFrame::new(
        image_col.len(),
        vec![
            Column::new("image".into(), image_col),
            Column::new(TagType::Species.col_name().into(), species_col),
            Column::new(TagType::Individual.col_name().into(), individual_col),
        ],
    )?)
}

/// Images whose individuals could not be given to one species: a warning for people, an `unresolved` event with
/// the species involved (image counts and the individual IDs seen with them) for programs.
fn report_unresolved(unresolved: &[(&str, &Vec<&str>, &Vec<&str>)]) {
    if crate::ui::json() {
        let mut species: std::collections::BTreeMap<&str, (usize, BTreeSet<&str>)> =
            Default::default();
        for (_, names, individuals) in unresolved {
            for name in names.iter().filter(|s| !s.is_empty()) {
                let entry = species.entry(name).or_default();
                entry.0 += 1;
                entry.1.extend(individuals.iter().filter(|i| !i.is_empty()));
            }
        }
        let species: Vec<_> = species
            .into_iter()
            .map(|(name, (images, individuals))| {
                serde_json::json!({"name": name, "images": images, "individuals": individuals})
            })
            .collect();
        let paths: Vec<&str> = unresolved
            .iter()
            .take(200)
            .map(|(path, _, _)| *path)
            .collect();
        crate::ui::event(serde_json::json!({
            "serval": "unresolved",
            "kind": "id-species",
            "count": unresolved.len(),
            "species": species,
            "paths": paths,
        }));
        return;
    }
    eprintln!(
        "Warning: {} image(s) with several species and individuals could not be \
         resolved; every individual is paired with every species there:",
        unresolved.len()
    );
    for (path, _, _) in unresolved.iter().take(5) {
        eprintln!("  {path}");
    }
    if unresolved.len() > 5 {
        eprintln!("  ... and {} more", unresolved.len() - 5);
    }
}

/// Ask which species are individually identified; empty without a terminal.
fn ask_id_species(ambiguous: &[&(&str, Vec<&str>, Vec<&str>)]) -> anyhow::Result<BTreeSet<String>> {
    let mut counts: std::collections::BTreeMap<&str, usize> = Default::default();
    for (_, species, _) in ambiguous {
        for s in species.iter().filter(|s| !s.is_empty()) {
            *counts.entry(s).or_default() += 1;
        }
    }
    let options: Vec<&str> = counts.keys().copied().collect();
    println!(
        "\n{} image(s) have several species and individual IDs (e.g. {}).",
        ambiguous.len(),
        ambiguous[0].0
    );
    println!("XMP does not record which individual belongs to which species.");
    println!("Species in these images:");
    for (n, species) in options.iter().enumerate() {
        println!("  {}) {species} ({} image(s))", n + 1, counts[species]);
    }
    if !io::stdin().is_terminal() {
        return Ok(BTreeSet::new());
    }
    let mut rl = rustyline::DefaultEditor::new()?;
    loop {
        let answer = rl.readline(
            "Which are individually identified? Numbers separated by commas, \
             empty = pair every individual with every species: ",
        )?;
        let chosen: Option<BTreeSet<String>> = answer
            .split(|c: char| c == ',' || c.is_whitespace())
            .filter(|part| !part.is_empty())
            .map(|part| {
                part.parse::<usize>()
                    .ok()
                    .and_then(|n| options.get(n.checked_sub(1)?))
                    .map(|species| species.to_string())
            })
            .collect();
        if let Some(chosen) = chosen {
            return Ok(chosen);
        }
    }
}

/// Options of `extract`. `keep_dirs` (folders kept above each file) and `on_existing` are asked on a terminal
/// when absent; without a terminal, a missing `keep_dirs` is an error and conflicts stop the run (exit code 3).
pub struct ExtractOptions {
    pub filter_type: ExtractFilterType,
    pub filter_value: String,
    pub rename: bool,
    pub skip_existing: bool,
    pub use_subdir: bool,
    pub subdir_type: SubdirType,
    pub keep_dirs: Option<usize>,
    pub on_existing: Option<OnConflict>,
}

pub fn extract_resources(
    csv_path: PathBuf,
    output_dir: PathBuf,
    options: ExtractOptions,
) -> anyhow::Result<()> {
    let ExtractOptions {
        filter_type,
        filter_value,
        rename,
        skip_existing,
        use_subdir,
        subdir_type: subdir_value,
        keep_dirs,
        on_existing,
    } = options;
    // Use subdir for default output_dir in case of overwrite
    let output_dir = if output_dir.ends_with("serval_extract") {
        let current_time = Local::now().format("%Y%m%d%H%M%S").to_string();
        // remove dot from output_dir
        let sanitized_filter_value = filter_value.replace('.', "");
        output_dir.join(format!("{current_time}_{sanitized_filter_value}"))
    } else {
        output_dir
    };

    let df = CsvReadOptions::default()
        .with_infer_schema_length(Some(0)) // parse all columns as string
        .with_ignore_errors(true)
        .with_parse_options(
            CsvParseOptions::default()
                .with_try_parse_dates(true)
                .with_missing_is_null(true),
        )
        .try_into_reader_with_file_path(Some(csv_path))?
        .finish()?;
    reject_duplicate_csv_columns(&df)?;
    // Create default values for missing columns
    // TODO: https://github.com/pola-rs/polars/issues/18372, wait for polars ergonomic improve
    let required_columns = [
        TagType::Species.col_name(),
        TagType::Individual.col_name(),
        "rating",
        "custom",
    ];

    let missing_columns = required_columns
        .iter()
        .filter(|col| {
            !df.get_column_names()
                .iter()
                .any(|name| name.as_str() == **col)
        })
        .map(|col| lit("").alias(*col))
        .collect::<Vec<_>>();
    let mut df_lazy = df.lazy();
    if !missing_columns.is_empty() {
        df_lazy = df_lazy.with_columns(missing_columns);
    }

    let df = df_lazy.collect()?;
    // --rename names each image after all of its tags, including rows the
    // filter drops, so every row of an image maps to the same file.
    let rename_prefixes = if rename {
        rename_prefixes(&df)?
    } else {
        HashMap::new()
    };

    let filter_expr = if filter_value == "ALL_VALUES" {
        match filter_type {
            ExtractFilterType::Species => col(TagType::Species.col_name()).is_not_null(),
            ExtractFilterType::Path => col("path").is_not_null(),
            ExtractFilterType::Individual => col(TagType::Individual.col_name()).is_not_null(),
            ExtractFilterType::Rating => col("rating").is_not_null(),
            ExtractFilterType::Event => col("event_id").is_not_null(),
            ExtractFilterType::Custom => col("custom").is_not_null(),
            ExtractFilterType::Advanced => {
                return Err(anyhow::anyhow!(
                    "Advanced filter requires a specific filter expression, not 'ALL_VALUES'"
                ));
            }
        }
    } else {
        match filter_type {
            ExtractFilterType::Species => {
                col(TagType::Species.col_name()).eq(lit(filter_value.clone()))
            }
            ExtractFilterType::Path => col("path")
                .str()
                .contains_literal(lit(filter_value.clone())),
            ExtractFilterType::Individual => {
                col(TagType::Individual.col_name()).eq(lit(filter_value.clone()))
            }
            ExtractFilterType::Rating => {
                // Support range syntax like "0-5" or "1-5", or exact match
                if let Some((min_str, max_str)) = filter_value.split_once('-') {
                    // Range filter
                    if let (Ok(min), Ok(max)) =
                        (min_str.trim().parse::<f64>(), max_str.trim().parse::<f64>())
                    {
                        // Cast to Float64 to handle decimal ratings
                        let rating_col = col("rating").cast(DataType::Float64);
                        rating_col
                            .clone()
                            .is_not_null()
                            .and(rating_col.clone().gt_eq(lit(min)))
                            .and(rating_col.lt_eq(lit(max)))
                    } else {
                        col("rating").eq(lit(filter_value.clone()))
                    }
                } else {
                    // Exact match
                    col("rating").eq(lit(filter_value.clone()))
                }
            }
            ExtractFilterType::Event => col("event_id").eq(lit(filter_value.clone())),
            ExtractFilterType::Custom => col("custom").eq(lit(filter_value.clone())),
            ExtractFilterType::Advanced => {
                // Parse the advanced filter expression
                let advanced_expr = parse_advanced_filter(&filter_value)?;

                // Same-field AND ("sp:A and sp:B") can only hold per image, not per
                // row: then each condition asks whether any row of the image matches.
                let per_image = has_same_field_and_conditions(&advanced_expr);
                if per_image {
                    println!("Matching conditions per image (a field is used twice with AND)");
                }
                filter_expr_to_polars(&advanced_expr, per_image)?
            }
        }
    };

    let df_filtered = df.lazy().filter(filter_expr).collect()?;

    // Check if any records match the filter
    if df_filtered.height() == 0 {
        return Err(anyhow::anyhow!("No records found matching the filter."));
    }

    println!("Found {} matching records", df_filtered.height());

    // How many folders above each file to keep: from --keep-dirs, else asked on a terminal.
    let path_sample = df_filtered
        .column("path")?
        .str()?
        .get(0)
        .ok_or_else(|| anyhow::anyhow!("Missing path value in the first filtered record"))?
        .to_string();
    let sample_dirs: Vec<PathBuf> = absolute_path(Path::new(&path_sample).to_path_buf())?
        .parent()
        .unwrap()
        .ancestors()
        .map(Path::to_path_buf)
        .collect();
    let num_option = i32::try_from(sample_dirs.len())?;
    let deploy_path_index = match keep_dirs {
        Some(keep) if keep <= sample_dirs.len() => keep,
        Some(keep) => {
            return Err(anyhow::anyhow!(
                "--keep-dirs {keep} is more than the {} folders above {path_sample}",
                sample_dirs.len()
            ));
        }
        None if crate::ui::can_ask() => {
            println!("Here is a sample of the file path ({path_sample}): ");
            println!("0): File Only (no directory)");
            for (i, entry) in sample_dirs.iter().enumerate() {
                println!("{}): {}", i + 1, entry.to_string_lossy());
            }
            let mut rl = Editor::new()?;
            rl.bind_sequence(
                Event::Any,
                EventHandler::Conditional(Box::new(NumericFilteringHandler)),
            );
            rl.set_helper(Some(NumericSelectValidator {
                min: 0,
                max: num_option,
                allow_empty: false,
            }));
            let readline = rl.readline("Select the top level directory to keep: ");
            readline?.trim().parse::<usize>()?
        }
        None => {
            return Err(anyhow::anyhow!(
                "Pass --keep-dirs N: how many folders above each file to keep (0 = file only)"
            ));
        }
    };
    let warnings = WarningCollector::default();
    let mut transfers = Vec::new();

    let paths = df_filtered.column("path")?.str()?;
    // Remove dot from tags, as it causes issues when cross-platform
    let species_tags = df_filtered
        .column(TagType::Species.col_name())?
        .str()?
        .replace_all(r"\.", "")?;
    let individual_tags = df_filtered
        .column(TagType::Individual.col_name())?
        .str()?
        .replace_all(r"\.", "")?;
    let rating_tags = df_filtered
        .column("rating")?
        .str()?
        .replace_all(r"\.", "")?;
    let custom_tags = df_filtered
        .column("custom")?
        .str()?
        .replace_all(r"\.", "")?;

    for (path, species_tag, individual_tag, rating_tag, custom_tag) in izip!(
        paths.iter(),
        species_tags.iter(),
        individual_tags.iter(),
        rating_tags.iter(),
        custom_tags.iter()
    ) {
        let subdir = if use_subdir {
            let (tag, fallback) = match subdir_value {
                SubdirType::Species => (species_tag, "untagged_species"),
                SubdirType::Individual => (individual_tag, "untagged_individual"),
                SubdirType::Rating => (rating_tag, "unrated"),
                SubdirType::Custom => (custom_tag, "no_custom"),
            };
            // The tag becomes a folder name: no "/" (nested folders) or
            // characters some file systems reject.
            tag.map(file_name_part)
                .filter(|name| !name.is_empty())
                .unwrap_or_else(|| fallback.to_string())
        } else {
            String::new()
        };
        let Some(path_str) = path else {
            warnings.warn_plain("Missing path value in tags CSV, skipping.");
            continue;
        };
        let media_path = underlying_media_path(Path::new(path_str));
        let (input_path_xmp, input_path_media) = if media_path == Path::new(path_str) {
            (format!("{path_str}.xmp"), path_str.to_string())
        } else {
            (
                path_str.to_string(),
                media_path.to_string_lossy().into_owned(),
            )
        };
        if !Path::new(&input_path_media).exists() {
            warnings.warn_plain(format!(
                "Skipping {path_str}: media file {input_path_media} does not exist"
            ));
            continue;
        }

        let filename_prefix = rename_prefixes.get(path_str).map_or("", String::as_str);
        // Target folder: the output root, plus the kept part of the source
        // folders, plus the subdirectory. The sidecar follows the media file.
        let input_media = Path::new(&input_path_media);
        let kept_dirs = if deploy_path_index == 0 {
            Path::new("")
        } else {
            let path_strip = input_media
                .ancestors()
                .nth(deploy_path_index + 1)
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "Failed to determine the preserved directory prefix for {}",
                        input_path_media
                    )
                })?;
            input_media.strip_prefix(path_strip)?.parent().unwrap()
        };
        let media_name = input_media.file_name().unwrap().to_string_lossy();
        let output_path_media = output_dir
            .join(kept_dirs)
            .join(subdir)
            .join(format!("{filename_prefix}{media_name}"));

        let sidecar = Path::new(&input_path_xmp);
        let sidecar = if sidecar.exists() {
            Some(sidecar.to_path_buf())
        } else {
            warnings.warn_plain(format!(
                "Missing XMP file for {input_path_media}, tag info for certain video files may be lost."
            ));
            None
        };
        transfers.push(Transfer {
            source: PathBuf::from(&input_path_media),
            sidecar,
            target: output_path_media,
            sidecar_slot: true,
        });
    }
    warnings.summarize();
    // --skip-existing predates the check below; it now just answers its question.
    let preset = on_existing.or(skip_existing.then_some(OnConflict::Skip));
    run_transfers(transfers, Mode::Copy, preset, false)?;
    crate::ui::output("folder", &output_dir, None);
    Ok(())
}

/// File name prefix per path for `extract --rename`:
/// "{species}__{individuals}__", each field the image's tags sorted and joined
/// by "+"; the individuals field is left out when empty, and an image without
/// species is "untagged".
fn rename_prefixes(df: &DataFrame) -> anyhow::Result<HashMap<String, String>> {
    let mut tags: HashMap<&str, (BTreeSet<String>, BTreeSet<String>)> = HashMap::new();
    for (path, species, individual) in izip!(
        df.column(PATH_COLUMN)?.str()?.iter(),
        df.column(TagType::Species.col_name())?.str()?.iter(),
        df.column(TagType::Individual.col_name())?.str()?.iter(),
    ) {
        let Some(path) = path else { continue };
        let (species_set, individual_set) = tags.entry(path).or_default();
        for (set, value) in [(species_set, species), (individual_set, individual)] {
            if let Some(value) = value.map(file_name_part).filter(|v| !v.is_empty()) {
                set.insert(value);
            }
        }
    }
    Ok(tags
        .into_iter()
        .map(|(path, (species, individuals))| {
            let species = join_capped(&species);
            let mut prefix = if species.is_empty() {
                "untagged".to_string()
            } else {
                species
            };
            prefix.push_str("__");
            let individuals = join_capped(&individuals);
            if !individuals.is_empty() {
                prefix.push_str(&individuals);
                prefix.push_str("__");
            }
            (path.to_string(), prefix)
        })
        .collect())
}

/// A tag as part of a file name: no dots (cross-platform issues) and no
/// characters that some file systems reject.
fn file_name_part(tag: &str) -> String {
    tag.chars()
        .filter(|c| *c != '.')
        .map(|c| {
            if matches!(c, '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|') || c.is_control() {
                '_'
            } else {
                c
            }
        })
        .collect::<String>()
        .trim()
        .to_string()
}

/// Join with "+", ending in "+N more" once the field would pass ~100 bytes
/// (file names are limited to 255 bytes; CJK tags take 3 bytes per character).
fn join_capped(values: &BTreeSet<String>) -> String {
    const MAX_BYTES: usize = 100;
    let mut joined = String::new();
    for (i, value) in values.iter().enumerate() {
        if !joined.is_empty() && joined.len() + 1 + value.len() > MAX_BYTES {
            joined.push_str(&format!("+{} more", values.len() - i));
            break;
        }
        if !joined.is_empty() {
            joined.push('+');
        }
        joined.push_str(value);
    }
    joined
}

/// Where the independence gap is measured from.
#[derive(Clone, Copy, Debug, PartialEq, clap::ValueEnum)]
pub enum MeasureFrom {
    /// The timer resets at every record (default).
    LastRecord,
    /// The timer resets only at records already kept as independent.
    LastIndependent,
}

/// What independence is analysed by.
#[derive(Clone, Copy, Debug, PartialEq, clap::ValueEnum)]
pub enum CaptureTarget {
    Species,
    Individual,
}

/// Options of `capture`. Each `None` is asked on a terminal, otherwise its default is used
/// (30 minutes, last record, species, the detected deployment level).
pub struct CaptureOptions {
    pub event: bool,
    pub no_exclude: bool,
    pub camtrap_dp: bool,
    pub min_gap: Option<i32>,
    pub measure_from: Option<MeasureFrom>,
    pub by: Option<CaptureTarget>,
    pub deployment_level: Option<i32>,
}

pub fn get_temporal_independence(
    csv_path: PathBuf,
    output_dir: PathBuf,
    options: CaptureOptions,
) -> anyhow::Result<()> {
    // Temporal independence analysis
    let CaptureOptions {
        event,
        no_exclude,
        camtrap_dp,
        min_gap,
        measure_from,
        by,
        deployment_level,
    } = options;

    // IDs such as "001" must stay text rather than be read as numbers.
    let id_columns = [
        "path",
        "species",
        "individual",
        DEPLOYMENT_ID_COLUMN,
        "observationID",
        "scientificName",
        "individualID",
    ]
    .map(|name| Field::new(name.into(), DataType::String));
    let mut read_opts = CsvReadOptions::default()
        .with_ignore_errors(false)
        .with_schema_overwrite(Some(Arc::new(Schema::from_iter(id_columns))));
    if camtrap_dp {
        read_opts = read_opts
            .with_columns(csv_projection_columns(&[
                "observationID",
                DEPLOYMENT_ID_COLUMN,
                "eventStart",
                "scientificName",
                "individualID",
            ]))
            .with_parse_options(CsvParseOptions::default());
    } else {
        read_opts =
            read_opts.with_parse_options(CsvParseOptions::default().with_try_parse_dates(true));
    }
    let df = match read_opts
        .try_into_reader_with_file_path(Some(csv_path))
        .and_then(|reader| reader.finish())
    {
        Ok(mut df) => {
            reject_duplicate_csv_columns(&df)?;
            if camtrap_dp {
                let event_col = df.column("eventStart")?;
                if event_col.null_count() > 0 {
                    return Err(anyhow::anyhow!(
                        "eventStart column contains empty values, please check."
                    ));
                }
            } else {
                // Old tags.csv files call the column datetime_original.
                if df.column(DATETIME_COLUMN).is_err() {
                    let _ = df.rename(LEGACY_DATETIME_COLUMN, DATETIME_COLUMN.into());
                }
                let datetime_col = df.column(DATETIME_COLUMN)?;
                // Check empty/null values first
                if datetime_col.null_count() > 0 {
                    return Err(anyhow::anyhow!(
                        "Datetime column contains empty values, please fill them before proceeding."
                    ));
                }
                // Check if the datetime column is parsed correctly, i.e. the type is not str
                if datetime_col.dtype() == &DataType::String {
                    return Err(anyhow::anyhow!(
                        "Datetime column parsing failed: column contains string data instead of datetime values.\n\
                        Hint: Ensure the datetime format in your file matches the pattern 'yyyy-MM-dd HH:mm:ss'."
                    ));
                }
            }
            df
        }
        Err(e) => {
            return Err(anyhow::anyhow!("Failed to read or parse CSV file: {e}"));
        }
    };

    // Parameters: from the flags, else asked on a terminal, else the defaults.
    let ask = crate::ui::can_ask();
    let mut rl = Editor::new()?;
    rl.bind_sequence(
        Event::Any,
        EventHandler::Conditional(Box::new(NumericFilteringHandler)), // Force numerical input
    );
    const DEFAULT_MIN_DELTA_TIME: i32 = 30;
    let min_delta_time: i32 = match min_gap {
        Some(minutes) if minutes >= 1 => minutes,
        Some(minutes) => {
            return Err(anyhow::anyhow!(
                "--min-gap must be at least 1 minute, got {minutes}"
            ));
        }
        None if ask => {
            // Empty input accepts the default; the validator otherwise requires a positive number.
            rl.set_helper(Some(NumericSelectValidator {
                min: 1,
                max: i32::MAX,
                allow_empty: true,
            }));
            let readline = rl.readline(
                "Minimum time gap in minutes for two records to count as independent [default 30]: ",
            );
            let trimmed = readline?.trim().to_string();
            if trimmed.is_empty() {
                DEFAULT_MIN_DELTA_TIME
            } else {
                trimmed
                    .parse()
                    .map_err(|_| anyhow::anyhow!("Invalid input: please enter a valid number"))?
            }
        }
        None => DEFAULT_MIN_DELTA_TIME,
    };
    if min_delta_time > 10080 {
        // 1 week
        println!("Note: {min_delta_time} minutes is unusually large (> 1 week)",);
    }
    let measure_from = match measure_from {
        Some(measure_from) => measure_from,
        None if ask => {
            rl.set_helper(Some(NumericSelectValidator {
                min: 1,
                max: 2,
                allow_empty: true,
            }));
            let readline = rl.readline(
                "\nMeasure that time gap from the previous:\n  1) independent record  - the timer resets only at records already kept as independent\n  2) record (default)    - the timer resets at every record\nEnter 1 or 2 [default 2]: ");
            match readline?.trim().parse() {
                Ok(1) => MeasureFrom::LastIndependent,
                _ => MeasureFrom::LastRecord, // "2" or empty default
            }
        }
        None => MeasureFrom::LastRecord,
    };
    let delta_time_compared_to = match measure_from {
        MeasureFrom::LastIndependent => "LastIndependentRecord",
        MeasureFrom::LastRecord => "LastRecord",
    };
    let by = match by {
        Some(by) => by,
        None if ask => {
            rl.set_helper(Some(NumericSelectValidator {
                min: 1,
                max: 2,
                allow_empty: true,
            }));
            let readline = rl.readline(
                "\nAnalyze independence by:\n  1) species (default)\n  2) individual ID\nEnter 1 or 2 [default 1]: ");
            match readline?.trim().parse() {
                Ok(2) => CaptureTarget::Individual,
                _ => CaptureTarget::Species, // "1" or empty default
            }
        }
        None => CaptureTarget::Species,
    };
    let target = match by {
        CaptureTarget::Species => TagType::Species,
        CaptureTarget::Individual => TagType::Individual,
    };
    // Find deployment
    let deploy_path_index = if camtrap_dp {
        None
    } else {
        let path_sample = df
            .column("path")?
            .str()?
            .get(0)
            .ok_or_else(|| anyhow::anyhow!("Missing path value in the first record"))?
            .to_string();
        let detected_index =
            detect_deployment_path_index(df.column("path")?.str()?.iter().flatten());
        Some(prompt_deployment_path_index(
            &mut rl,
            path_sample,
            detected_index,
            deployment_level,
        )?)
    };

    let mut exclude_expr = lit(false);
    for tag in DEFAULT_EXCLUDE_TAGS {
        let tag_expr = if tag.is_empty() {
            col(target.col_name()).eq(lit(""))
        } else {
            col(target.col_name()).str().starts_with(lit(*tag))
        };
        exclude_expr = exclude_expr.or(tag_expr);
    }

    // Data processing
    let id_col_name = if camtrap_dp { "observationID" } else { "path" };
    let df_deployment = if camtrap_dp {
        let path_col = "observationID";
        if df.column(path_col).is_err() {
            return Err(anyhow::anyhow!(
                "Missing observationID column in camtrap-dp input."
            ));
        }
        let target_col = match target {
            TagType::Species => "scientificName",
            TagType::Individual => "individualID",
            _ => unreachable!("capture prompt only allows species or individual"),
        };
        let time_expr = col("eventStart")
            .cast(DataType::String)
            .str()
            .replace_all(lit("T"), lit(" "), true)
            .str()
            .replace_all(lit(r"(\.\d+)?([+-]\d{2}:?\d{2}|Z)?$"), lit(""), false)
            .str()
            .strptime(
                DataType::Datetime(TimeUnit::Milliseconds, None),
                StrptimeOptions {
                    format: Some("%Y-%m-%d %H:%M:%S".into()),
                    strict: false,
                    exact: true,
                    cache: true,
                },
                lit("raise"),
            )
            .alias("time");
        let df_deployment = df
            .clone()
            .lazy()
            .select([
                col(path_col).alias(id_col_name),
                col(DEPLOYMENT_ID_COLUMN).alias("deployment"),
                time_expr,
                col(target_col).alias(target.col_name()),
            ])
            .collect()?;
        // eventStart nulls were rejected on read, so any null here is a parse failure
        // that drop_nulls would otherwise silently discard.
        let num_unparsed = df_deployment.column("time")?.null_count();
        if num_unparsed > 0 {
            return Err(anyhow::anyhow!(
                "{num_unparsed} eventStart value(s) could not be parsed: expected ISO-8601 like 2023-12-08T10:47:39+0800."
            ));
        }
        df_deployment
    } else {
        let deploy_path_index = deploy_path_index
            .ok_or_else(|| anyhow::anyhow!("Missing deployment path index selection"))?;
        df.clone()
            .lazy()
            .select([
                col(PATH_COLUMN).alias(id_col_name),
                deployment_from_path_expr(col(PATH_COLUMN), deploy_path_index).alias("deployment"),
                col(DATETIME_COLUMN).alias("time"),
                col(target.col_name()),
            ])
            .collect()?
    };

    let df_cleaned = if no_exclude {
        df_deployment
            .clone()
            .lazy()
            .drop_nulls(None)
            .unique_stable(
                Some(cols(vec![
                    "deployment".to_string(),
                    "time".to_string(),
                    target.col_name().to_string(),
                ])),
                UniqueKeepStrategy::First,
            )
            .collect()?
    } else {
        df_deployment
            .clone()
            .lazy()
            .drop_nulls(None)
            .filter(exclude_expr.not())
            .unique_stable(
                Some(cols(vec![
                    "deployment".to_string(),
                    "time".to_string(),
                    target.col_name().to_string(),
                ])),
                UniqueKeepStrategy::First,
            )
            .collect()?
    };

    // The temporal pass relies on contiguous [deployment, target] groups and ascending time.
    // Keep the sort stable so exact duplicate keys preserve input order deterministically.
    let df_sorted = df_cleaned.sort(
        ["deployment", target.col_name(), "time"],
        SortMultipleOptions::default().with_maintain_order(true),
    )?;

    let mut df_capture_independent;
    if delta_time_compared_to == "LastRecord" {
        df_capture_independent = df_sorted
            .clone()
            .lazy()
            .rolling(
                col("time"),
                [col("deployment"), col(target.col_name())],
                RollingGroupOptions {
                    period: Duration::parse(format!("{min_delta_time}m").as_str()),
                    offset: Duration::parse(format!("-{min_delta_time}m").as_str()),
                    closed_window: ClosedWindow::Right,
                    ..Default::default()
                },
            )
            .agg([
                col(target.col_name()).count().alias("count"),
                col(id_col_name).last(),
            ])
            .filter(col("count").eq(lit(1)))
            .select([
                col(id_col_name),
                col("deployment"),
                col("time"),
                col(target.col_name()),
            ])
            .collect()?;
        println!("{df_capture_independent}");
    } else {
        if df_sorted.height() == 0 {
            return Err(anyhow::anyhow!(
                "No records remain after filtering empty/default tags."
            ));
        }
        let time_col = df_sorted.column("time")?.datetime()?;
        let target_col = df_sorted.column(target.col_name())?.str()?;
        let deploy_col = df_sorted.column("deployment")?.str()?;
        let ticks_per_minute: i64 = match time_col.time_unit() {
            TimeUnit::Milliseconds => 60_000,
            TimeUnit::Microseconds => 60_000_000,
            TimeUnit::Nanoseconds => 60_000_000_000,
        };
        let min_delta_ticks = i64::from(min_delta_time) * ticks_per_minute;

        // Get temporal independent records
        let mut capture_independent = Vec::with_capacity(df_sorted.height());
        let mut last_indep: Option<(i64, &str, &str)> = None;
        for (time, species, deployment) in izip!(
            time_col.physical().iter(),
            target_col.iter(),
            deploy_col.iter()
        ) {
            let (time, species, deployment) = (
                time.ok_or_else(|| anyhow::anyhow!("Unexpected null time value"))?,
                species.ok_or_else(|| anyhow::anyhow!("Unexpected null target tag"))?,
                deployment.ok_or_else(|| anyhow::anyhow!("Unexpected null deployment"))?,
            );
            let independent = match last_indep {
                Some((last_time, last_species, last_deployment)) => {
                    species != last_species
                        || deployment != last_deployment
                        || time - last_time >= min_delta_ticks
                }
                None => true,
            };
            if independent {
                last_indep = Some((time, species, deployment));
            }
            capture_independent.push(independent);
        }

        df_capture_independent = df_sorted
            .lazy()
            .filter(Series::new("independent".into(), capture_independent).lit())
            .collect()?;
        println!("{df_capture_independent}");
    }

    // Include parameters in the output filename, LIR: Last Independent Record, LR: Last Record
    let output_suffix = format!(
        "_{}_{}m_{}.csv",
        target.to_string().to_lowercase(),
        min_delta_time,
        if delta_time_compared_to == "LastIndependentRecord" {
            "LIR"
        } else {
            "LR"
        },
    );
    fs::create_dir_all(output_dir.clone())?;
    let filename = format!("temporal-independence{output_suffix}");
    let mut file = std::fs::File::create(output_dir.join(filename.clone()))?;
    CsvWriter::new(&mut file)
        .include_bom(true)
        .with_datetime_format(Some("%Y-%m-%d %H:%M:%S".into()))
        .finish(&mut df_capture_independent)?;
    crate::ui::output("csv", &output_dir.join(&filename), None);

    if event {
        let df_events = df_capture_independent.with_row_index("event_id".into(), Some(1))?;
        let by_columns = &[target.col_name(), "deployment"];
        let df_raw_sorted = df_deployment.sort(
            ["deployment", target.col_name(), "time"],
            SortMultipleOptions::default().with_maintain_order(true),
        )?;
        let mut df_with_events = df_raw_sorted.join_asof_by(
            &df_events,
            "time",
            "time",
            by_columns,
            by_columns,
            AsofStrategy::Backward,
            None,
            true,
            false, // Sortedness of columns cannot be checked when 'by' groups provided
        )?;
        df_with_events = df_with_events
            .lazy()
            .select([
                col(id_col_name),
                col("deployment"),
                col("time"),
                col(target.col_name()),
                col("event_id"),
            ])
            .collect()?;
        let filename = format!("events{output_suffix}");
        let mut file = std::fs::File::create(output_dir.join(filename.clone()))?;
        CsvWriter::new(&mut file)
            .include_bom(true)
            .with_datetime_format(Some("%Y-%m-%d %H:%M:%S".into()))
            .finish(&mut df_with_events.clone())?;
        crate::ui::output("csv", &output_dir.join(&filename), None);
    }

    let mut df_count_independent = df_capture_independent
        .clone()
        .lazy()
        .group_by_stable([col("deployment"), col(target.col_name())])
        .agg([col(target.col_name()).count().alias("count")])
        .collect()?;
    println!("{df_count_independent}");

    let filename = format!("count_by_deployment{output_suffix}");
    let mut file = std::fs::File::create(output_dir.join(&filename))?;
    CsvWriter::new(&mut file)
        .include_bom(true)
        .with_datetime_format(Some("%Y-%m-%d %H:%M:%S".into()))
        .finish(&mut df_count_independent)?;
    crate::ui::output("csv", &output_dir.join(&filename), None);

    if target == TagType::Species {
        let mut df_count_independent_species = df_capture_independent
            .clone()
            .lazy()
            .group_by_stable([col(TagType::Species.col_name())])
            .agg([col(TagType::Species.col_name()).count().alias("count")])
            .collect()?;
        println!("{df_count_independent_species}");

        let filename = format!("count_all{output_suffix}");
        let mut file = std::fs::File::create(output_dir.join(&filename))?;
        CsvWriter::new(&mut file)
            .include_bom(true)
            .with_datetime_format(Some("%Y-%m-%d %H:%M:%S".into()))
            .finish(&mut df_count_independent_species)?;
        crate::ui::output("csv", &output_dir.join(&filename), None);
    }
    Ok(())
}

/// One `xmp_update` row of the CSV: replace `old` with `new` (insert `new`
/// when `old` is empty). `row` is the CSV line number, for error messages.
struct UpdateOp {
    row: usize,
    old: String,
    new: String,
}

fn read_xmp(file_path: &Path) -> anyhow::Result<XmpMeta> {
    let xmp_content = fs::read_to_string(file_path)?;
    XmpMeta::from_str_with_options(&xmp_content, FromStrOptions::default())
        .map_err(|e| anyhow::anyhow!("Failed to parse XMP: {e:?}"))
}

/// Apply all update rows of one file to `xmp`, judged against its current
/// content. Returns `Ok(false)` when the file already shows the result (e.g.
/// on a rerun after an interrupted update), so there is nothing to write.
fn apply_update_ops(
    xmp: &mut XmpMeta,
    update_type: XmpUpdateType,
    ops: &[UpdateOp],
) -> anyhow::Result<bool> {
    match update_type.tag_type() {
        Some(tag_type) => apply_tag_ops(xmp, tag_type, ops),
        None => apply_rating_ops(xmp, ops),
    }
}

fn apply_rating_ops(xmp: &mut XmpMeta, ops: &[UpdateOp]) -> anyhow::Result<bool> {
    let new_value = single_target(ops, "Rating")?;
    let current = xmp
        .property(xmp_ns::XMP, "Rating")
        .map(|value| value.value)
        .unwrap_or_default();
    if current == new_value {
        return Ok(false);
    }
    if let Some(op) = ops
        .iter()
        .find(|op| !op.old.is_empty() && op.old != current)
    {
        return Err(anyhow::anyhow!(
            "Rating mismatch (row {}): expected '{}', found '{}'",
            op.row,
            op.old,
            current
        ));
    }
    xmp.set_property(xmp_ns::XMP, "Rating", &XmpValue::new(new_value.to_string()))?;
    Ok(true)
}

fn apply_tag_ops(xmp: &mut XmpMeta, tag_type: TagType, ops: &[UpdateOp]) -> anyhow::Result<bool> {
    // Merge duplicate rows; the same old tag may map to only one new tag.
    let mut replace: Vec<&UpdateOp> = Vec::new();
    let mut inserts: Vec<&str> = Vec::new();
    for op in ops {
        if op.old.is_empty() {
            if !inserts.contains(&op.new.as_str()) {
                inserts.push(&op.new);
            }
        } else if op.old != op.new {
            match replace.iter().find(|r| r.old == op.old) {
                Some(r) if r.new != op.new => {
                    return Err(anyhow::anyhow!(
                        "conflicting updates for '{}': row {} -> '{}', row {} -> '{}'",
                        op.old,
                        r.row,
                        r.new,
                        op.row,
                        op.new
                    ));
                }
                Some(_) => {}
                None => replace.push(op),
            }
        }
    }

    // hierarchicalSubject is the reference: every old tag must be there, unless
    // the file already shows the whole result.
    let adobe = |value: &str| format!("{}{value}", tag_type.adobe_tag_prefix());
    let current: Vec<String> = xmp
        .property_array(LIGHTROOM_NS, LR_HIERARCHICAL_SUBJECT)
        .map(|item| item.value)
        .collect();
    let has = |value: &str| current.contains(&adobe(value));
    let missing: Vec<&&UpdateOp> = replace.iter().filter(|r| !has(&r.old)).collect();
    if !missing.is_empty() {
        let is_new =
            |value: &str| replace.iter().any(|r| r.new == value) || inserts.contains(&value);
        let already_applied = replace.iter().all(|r| has(&r.new))
            && inserts.iter().all(|value| has(value))
            && replace.iter().all(|r| is_new(&r.old) || !has(&r.old));
        if already_applied {
            return Ok(false);
        }
        let expected: Vec<String> = missing
            .iter()
            .map(|r| format!("'{}' (row {})", adobe(&r.old), r.row))
            .collect();
        return Err(anyhow::anyhow!(
            "Tag mismatch: expected {} in {}",
            expected.join(", "),
            LR_HIERARCHICAL_SUBJECT
        ));
    }

    let mut changed = false;
    for (ns, array_name, prefix) in [
        (
            LIGHTROOM_NS,
            LR_HIERARCHICAL_SUBJECT,
            tag_type.adobe_tag_prefix(),
        ),
        (DIGIKAM_NS, DIGIKAM_TAGSLIST, tag_type.digikam_tag_prefix()),
        (xmp_ns::DC, "subject", ""),
    ] {
        let replace: Vec<(String, String)> = replace
            .iter()
            .map(|r| (format!("{prefix}{}", r.old), format!("{prefix}{}", r.new)))
            .collect();
        let inserts: Vec<String> = inserts.iter().map(|v| format!("{prefix}{v}")).collect();
        changed |= rewrite_tag_array(xmp, ns, array_name, &replace, &inserts)?;
    }
    Ok(changed)
}

/// Rewrite one tag array in place: map every item through `replace` (all
/// against the original items, so A->B plus B->C gives B, C), drop duplicates
/// of the resulting new tags, then append missing `inserts`. The array keeps
/// its type (bag/seq). Returns whether anything changed.
fn rewrite_tag_array(
    xmp: &mut XmpMeta,
    ns: &str,
    array_name: &str,
    replace: &[(String, String)],
    inserts: &[String],
) -> anyhow::Result<bool> {
    let is_target = |value: &str| {
        replace.iter().any(|(_, new)| new == value) || inserts.iter().any(|v| v == value)
    };
    let mut changed = false;
    let mut kept: Vec<String> = Vec::new();
    let mut len = xmp.array_len(ns, array_name);
    let mut i = 1;
    while i <= len {
        let item_path = format!("{array_name}[{i}]");
        let Some(value) = xmp.property(ns, &item_path).map(|p| p.value) else {
            i += 1;
            continue;
        };
        let mapped = replace
            .iter()
            .find(|(old, _)| *old == value)
            .map_or(value.clone(), |(_, new)| new.clone());
        if is_target(&mapped) && kept.contains(&mapped) {
            xmp.delete_property(ns, &item_path)?;
            len -= 1;
            changed = true;
            continue;
        }
        if mapped != value {
            xmp.set_property(ns, &item_path, &XmpValue::new(mapped.clone()))?;
            changed = true;
        }
        kept.push(mapped);
        i += 1;
    }
    for value in inserts {
        if !kept.contains(value) {
            let array = XmpValue::new(array_name.to_string()).set_is_array(true);
            xmp.append_array_item(ns, &array, &XmpValue::new(value.clone()))?;
            kept.push(value.clone());
            changed = true;
        }
    }
    Ok(changed)
}

/// Serialize `xmp` to `file_path` atomically; with `backup`, an existing file
/// is kept as a timestamped .backup first.
fn write_xmp_with_backup(file_path: &Path, xmp: &XmpMeta, backup: bool) -> anyhow::Result<()> {
    let modified_xmp =
        xmp.to_string_with_options(ToStringOptions::default().set_newline("\n".to_string()))?;
    let timestamp = chrono::Local::now().format("%Y%m%d_%H%M%S").to_string();
    write_with_backup(file_path, &modified_xmp, &timestamp, backup)
}

/// Replace `file_path` with `content` via a temp file and rename. With `backup`,
/// an existing file is first copied to `<file>.<timestamp>.backup`; backups
/// never replace an earlier one, so several writes within one second each keep
/// theirs.
fn write_with_backup(
    file_path: &Path,
    content: &str,
    timestamp: &str,
    backup: bool,
) -> anyhow::Result<()> {
    if backup && file_path.exists() {
        let (mut backup, _) = create_new_sibling(file_path, timestamp, "backup")?;
        io::copy(&mut fs::File::open(file_path)?, &mut backup)?;
    }
    let (mut temp, temp_path) = create_new_sibling(file_path, timestamp, "tmp")?;
    let result = temp.write_all(content.as_bytes()).and_then(|()| {
        drop(temp);
        fs::rename(&temp_path, file_path)
    });
    if let Err(err) = result {
        let _ = fs::remove_file(&temp_path);
        return Err(anyhow::anyhow!(
            "Failed to write {}: {err}",
            file_path.display()
        ));
    }
    Ok(())
}

/// Apply the `xmp_update` column to the listed XMP files. Rows are grouped per
/// file and every file is checked before any is written, so a bad CSV changes
/// nothing. Files that already show the result are skipped, so rerunning the
/// same CSV after an interruption finishes the job.
pub fn update_tags(csv_path: PathBuf, update_type: XmpUpdateType) -> anyhow::Result<()> {
    let tag_column_name = update_type.col_name();
    let df = CsvReadOptions::default()
        .with_infer_schema_length(Some(0))
        .with_columns(csv_projection_columns(&[
            PATH_COLUMN,
            XMP_UPDATE_COLUMN,
            tag_column_name,
        ]))
        .with_ignore_errors(false)
        .try_into_reader_with_file_path(Some(csv_path))?
        .finish()?;
    reject_duplicate_csv_columns(&df)?;

    let df_filtered = df
        .with_row_index(ROW_COLUMN.into(), Some(2))?
        .lazy()
        .filter(col(XMP_UPDATE_COLUMN).is_not_null())
        .select([
            col(ROW_COLUMN),
            col(PATH_COLUMN),
            col(XMP_UPDATE_COLUMN),
            col(tag_column_name),
        ])
        .collect()?;

    let warnings = WarningCollector::default();
    let groups = group_update_rows(
        izip!(
            df_filtered.column(ROW_COLUMN)?.idx()?.iter(),
            df_filtered.column(PATH_COLUMN)?.str()?.iter(),
            df_filtered.column(tag_column_name)?.str()?.iter(),
            df_filtered.column(XMP_UPDATE_COLUMN)?.str()?.iter(),
        ),
        &warnings,
    );
    XmpMeta::register_namespace(LIGHTROOM_NS, "lr")?;
    XmpMeta::register_namespace(DIGIKAM_NS, "digiKam")?;
    let counts = apply_xmp_updates(&groups, &warnings, false, read_xmp, |xmp, ops| {
        apply_update_ops(xmp, update_type, ops)
    })?;
    report_updates(&counts);
    Ok(())
}

/// CSV line number column; the header is line 1, so data starts at 2.
const ROW_COLUMN: &str = "csv_row";

/// Group update rows (line, path, old value, new value) by XMP file. Rows
/// without a new value are ignored; rows not pointing at an XMP file are
/// skipped with a warning.
fn group_update_rows<'a>(
    rows: impl Iterator<
        Item = (
            Option<IdxSize>,
            Option<&'a str>,
            Option<&'a str>,
            Option<&'a str>,
        ),
    >,
    warnings: &WarningCollector,
) -> Vec<(PathBuf, Vec<UpdateOp>)> {
    let mut groups: std::collections::BTreeMap<PathBuf, Vec<UpdateOp>> = Default::default();
    for (row, path, old, new) in rows {
        let row = row.unwrap_or_default() as usize;
        let new = new.unwrap_or("");
        if new.is_empty() {
            continue;
        }
        let Some(path_str) = path else {
            warnings.warn_plain(format!("Missing xmp path (row {row}), skipping."));
            continue;
        };
        let path = PathBuf::from(path_str);
        if resource_extension(&path).as_deref() != Some("xmp") {
            warnings.warn_plain(format!("Skipping non-XMP file (row {row}): {path_str}"));
            continue;
        }
        groups.entry(path).or_default().push(UpdateOp {
            row,
            old: old.unwrap_or("").to_string(),
            new: new.to_string(),
        });
    }
    let groups: Vec<_> = groups.into_iter().collect();
    let num_rows: usize = groups.iter().map(|(_, ops)| ops.len()).sum();
    println!(
        "Found {num_rows} rows with updates in {} files",
        groups.len()
    );
    groups
}

/// Apply grouped updates with `apply` (which returns whether the file changes).
/// Every file is checked before any is written, so a bad CSV changes nothing;
/// files that already show the result are skipped, so rerunning the same CSV
/// after an interruption finishes the job.
/// Files that `apply_xmp_updates` changed (or would change) and that already showed the result.
struct UpdateCounts {
    changed: usize,
    already: usize,
}

/// Apply per-file updates with `apply` (which returns whether the file changes) to files read with `load`.
/// Every file is checked before any is written, so a bad input changes nothing; files that already show the
/// result are skipped, so rerunning finishes an interrupted run. With `dry_run`, only the check runs.
fn apply_xmp_updates<T: Sync>(
    groups: &[(PathBuf, T)],
    warnings: &WarningCollector,
    dry_run: bool,
    load: impl Fn(&Path) -> anyhow::Result<XmpMeta> + Sync,
    apply: impl Fn(&mut XmpMeta, &T) -> anyhow::Result<bool> + Sync,
) -> anyhow::Result<UpdateCounts> {
    // Pass 1: check every file in memory, write nothing.
    let pb = crate::ui::progress_bar(groups.len() as u64, "plan");
    pb.set_message("Checking XMP files...");
    let checks: Vec<anyhow::Result<bool>> = groups
        .par_iter()
        .map(|(path, ops)| {
            let result = load(path).and_then(|mut xmp| apply(&mut xmp, ops));
            pb.inc(1);
            result
        })
        .collect();
    pb.finish_and_clear();

    let mut failed = 0;
    let mut to_write = Vec::new();
    for ((path, ops), check) in groups.iter().zip(checks) {
        match check {
            Ok(true) => to_write.push((path, ops)),
            Ok(false) => {}
            Err(err) => {
                failed += 1;
                let message = format!("{}: {err}", path.display());
                log_line(&format!("Error: {message}"));
                if crate::ui::json() {
                    crate::ui::event(serde_json::json!({
                        "serval": "error",
                        "path": path.to_string_lossy(),
                        "message": err.to_string(),
                    }));
                } else {
                    eprintln!("Error: {message}");
                }
            }
        }
    }
    if failed > 0 {
        warnings.summarize();
        return Err(anyhow::anyhow!(
            "{failed} file(s) cannot be updated, no file was changed. Fix the CSV and rerun."
        ));
    }
    let already = groups.len() - to_write.len();
    if dry_run {
        warnings.summarize();
        return Ok(UpdateCounts {
            changed: to_write.len(),
            already,
        });
    }

    // Pass 2: write. Each file is replaced atomically; if this stops midway,
    // rerunning the same CSV skips the files already updated.
    let pb = crate::ui::progress_bar(to_write.len() as u64, "write");
    pb.set_message("Updating XMP files...");
    for (done, (path, ops)) in to_write.iter().enumerate() {
        pb_status(&pb, format!("Updating {}", path.display()));
        let result = load(path).and_then(|mut xmp| {
            apply(&mut xmp, ops)?;
            write_xmp_with_backup(path, &xmp, true)
        });
        if let Err(err) = result {
            pb.abandon();
            return Err(err.context(format!(
                "Stopped at {} after updating {done} of {} file(s); rerun the same CSV to finish",
                path.display(),
                to_write.len()
            )));
        }
        pb.inc(1);
    }
    pb.finish_and_clear();
    warnings.summarize();
    Ok(UpdateCounts {
        changed: to_write.len(),
        already,
    })
}

fn report_updates(counts: &UpdateCounts) {
    let summary = format!(
        "{} file(s) updated, {} already up to date",
        counts.changed, counts.already
    );
    log_line(&summary);
    println!("{summary}");
    crate::ui::summary(serde_json::json!({
        "updated": counts.changed,
        "already": counts.already,
        "failed": 0,
    }));
}

/// Set the datetime of the listed XMP files from `xmp_update_datetime`, with the
/// same check-first and rerun behavior as `update_tags`.
pub fn update_datetime(csv_path: PathBuf) -> anyhow::Result<()> {
    let df = CsvReadOptions::default()
        .with_columns(csv_projection_columns(&[
            PATH_COLUMN,
            XMP_UPDATE_DATETIME_COLUMN,
        ]))
        .with_ignore_errors(false)
        .try_into_reader_with_file_path(Some(csv_path))?
        .finish()?;
    reject_duplicate_csv_columns(&df)?;

    let df_filtered = df
        .with_row_index(ROW_COLUMN.into(), Some(2))?
        .lazy()
        .filter(col(XMP_UPDATE_DATETIME_COLUMN).is_not_null())
        .select([
            col(ROW_COLUMN),
            col(PATH_COLUMN),
            col(XMP_UPDATE_DATETIME_COLUMN)
                .str()
                .to_datetime(
                    None,
                    None,
                    StrptimeOptions::default(),
                    lit("raise"), // Tell Polars how to handle errors
                )
                .alias(XMP_UPDATE_DATETIME_COLUMN),
        ])
        .collect()?;

    // Check if the datetime column is parsed correctly
    let datetime_col = df_filtered.column(XMP_UPDATE_DATETIME_COLUMN)?;
    if datetime_col.dtype() == &DataType::String {
        return Err(anyhow::anyhow!(
            "XMP update datetime column parsing failed: column contains string data instead of datetime values.\n\
            Hint: Ensure the datetime format in your file matches the pattern 'yyyy-MM-dd HH:mm:ss'."
        ));
    }
    let datetime_strings = datetime_col.datetime()?.to_string("%Y-%m-%dT%H:%M:%S")?;

    let warnings = WarningCollector::default();
    let groups = group_update_rows(
        izip!(
            df_filtered.column(ROW_COLUMN)?.idx()?.iter(),
            df_filtered.column(PATH_COLUMN)?.str()?.iter(),
            std::iter::repeat(None),
            datetime_strings.iter(),
        ),
        &warnings,
    );
    let counts = apply_xmp_updates(
        &groups,
        &warnings,
        false,
        read_xmp,
        |xmp, ops: &Vec<UpdateOp>| apply_datetime_ops(xmp, ops),
    )?;
    report_updates(&counts);
    Ok(())
}

fn apply_datetime_ops(xmp: &mut XmpMeta, ops: &[UpdateOp]) -> anyhow::Result<bool> {
    let new_value = single_target(ops, "datetime")?;
    let target = naive_datetime_to_xmp(new_value)?.to_string();
    let current = |ns: &str, name: &str| xmp.property_date(ns, name).map(|v| v.value.to_string());
    if current(xmp_ns::EXIF, "DateTimeOriginal").as_ref() == Some(&target)
        && current(xmp_ns::PHOTOSHOP, "DateCreated").as_ref() == Some(&target)
    {
        return Ok(false);
    }
    set_xmp_datetime_fields(xmp, new_value)?;
    Ok(true)
}

/// The one new value all rows of a file agree on, or an error naming the rows.
fn single_target<'a>(ops: &'a [UpdateOp], what: &str) -> anyhow::Result<&'a str> {
    let new_value = &ops[0].new;
    match ops.iter().find(|op| &op.new != new_value) {
        Some(op) => Err(anyhow::anyhow!(
            "conflicting {what} updates: row {} sets '{}', row {} sets '{}'",
            ops[0].row,
            new_value,
            op.row,
            op.new
        )),
        None => Ok(new_value),
    }
}

/// A field `xmp write` can set from a table.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, clap::ValueEnum)]
pub enum WriteField {
    Species,
    Individual,
    Rating,
    Datetime,
}

impl WriteField {
    const ALL: [WriteField; 4] = [
        WriteField::Species,
        WriteField::Individual,
        WriteField::Rating,
        WriteField::Datetime,
    ];

    fn name(self) -> &'static str {
        match self {
            WriteField::Species => "species",
            WriteField::Individual => "individual",
            WriteField::Rating => "rating",
            WriteField::Datetime => "datetime",
        }
    }
}

/// What one file should say after `xmp write`; `None` leaves the field as it is.
#[derive(Default)]
struct Desired {
    index: usize,
    /// The sidecar does not exist yet (written even if the labels already match the media file's XMP).
    missing: bool,
    species: Option<Vec<String>>,
    individuals: Option<Vec<String>>,
    rating: Option<String>,
    datetime: Option<String>,
}

/// What `xmp write` changed in one file, for the summary.
#[derive(Default, Clone)]
struct WriteChange {
    fields: Vec<WriteField>,
    removed_values: bool,
}

/// Write the labels in a table into the sidecars as they are: for every file in the table, each chosen field
/// becomes exactly the table's value (species and individuals collected from all the file's rows). Empty cells
/// leave the field unchanged; other tags and metadata are kept.
pub fn write_tags(
    csv_path: PathBuf,
    fields: &[WriteField],
    dry_run: bool,
    create_missing: bool,
) -> anyhow::Result<()> {
    let fields: BTreeSet<WriteField> = if fields.is_empty() {
        WriteField::ALL.into_iter().collect()
    } else {
        fields.iter().copied().collect()
    };
    let mut df = CsvReadOptions::default()
        .with_infer_schema_length(Some(0))
        .with_ignore_errors(false)
        .try_into_reader_with_file_path(Some(csv_path))?
        .finish()?;
    reject_duplicate_csv_columns(&df)?;
    let has_column =
        |df: &DataFrame, name: &str| df.get_column_names().iter().any(|c| c.as_str() == name);
    if !has_column(&df, DATETIME_COLUMN) && has_column(&df, LEGACY_DATETIME_COLUMN) {
        df.rename(LEGACY_DATETIME_COLUMN, DATETIME_COLUMN.into())?;
    }
    if !has_column(&df, PATH_COLUMN) {
        return Err(anyhow::anyhow!("The table has no 'path' column"));
    }
    let column = |name: &str| -> anyhow::Result<Option<StringChunked>> {
        Ok(if has_column(&df, name) {
            Some(df.column(name)?.str()?.clone())
        } else {
            None
        })
    };
    let pick = |field: WriteField, name: &str| -> anyhow::Result<Option<StringChunked>> {
        if fields.contains(&field) {
            column(name)
        } else {
            Ok(None)
        }
    };
    let paths = df.column(PATH_COLUMN)?.str()?.clone();
    let species = pick(WriteField::Species, TagType::Species.col_name())?;
    let individuals = pick(WriteField::Individual, TagType::Individual.col_name())?;
    let ratings = pick(WriteField::Rating, RATING_COLUMN)?;
    let datetimes = pick(WriteField::Datetime, DATETIME_COLUMN)?;
    let cell = |values: &Option<StringChunked>, i: usize| -> Option<String> {
        values
            .as_ref()
            .and_then(|v| v.get(i))
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .map(str::to_string)
    };

    // Group rows by sidecar; species/individuals collect, rating/datetime must agree.
    let warnings = WarningCollector::default();
    let mut order: Vec<PathBuf> = Vec::new();
    let mut desired: std::collections::HashMap<PathBuf, (Desired, Vec<usize>, Vec<String>)> =
        Default::default();
    for i in 0..df.height() {
        let row = i + 2; // the header is line 1
        let Some(path) = paths.get(i).map(str::trim).filter(|p| !p.is_empty()) else {
            warnings.warn_plain(format!("Missing path (row {row}), skipping."));
            continue;
        };
        let path = PathBuf::from(path);
        let sidecar = sidecar_for(path);
        let entry = desired.entry(sidecar.clone()).or_insert_with(|| {
            order.push(sidecar.clone());
            (Desired::default(), Vec::new(), Vec::new())
        });
        let (want, rows, problems) = entry;
        rows.push(row);
        for (list, value) in [
            (&mut want.species, cell(&species, i)),
            (&mut want.individuals, cell(&individuals, i)),
        ] {
            if let Some(value) = value {
                let list = list.get_or_insert_with(Vec::new);
                if !list.contains(&value) {
                    list.push(value);
                }
            }
        }
        if let Some(rating) = cell(&ratings, i) {
            if rating.parse::<i32>().is_err() {
                problems.push(format!("rating '{rating}' is not a number (row {row})"));
            } else if want.rating.as_ref().is_some_and(|r| r != &rating) {
                problems.push(format!("different ratings for the same file (row {row})"));
            } else {
                want.rating = Some(rating);
            }
        }
        if let Some(datetime) = cell(&datetimes, i) {
            let iso = datetime.replace(' ', "T");
            if NaiveDateTime::parse_from_str(&iso, "%Y-%m-%dT%H:%M:%S").is_err() {
                problems.push(format!(
                    "datetime '{datetime}' is not yyyy-MM-dd HH:mm:ss (row {row})"
                ));
            } else if want.datetime.as_ref().is_some_and(|d| d != &iso) {
                problems.push(format!("different datetimes for the same file (row {row})"));
            } else {
                want.datetime = Some(iso);
            }
        }
    }
    let mut table_problems = 0;
    for sidecar in &order {
        let (_, rows, problems) = &desired[sidecar];
        if !problems.is_empty() {
            table_problems += 1;
            let rows_text = rows
                .iter()
                .map(|r| r.to_string())
                .collect::<Vec<_>>()
                .join(", ");
            let message = format!(
                "{} (rows {rows_text}): {}",
                sidecar.display(),
                problems.join("; ")
            );
            log_line(&format!("Error: {message}"));
            if crate::ui::json() {
                crate::ui::event(serde_json::json!({
                    "serval": "error",
                    "path": sidecar.to_string_lossy(),
                    "rows": rows,
                    "message": problems.join("; "),
                }));
            } else {
                eprintln!("Error: {message}");
            }
        }
    }
    if table_problems > 0 {
        return Err(anyhow::anyhow!(
            "{table_problems} file(s) have conflicting or invalid values in the table, no file was changed. Fix the table and rerun."
        ));
    }
    let groups: Vec<(PathBuf, Desired)> = order
        .into_iter()
        .enumerate()
        .map(|(index, sidecar)| {
            let (mut want, _, _) = desired.remove(&sidecar).unwrap();
            want.index = index;
            want.missing = !sidecar.exists();
            (sidecar, want)
        })
        .collect();
    println!("Found {} files in the table", groups.len());

    XmpMeta::register_namespace(LIGHTROOM_NS, "lr")?;
    XmpMeta::register_namespace(DIGIKAM_NS, "digiKam")?;
    let changes: std::sync::Mutex<Vec<Option<WriteChange>>> =
        std::sync::Mutex::new(vec![None; groups.len()]);
    let load = |sidecar: &Path| -> anyhow::Result<XmpMeta> {
        if sidecar.exists() {
            return read_xmp(sidecar);
        }
        if !create_missing {
            return Err(anyhow::anyhow!(
                "no XMP sidecar; pass --create-missing to create it from the media file"
            ));
        }
        let media = underlying_media_path(sidecar);
        sidecar_from_media(
            &media,
            &mut InitRow::default(),
            media_modified_time(&media).as_deref(),
        )
    };
    let apply = |xmp: &mut XmpMeta, want: &Desired| -> anyhow::Result<bool> {
        let mut change = WriteChange::default();
        for (field, list, tag_type) in [
            (WriteField::Species, &want.species, TagType::Species),
            (
                WriteField::Individual,
                &want.individuals,
                TagType::Individual,
            ),
        ] {
            if let Some(list) = list {
                let (changed, removed) = set_tag_list(xmp, tag_type, list)?;
                if changed {
                    change.fields.push(field);
                }
                change.removed_values |= removed;
            }
        }
        if let Some(rating) = &want.rating {
            let current = xmp.property(xmp_ns::XMP, "Rating").map(|v| v.value);
            if current.as_deref() != Some(rating.as_str()) {
                xmp.set_property(xmp_ns::XMP, "Rating", &XmpValue::new(rating.clone()))?;
                change.fields.push(WriteField::Rating);
            }
        }
        if let Some(datetime) = &want.datetime {
            // Tables show local time without the offset (as observe reads it), so a matching value that has an
            // offset is left as it is.
            let target = naive_datetime_to_xmp(datetime)?.to_string();
            let matches = |ns: &str, name: &str| {
                xmp.property_date(ns, name).is_some_and(|v| {
                    v.value
                        .to_string()
                        .strip_prefix(&target)
                        .is_some_and(|offset| {
                            offset.is_empty() || offset.starts_with(['+', '-', 'Z'])
                        })
                })
            };
            if !matches(xmp_ns::EXIF, "DateTimeOriginal")
                || !matches(xmp_ns::PHOTOSHOP, "DateCreated")
            {
                set_xmp_datetime_fields(xmp, datetime)?;
                change.fields.push(WriteField::Datetime);
            }
        }
        let changed = !change.fields.is_empty();
        changes.lock().unwrap()[want.index] = Some(change);
        Ok(changed || want.missing)
    };
    let counts = apply_xmp_updates(&groups, &warnings, dry_run, load, apply)?;

    // Per-field summary from the check pass.
    let changes = changes.into_inner().unwrap();
    let per_field: Vec<(WriteField, usize)> = WriteField::ALL
        .into_iter()
        .filter(|f| fields.contains(f))
        .map(|f| {
            let n = changes
                .iter()
                .flatten()
                .filter(|c| c.fields.contains(&f))
                .count();
            (f, n)
        })
        .collect();
    let removed = changes
        .iter()
        .flatten()
        .filter(|c| c.removed_values)
        .count();
    let created = groups.iter().filter(|(_, want)| want.missing).count();
    let verb = if dry_run { "would change" } else { "changed" };
    let fields_text = per_field
        .iter()
        .map(|(f, n)| format!("{} {n}", f.name()))
        .collect::<Vec<_>>()
        .join(", ");
    let mut summary = format!(
        "{} file(s) {verb} ({fields_text}), {} already up to date",
        counts.changed, counts.already
    );
    if removed > 0 {
        summary.push_str(&format!(
            "; {removed} file(s) lose values that are not in the table"
        ));
    }
    if created > 0 {
        summary.push_str(&format!(
            "; {created} sidecar(s) {} from the media files",
            if dry_run {
                "would be created"
            } else {
                "created"
            }
        ));
    }
    log_line(&summary);
    println!("{summary}");
    let mut event = serde_json::json!({
        "dry_run": dry_run,
        "changed": counts.changed,
        "already": counts.already,
        "removed_values": removed,
        "created_sidecars": created,
        "failed": 0,
    });
    for (field, n) in per_field {
        event["fields"][field.name()] = serde_json::json!(n);
    }
    crate::ui::summary(event);
    Ok(())
}

/// The sidecar of `path`: the path itself for an .xmp file, else the media file's existing `.xmp`/`.XMP` sidecar,
/// else where `xmp init` would create it.
fn sidecar_for(path: PathBuf) -> PathBuf {
    if resource_extension(&path).as_deref() == Some("xmp") {
        return path;
    }
    ["xmp", "XMP"]
        .into_iter()
        .map(|ext| path.with_added_extension(ext))
        .find(|p| p.exists())
        .unwrap_or_else(|| path.with_added_extension("xmp"))
}

/// The species or individual list of a file, from hierarchicalSubject (as observe reads it).
fn tag_list(xmp: &XmpMeta, tag_type: TagType) -> Vec<String> {
    let adobe = tag_type.adobe_tag_prefix();
    xmp.property_array(LIGHTROOM_NS, LR_HIERARCHICAL_SUBJECT)
        .filter_map(|item| item.value.strip_prefix(adobe).map(str::to_string))
        .collect()
}

/// Make the species or individual list of a file exactly `desired`: entries of that field not in `desired` are
/// removed and missing ones added, in hierarchicalSubject, digiKam's TagsList and dc:subject. hierarchicalSubject
/// is the reference for what the file says now. Returns (changed, removed some value).
fn set_tag_list(
    xmp: &mut XmpMeta,
    tag_type: TagType,
    desired: &[String],
) -> anyhow::Result<(bool, bool)> {
    let adobe = tag_type.adobe_tag_prefix();
    let current = tag_list(xmp, tag_type);
    let removed: Vec<&String> = current.iter().filter(|v| !desired.contains(v)).collect();
    let added: Vec<&String> = desired.iter().filter(|v| !current.contains(v)).collect();
    if removed.is_empty() && added.is_empty() {
        return Ok((false, false));
    }
    for (ns, array_name, prefix) in [
        (LIGHTROOM_NS, LR_HIERARCHICAL_SUBJECT, adobe),
        (DIGIKAM_NS, DIGIKAM_TAGSLIST, tag_type.digikam_tag_prefix()),
        (xmp_ns::DC, "subject", ""),
    ] {
        let mut i = xmp.array_len(ns, array_name);
        while i >= 1 {
            let item_path = format!("{array_name}[{i}]");
            if let Some(value) = xmp.property(ns, &item_path).map(|p| p.value)
                && let Some(bare) = value.strip_prefix(prefix)
                && removed.iter().any(|r| r.as_str() == bare)
            {
                xmp.delete_property(ns, &item_path)?;
            }
            i -= 1;
        }
        let present: Vec<String> = xmp
            .property_array(ns, array_name)
            .map(|p| p.value)
            .collect();
        for value in &added {
            let full = format!("{prefix}{value}");
            if !present.contains(&full) {
                let array = XmpValue::new(array_name.to_string()).set_is_array(true);
                xmp.append_array_item(ns, &array, &XmpValue::new(full))?;
            }
        }
    }
    Ok((true, !removed.is_empty()))
}

/// A file's labels as observe reads them: species and individuals from hierarchicalSubject, rating (0 when
/// there is none), DateTimeOriginal or else a usable CreateDate.
fn read_labels(xmp: &XmpMeta) -> anyhow::Result<Labels> {
    let date = |ns: &str, name: &str| xmp.property_date(ns, name).map(|v| v.value.to_string());
    let datetime = date(xmp_ns::EXIF, "DateTimeOriginal").or_else(|| {
        date(xmp_ns::XMP, "CreateDate")
            .filter(|value| !value.starts_with("1904") && !value.starts_with("1970"))
    });
    let rating = match xmp.property(xmp_ns::XMP, "Rating") {
        // xmp:Rating is a real number in the XMP specification
        Some(value) => value
            .value
            .parse::<f64>()
            .map(|r| r.round() as i32)
            .map_err(|_| anyhow::anyhow!("invalid rating '{}'", value.value))?,
        None => 0,
    };
    Ok(Labels {
        species: tag_list(xmp, TagType::Species),
        individuals: tag_list(xmp, TagType::Individual),
        rating,
        datetime: match datetime {
            Some(value) => Some(iso_datetime_to_csv_format(&ignore_timezone(value)?)),
            None => None,
        },
    })
}

/// Print one JSON line per state on stdout. A path that is not valid UTF-8 cannot be written as JSON and fails
/// the run, naming the path, rather than being changed.
fn print_states(states: &[FileState]) -> anyhow::Result<()> {
    let mut out = io::stdout().lock();
    for state in states {
        let line = serde_json::to_string(state)
            .map_err(|err| anyhow::anyhow!("{}: {err}", state.path.to_string_lossy()))?;
        writeln!(out, "{line}")?;
    }
    Ok(())
}

/// `xmp get`: print the labels of each file as one JSON line on stdout, in the order given. `paths` are sidecars
/// or media files; without paths, they are read from stdin, one per line.
pub fn get_labels(paths: Vec<PathBuf>) -> anyhow::Result<()> {
    let paths = if paths.is_empty() {
        io::stdin()
            .lines()
            .map(|line| line.map(|l| PathBuf::from(l.trim())))
            .filter(|line| !line.as_ref().is_ok_and(|p| p.as_os_str().is_empty()))
            .collect::<io::Result<Vec<_>>>()?
    } else {
        paths
    };
    let pb = crate::ui::progress_bar(paths.len() as u64, "read");
    let states: Vec<FileState> = paths
        .into_par_iter()
        .map(|path| {
            let sidecar = sidecar_for(path);
            let mut state = FileState {
                exists: sidecar.exists(),
                path: sidecar,
                result: None,
                labels: None,
                modified: None,
                message: None,
            };
            if state.exists {
                match read_xmp(&state.path).and_then(|xmp| read_labels(&xmp)) {
                    Ok(labels) => {
                        state.labels = Some(labels);
                        state.modified = modified_millis(&state.path);
                    }
                    Err(err) => state.message = Some(format!("{err:#}")),
                }
            }
            pb.inc(1);
            state
        })
        .collect();
    pb.finish_and_clear();
    print_states(&states)
}

/// The sidecar's modification time in milliseconds since the Unix epoch, so Waxbill can tell when a file changed.
fn modified_millis(path: &Path) -> Option<u64> {
    fs::metadata(path)
        .and_then(|m| m.modified())
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_millis() as u64)
}

/// Keep only the first backup of each file: files that already have one in their folder.
#[derive(Default)]
struct FirstBackups(std::sync::Mutex<HashMap<PathBuf, std::sync::Arc<BTreeSet<String>>>>);

impl FirstBackups {
    fn has_backup(&self, file: &Path) -> bool {
        let dir = file.parent().unwrap_or(Path::new(".")).to_path_buf();
        let names = self
            .0
            .lock()
            .unwrap()
            .entry(dir.clone())
            .or_insert_with(|| {
                // `<name>.<timestamp>.backup` → `<name>`, listed once per folder
                let names = fs::read_dir(&dir)
                    .into_iter()
                    .flatten()
                    .flatten()
                    .filter_map(|entry| {
                        let name = entry.file_name().to_string_lossy().into_owned();
                        let stem = name.strip_suffix(".backup")?;
                        Some(stem.rsplit_once('.')?.0.to_string())
                    })
                    .collect();
                std::sync::Arc::new(names)
            })
            .clone();
        file.file_name()
            .is_some_and(|name| names.contains(name.to_string_lossy().as_ref()))
    }
}

/// How `xmp set` keeps backups.
#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
pub enum BackupMode {
    /// One backup per write (as every other command)
    Each,
    /// Only each file's original: no new backup when the file already has one
    First,
}

/// `xmp set`: apply edits read as JSON lines from stdin (`FileEdit`), written once per file. Prints one result per
/// file on stdout (`written`, `unchanged`, `conflict` or `error`) with the labels now on disk. A file whose labels
/// no longer match `expect` is a conflict and is left untouched. Malformed input stops the run before anything is
/// written.
pub fn set_labels(backup: BackupMode, create_missing: bool) -> anyhow::Result<()> {
    let mut order: Vec<PathBuf> = Vec::new();
    let mut groups: HashMap<PathBuf, Vec<FileEdit>> = HashMap::new();
    for (i, line) in io::stdin().lines().enumerate() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let edit = serde_json::from_str::<FileEdit>(&line)
            .map_err(anyhow::Error::from)
            .and_then(|edit| validate_edit(&edit).map(|()| edit))
            .map_err(|err| anyhow::anyhow!("Edit on line {}: {err}", i + 1))?;
        let sidecar = sidecar_for(edit.path.clone());
        groups
            .entry(sidecar.clone())
            .or_insert_with(|| {
                order.push(sidecar.clone());
                Vec::new()
            })
            .push(edit);
    }
    XmpMeta::register_namespace(LIGHTROOM_NS, "lr")?;
    XmpMeta::register_namespace(DIGIKAM_NS, "digiKam")?;
    let first_backups = FirstBackups::default();
    let pb = crate::ui::progress_bar(order.len() as u64, "write");
    let states: Vec<FileState> = order
        .par_iter()
        .map(|sidecar| {
            let mut state = FileState {
                path: sidecar.clone(),
                result: Some(Outcome::Error),
                exists: sidecar.exists(),
                labels: None,
                modified: None,
                message: None,
            };
            match set_one(
                sidecar,
                &groups[sidecar],
                backup,
                create_missing,
                &first_backups,
            ) {
                Ok((outcome, message, labels)) => {
                    match (&outcome, &message) {
                        (Outcome::Written, _) => {
                            log_line(&format!("Updated {}", sidecar.display()))
                        }
                        (_, Some(message)) => {
                            log_line(&format!("Conflict: {}: {message}", sidecar.display()))
                        }
                        _ => {}
                    }
                    state.result = Some(outcome);
                    state.message = message;
                    state.exists = sidecar.exists();
                    state.labels = Some(labels);
                    state.modified = modified_millis(sidecar);
                }
                Err(err) => {
                    log_line(&format!("Error: {}: {err:#}", sidecar.display()));
                    state.message = Some(format!("{err:#}"));
                }
            }
            pb.inc(1);
            state
        })
        .collect();
    pb.finish_and_clear();
    let count = |outcome: Outcome| states.iter().filter(|s| s.result == Some(outcome)).count();
    crate::ui::summary(serde_json::json!({
        "written": count(Outcome::Written),
        "unchanged": count(Outcome::Unchanged),
        "conflict": count(Outcome::Conflict),
        "failed": count(Outcome::Error),
    }));
    print_states(&states)
}

/// Values the files can hold: ratings -1–5, datetimes `yyyy-MM-dd HH:mm:ss`, no empty tags.
fn validate_edit(edit: &FileEdit) -> anyhow::Result<()> {
    for labels in [&edit.expect, &edit.set] {
        if let Some(rating) = labels.rating {
            anyhow::ensure!((-1..=5).contains(&rating), "rating {rating} is not -1–5");
        }
        if let Some(datetime) = &labels.datetime {
            anyhow::ensure!(
                NaiveDateTime::parse_from_str(datetime, "%Y-%m-%d %H:%M:%S").is_ok(),
                "datetime '{datetime}' is not yyyy-MM-dd HH:mm:ss"
            );
        }
    }
    let tags = [&edit.set.species, &edit.set.individuals]
        .into_iter()
        .flatten()
        .flatten()
        .chain(&edit.add.species)
        .chain(&edit.add.individuals);
    for tag in tags {
        anyhow::ensure!(!tag.trim().is_empty(), "empty species or individual");
    }
    Ok(())
}

/// Whether the file's current labels show what Waxbill expected. Lists compare as sets.
fn expect_matches(expect: &EditLabels, current: &Labels) -> bool {
    let same_set = |expected: &Option<Vec<String>>, current: &[String]| {
        expected.as_ref().is_none_or(|expected| {
            expected.iter().collect::<BTreeSet<_>>() == current.iter().collect::<BTreeSet<_>>()
        })
    };
    same_set(&expect.species, &current.species)
        && same_set(&expect.individuals, &current.individuals)
        && expect.rating.is_none_or(|r| current.rating == r)
        && expect
            .datetime
            .as_ref()
            .is_none_or(|d| current.datetime.as_ref() == Some(d))
}

/// Apply one file's edits. Returns the outcome, a message for a conflict, and the labels now on disk.
fn set_one(
    sidecar: &Path,
    edits: &[FileEdit],
    backup: BackupMode,
    create_missing: bool,
    first_backups: &FirstBackups,
) -> anyhow::Result<(Outcome, Option<String>, Labels)> {
    let missing = !sidecar.exists();
    let mut xmp = if !missing {
        read_xmp(sidecar)?
    } else if create_missing {
        let media = underlying_media_path(sidecar);
        sidecar_from_media(
            &media,
            &mut InitRow::default(),
            media_modified_time(&media).as_deref(),
        )?
    } else {
        anyhow::bail!("no XMP sidecar (create it with --create-missing)");
    };
    let before = read_labels(&xmp)?;
    if !edits
        .iter()
        .all(|edit| expect_matches(&edit.expect, &before))
    {
        let message = "the file's labels changed since they were shown".to_string();
        return Ok((Outcome::Conflict, Some(message), before));
    }
    let mut after = before.clone();
    for edit in edits {
        for (list, set, add, remove) in [
            (
                &mut after.species,
                &edit.set.species,
                &edit.add.species,
                &edit.remove.species,
            ),
            (
                &mut after.individuals,
                &edit.set.individuals,
                &edit.add.individuals,
                &edit.remove.individuals,
            ),
        ] {
            if let Some(set) = set {
                list.clear();
                add_missing(list, set);
            }
            add_missing(list, add);
            list.retain(|v| !remove.contains(v));
        }
        if let Some(rating) = edit.set.rating {
            after.rating = rating;
        }
        if edit.set.datetime.is_some() {
            after.datetime = edit.set.datetime.clone();
        }
    }
    if after == before && !missing {
        return Ok((Outcome::Unchanged, None, before));
    }
    for (tag_type, list) in [
        (TagType::Species, &after.species),
        (TagType::Individual, &after.individuals),
    ] {
        set_tag_list(&mut xmp, tag_type, list)?;
    }
    if after.rating != before.rating {
        match after.rating {
            0 => xmp.delete_property(xmp_ns::XMP, "Rating")?,
            rating => {
                xmp.set_property(xmp_ns::XMP, "Rating", &XmpValue::new(rating.to_string()))?
            }
        }
    }
    if after.datetime != before.datetime
        && let Some(datetime) = &after.datetime
    {
        set_xmp_datetime_fields(&mut xmp, &datetime.replace(' ', "T"))?;
    }
    let keep_backup = backup == BackupMode::Each || !first_backups.has_backup(sidecar);
    write_xmp_with_backup(sidecar, &xmp, keep_backup)?;
    Ok((Outcome::Written, None, read_labels(&xmp)?))
}

fn add_missing(list: &mut Vec<String>, values: &[String]) {
    for v in values {
        if !list.contains(v) {
            list.push(v.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backups_within_one_second_do_not_overwrite() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("a.jpg.xmp");
        fs::write(&file, "original").unwrap();
        write_with_backup(&file, "first", "20260930_120000", true).unwrap();
        write_with_backup(&file, "second", "20260930_120000", true).unwrap();

        assert_eq!(fs::read_to_string(&file).unwrap(), "second");
        let backup = |name: &str| fs::read_to_string(dir.path().join(name)).unwrap();
        assert_eq!(backup("a.jpg.xmp.20260930_120000.backup"), "original");
        assert_eq!(backup("a.jpg.xmp.20260930_120000_1.backup"), "first");
        // no temp files left behind
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 3);
    }

    fn species_xmp(species: &[&str]) -> XmpMeta {
        let items = |prefix: &str| {
            species
                .iter()
                .map(|s| format!("<rdf:li>{prefix}{s}</rdf:li>"))
                .collect::<String>()
        };
        XmpMeta::from_str(&format!(
            r#"<x:xmpmeta xmlns:x="adobe:ns:meta/"><rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">
            <rdf:Description rdf:about="" xmlns:lr="{LIGHTROOM_NS}" xmlns:digiKam="{DIGIKAM_NS}" xmlns:dc="http://purl.org/dc/elements/1.1/">
            <lr:hierarchicalSubject><rdf:Bag>{}</rdf:Bag></lr:hierarchicalSubject>
            <digiKam:TagsList><rdf:Seq>{}</rdf:Seq></digiKam:TagsList>
            <dc:subject><rdf:Bag>{}</rdf:Bag></dc:subject>
            </rdf:Description></rdf:RDF></x:xmpmeta>"#,
            items("Species|"),
            items("Species/"),
            items(""),
        ))
        .unwrap()
    }

    fn ops(rows: &[(&str, &str)]) -> Vec<UpdateOp> {
        rows.iter()
            .enumerate()
            .map(|(i, (old, new))| UpdateOp {
                row: i + 2,
                old: old.to_string(),
                new: new.to_string(),
            })
            .collect()
    }

    fn species_of(xmp: &XmpMeta) -> Vec<String> {
        let values = |ns: &str, name: &str, prefix: &str| -> Vec<String> {
            xmp.property_array(ns, name)
                .map(|item| item.value.strip_prefix(prefix).unwrap().to_string())
                .collect()
        };
        let adobe = values(LIGHTROOM_NS, LR_HIERARCHICAL_SUBJECT, "Species|");
        assert_eq!(adobe, values(DIGIKAM_NS, DIGIKAM_TAGSLIST, "Species/"));
        assert_eq!(adobe, values(xmp_ns::DC, "subject", ""));
        adobe
    }

    #[test]
    fn rename_prefix_lists_all_tags_of_an_image() {
        let many: Vec<String> = (0..30).map(|i| format!("Species {i:02}")).collect();
        let mut paths = vec!["a", "a", "a", "b", "c"];
        let mut species = vec![Some("Pika"), Some("Fox"), Some("Fox"), None, Some("W/lf.")];
        let mut individuals = vec![Some("F03"), Some("F01"), None, None, Some("")];
        for name in &many {
            paths.push("d");
            species.push(Some(name.as_str()));
            individuals.push(None);
        }
        let df = df!(
            PATH_COLUMN => paths,
            TagType::Species.col_name() => species,
            TagType::Individual.col_name() => individuals,
        )
        .unwrap();
        let prefixes = rename_prefixes(&df).unwrap();
        assert_eq!(prefixes["a"], "Fox+Pika__F01+F03__");
        assert_eq!(prefixes["b"], "untagged__");
        assert_eq!(prefixes["c"], "W_lf__");
        assert!(prefixes["d"].starts_with("Species 00+Species 01+"));
        assert!(prefixes["d"].ends_with(" more__") && prefixes["d"].len() < 120);
    }

    #[test]
    fn written_tag_list_replaces_the_files_list() {
        let mut xmp = species_xmp(&["Fox", "Wolf"]);
        let desired = ["Wolf".to_string(), "Deer".to_string()];
        assert_eq!(
            set_tag_list(&mut xmp, TagType::Species, &desired).unwrap(),
            (true, true)
        );
        assert_eq!(species_of(&xmp), desired);
        // rerun: nothing to do
        assert_eq!(
            set_tag_list(&mut xmp, TagType::Species, &desired).unwrap(),
            (false, false)
        );
    }

    #[test]
    fn tag_updates_are_grouped_per_file() {
        let apply = |species: &[&str], rows: &[(&str, &str)]| {
            let mut xmp = species_xmp(species);
            apply_tag_ops(&mut xmp, TagType::Species, &ops(rows)).map(|changed| (changed, xmp))
        };
        let species = |result: anyhow::Result<(bool, XmpMeta)>| {
            let (changed, xmp) = result.unwrap();
            (changed, species_of(&xmp))
        };

        // duplicate rows (several individuals of one species) apply once
        assert_eq!(
            species(apply(&["Fox"], &[("Fox", "Red fox"), ("Fox", "Red fox")])),
            (true, vec!["Red fox".to_string()])
        );
        // replacements are judged against the original tags
        assert_eq!(
            species(apply(&["A", "B"], &[("A", "B"), ("B", "C")])),
            (true, vec!["B".to_string(), "C".to_string()])
        );
        // rerun after the update was written: nothing to do
        assert_eq!(
            species(apply(&["B", "C"], &[("A", "B"), ("B", "C")])),
            (false, vec!["B".to_string(), "C".to_string()])
        );
        // repeated inserts add one tag, and not again on a rerun
        assert_eq!(
            species(apply(&["Fox"], &[("", "Deer"), ("", "Deer")])),
            (true, vec!["Fox".to_string(), "Deer".to_string()])
        );
        assert!(!apply(&["Fox", "Deer"], &[("", "Deer")]).unwrap().0);
        // conflicting and mismatching rows are errors naming the rows
        let err = apply(&["Fox"], &[("Fox", "A"), ("Fox", "B")])
            .err()
            .unwrap();
        assert!(err.to_string().contains("row 2") && err.to_string().contains("row 3"));
        let err = apply(&["Fox"], &[("Wolf", "Red fox")]).err().unwrap();
        assert!(err.to_string().contains("row 2"));
        // Rating: different targets for one file conflict
        let mut xmp = species_xmp(&[]);
        assert!(apply_rating_ops(&mut xmp, &ops(&[("", "3"), ("", "5")])).is_err());
    }
}
