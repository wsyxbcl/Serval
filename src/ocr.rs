use crate::schema::{FILENAME_COLUMN, MEDIA_TYPE_COLUMN, PATH_COLUMN, infer_media_type};
use crate::utils::{ResourceType, configure_progress_bar, path_enumerate};
use anyhow::{Context, anyhow};
use chrono::{Datelike, NaiveDateTime};
use image::{DynamicImage, ImageBuffer, Rgb};
use indicatif::ProgressBar;
use ocrs::{ImageSource, OcrEngine, OcrEngineParams};
use polars::prelude::*;
use regex::Regex;
use rten::Model;
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

const DETECTION_MODEL_PATH: &str = "assets/text-detection.rten";
const RECOGNITION_MODEL_PATH: &str = "assets/text-recognition.rten";
pub const DEFAULT_DATETIME_ALLOWED_CHARS: &str = "0123456789-: ";

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CropBox {
    pub x: f32,
    pub y: f32,
    pub width: f32,
    pub height: f32,
}

impl CropBox {
    pub fn parse(value: &str) -> anyhow::Result<Self> {
        let parts = value
            .split(',')
            .map(str::trim)
            .map(str::parse::<f32>)
            .collect::<Result<Vec<_>, _>>()
            .with_context(|| format!("invalid crop box '{value}', expected x,y,w,h"))?;

        if parts.len() != 4 {
            return Err(anyhow!("invalid crop box '{value}', expected x,y,w,h"));
        }

        let crop = Self {
            x: parts[0],
            y: parts[1],
            width: parts[2],
            height: parts[3],
        };

        if crop.x < 0.0 || crop.y < 0.0 || crop.width <= 0.0 || crop.height <= 0.0 {
            return Err(anyhow!(
                "crop box values must be non-negative and width/height must be positive"
            ));
        }
        if crop.x > 1.0 || crop.y > 1.0 || crop.width > 1.0 || crop.height > 1.0 {
            return Err(anyhow!("crop box values must be between 0 and 1"));
        }
        if crop.x + crop.width > 1.0 || crop.y + crop.height > 1.0 {
            return Err(anyhow!("crop box exceeds image bounds"));
        }

        Ok(crop)
    }

    pub fn to_pixel_rect(self, image_width: u32, image_height: u32) -> (u32, u32, u32, u32) {
        let x = (image_width as f32 * self.x).round() as u32;
        let y = (image_height as f32 * self.y).round() as u32;
        let width = (image_width as f32 * self.width).round().max(1.0) as u32;
        let height = (image_height as f32 * self.height).round().max(1.0) as u32;
        (x, y, width, height)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ParsedDatetime {
    pub raw: String,
    pub normalized: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RepairedDatetime {
    pub raw: String,
    pub normalized: String,
    pub method: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct YearRange {
    start: i32,
    end: i32,
}

impl YearRange {
    pub fn parse(value: &str) -> anyhow::Result<Self> {
        let (start, end) = value
            .split_once("..")
            .ok_or_else(|| anyhow!("invalid year range '{value}', expected START..END"))?;
        let start = start
            .parse::<i32>()
            .with_context(|| format!("invalid year range start '{start}'"))?;
        let end = end
            .parse::<i32>()
            .with_context(|| format!("invalid year range end '{end}'"))?;
        if start > end {
            return Err(anyhow!(
                "invalid year range '{value}', start must be <= end"
            ));
        }
        Ok(Self { start, end })
    }

    pub fn contains(self, year: i32) -> bool {
        self.start <= year && year <= self.end
    }

    fn unique_single_digit_correction(self, ocr_year: &str) -> Option<i32> {
        if ocr_year.len() != 4 || !ocr_year.chars().all(|ch| ch.is_ascii_digit()) {
            return None;
        }

        let mut matches = (self.start..=self.end).filter(|year| {
            let year = year.to_string();
            year.len() == 4
                && year
                    .chars()
                    .zip(ocr_year.chars())
                    .filter(|(expected, actual)| expected != actual)
                    .count()
                    == 1
        });
        let first = matches.next()?;
        if matches.next().is_some() {
            None
        } else {
            Some(first)
        }
    }
}

pub fn datetime_format_to_chrono(format: &str) -> String {
    if format.contains('%') {
        return format.to_string();
    }

    let mut converted = String::new();
    let mut chars = format.chars().peekable();
    let mut in_time = false;

    while let Some(ch) = chars.next() {
        if ch.is_ascii_alphabetic() {
            let mut token = String::from(ch);
            while chars.peek().is_some_and(|next| *next == ch) {
                token.push(chars.next().unwrap());
            }

            let replacement = match token.as_str() {
                "YYYY" | "yyyy" => "%Y",
                "YY" | "yy" => "%y",
                "DD" | "dd" => "%d",
                "HH" | "hh" => {
                    in_time = true;
                    "%H"
                }
                "SS" | "ss" => {
                    in_time = true;
                    "%S"
                }
                "MM" => "%m",
                "mm" if in_time => "%M",
                "mm" => "%m",
                _ => token.as_str(),
            };
            converted.push_str(replacement);
        } else {
            if ch.is_whitespace() {
                in_time = true;
            }
            converted.push(ch);
        }
    }

    converted
}

fn datetime_candidate_regex(chrono_format: &str) -> anyhow::Result<Regex> {
    let mut pattern = regex::escape(chrono_format);
    for (token, replacement) in [
        ("%Y", r"\d{4}"),
        ("%y", r"\d{2}"),
        ("%m", r"\d{2}"),
        ("%d", r"\d{2}"),
        ("%H", r"\d{2}"),
        ("%M", r"\d{2}"),
        ("%S", r"\d{2}"),
    ] {
        pattern = pattern.replace(&regex::escape(token), replacement);
    }
    Regex::new(&pattern).context("failed to build datetime candidate regex")
}

pub fn extract_datetime(
    text: &str,
    user_format: &str,
    year_range: Option<YearRange>,
) -> anyhow::Result<ParsedDatetime> {
    let chrono_format = datetime_format_to_chrono(user_format);
    let re = datetime_candidate_regex(&chrono_format)?;

    for mat in re.find_iter(text) {
        let raw = mat.as_str().to_string();
        if let Ok(dt) = NaiveDateTime::parse_from_str(&raw, &chrono_format) {
            if year_range.is_some_and(|range| !range.contains(dt.year())) {
                continue;
            }
            return Ok(ParsedDatetime {
                raw,
                normalized: dt.format("%Y-%m-%d %H:%M:%S").to_string(),
            });
        }
    }

    Err(anyhow!("no datetime matched the supplied format"))
}

fn year_candidate_starts(text: &str, year_range: Option<YearRange>) -> anyhow::Result<Vec<usize>> {
    if year_range.is_none() {
        let year_re = Regex::new(r"20\d{2}")?;
        return Ok(year_re.find_iter(text).map(|mat| mat.start()).collect());
    }

    let chars = text.char_indices().collect::<Vec<_>>();
    let mut starts = Vec::new();
    for window in chars.windows(4) {
        if window.iter().all(|(_, ch)| ch.is_ascii_digit()) {
            starts.push(window[0].0);
        }
    }
    Ok(starts)
}

pub fn repair_datetime(
    text: &str,
    user_format: &str,
    year_range: Option<YearRange>,
) -> anyhow::Result<RepairedDatetime> {
    let chrono_format = datetime_format_to_chrono(user_format);
    for token in ["%Y", "%m", "%d", "%H", "%M", "%S"] {
        if !chrono_format.contains(token) {
            return Err(anyhow!(
                "datetime repair requires year, month, day, hour, minute, and second"
            ));
        }
    }

    for year_start in year_candidate_starts(text, year_range)? {
        let mut digits = String::new();
        let mut raw_end = year_start;

        for (offset, ch) in text[year_start..].char_indices() {
            if ch.is_ascii_digit() {
                digits.push(ch);
                raw_end = year_start + offset + ch.len_utf8();
                if digits.len() == 14 {
                    break;
                }
            }
        }

        if digits.len() != 14 {
            continue;
        }

        let mut method = "first_14_digits_after_year";
        if let Some(range) = year_range {
            let raw_year = &digits[0..4];
            let parsed_year = raw_year.parse::<i32>().ok();
            if parsed_year.is_some_and(|year| range.contains(year)) {
                method = "first_14_digits_after_year";
            } else if let Some(corrected_year) = range.unique_single_digit_correction(raw_year) {
                digits.replace_range(0..4, &corrected_year.to_string());
                method = "unique_year_range_correction";
            } else {
                continue;
            }
        }

        let candidate = format!(
            "{}-{}-{} {}:{}:{}",
            &digits[0..4],
            &digits[4..6],
            &digits[6..8],
            &digits[8..10],
            &digits[10..12],
            &digits[12..14]
        );
        if let Ok(dt) = NaiveDateTime::parse_from_str(&candidate, "%Y-%m-%d %H:%M:%S") {
            return Ok(RepairedDatetime {
                raw: text[year_start..raw_end].to_string(),
                normalized: dt.format("%Y-%m-%d %H:%M:%S").to_string(),
                method: method.to_string(),
            });
        }
    }

    Err(anyhow!("no repairable datetime found"))
}

pub fn effective_allowed_chars(
    allowed_chars: Option<String>,
    no_allowed_chars: bool,
) -> Option<String> {
    if no_allowed_chars {
        None
    } else {
        Some(allowed_chars.unwrap_or_else(|| DEFAULT_DATETIME_ALLOWED_CHARS.to_string()))
    }
}

pub fn apply_sample_limit(mut paths: Vec<PathBuf>, sample: Option<usize>) -> Vec<PathBuf> {
    if let Some(limit) = sample {
        paths.truncate(limit);
    }
    paths
}

fn crop_image(image: DynamicImage, crop: Option<CropBox>) -> ImageBuffer<Rgb<u8>, Vec<u8>> {
    let mut rgb = image.into_rgb8();
    if let Some(crop) = crop {
        let (x, y, width, height) = crop.to_pixel_rect(rgb.width(), rgb.height());
        image::imageops::crop(&mut rgb, x, y, width, height).to_image()
    } else {
        rgb
    }
}

fn load_image_input(
    path: &Path,
    crop: Option<CropBox>,
) -> anyhow::Result<ImageBuffer<Rgb<u8>, Vec<u8>>> {
    let image =
        image::open(path).with_context(|| format!("failed to read image {}", path.display()))?;
    Ok(crop_image(image, crop))
}

fn extract_first_video_frame(path: &Path) -> anyhow::Result<Vec<u8>> {
    let mut child = Command::new("ffmpeg")
        .args([
            "-hide_banner",
            "-loglevel",
            "error",
            "-i",
            path.to_str()
                .ok_or_else(|| anyhow!("non-UTF-8 video path: {}", path.display()))?,
            "-frames:v",
            "1",
            "-f",
            "image2pipe",
            "-vcodec",
            "png",
            "-",
        ])
        .stdout(Stdio::piped())
        .spawn()
        .context("failed to start ffmpeg")?;

    let mut stdout = child
        .stdout
        .take()
        .context("failed to capture ffmpeg stdout")?;
    let mut buffer = Vec::new();
    stdout.read_to_end(&mut buffer)?;
    let status = child.wait()?;
    if !status.success() {
        return Err(anyhow!(
            "ffmpeg failed while extracting first frame from {}",
            path.display()
        ));
    }
    Ok(buffer)
}

fn load_video_input(
    path: &Path,
    crop: Option<CropBox>,
) -> anyhow::Result<ImageBuffer<Rgb<u8>, Vec<u8>>> {
    let frame = extract_first_video_frame(path)?;
    let image = image::load_from_memory(&frame)
        .with_context(|| format!("failed to decode first frame for {}", path.display()))?;
    Ok(crop_image(image, crop))
}

fn save_debug_crop(
    output_dir: &Path,
    media_path: &Path,
    image: &ImageBuffer<Rgb<u8>, Vec<u8>>,
) -> anyhow::Result<()> {
    let debug_dir = output_dir.join("debug_crops");
    fs::create_dir_all(&debug_dir)?;
    let filename = media_path
        .file_name()
        .ok_or_else(|| anyhow!("media path has no filename: {}", media_path.display()))?;
    let mut debug_path = debug_dir.join(filename);
    debug_path.set_extension("png");
    image.save(&debug_path)?;
    Ok(())
}

struct ServalOcrEngine {
    engine: OcrEngine,
}

impl ServalOcrEngine {
    fn load(allowed_chars: Option<String>) -> anyhow::Result<Self> {
        let detection_model = Model::load_file(DETECTION_MODEL_PATH).with_context(|| {
            format!("failed to load OCR detection model from {DETECTION_MODEL_PATH}")
        })?;
        let recognition_model = Model::load_file(RECOGNITION_MODEL_PATH).with_context(|| {
            format!("failed to load OCR recognition model from {RECOGNITION_MODEL_PATH}")
        })?;

        let engine = OcrEngine::new(OcrEngineParams {
            detection_model: Some(detection_model),
            recognition_model: Some(recognition_model),
            allowed_chars,
            ..Default::default()
        })?;

        Ok(Self { engine })
    }

    fn get_text(&self, image: &ImageBuffer<Rgb<u8>, Vec<u8>>) -> anyhow::Result<String> {
        let source = ImageSource::from_bytes(image.as_raw(), image.dimensions())?;
        let input = self.engine.prepare_input(source)?;
        Ok(self.engine.get_text(&input)?)
    }
}

#[derive(Debug, Clone)]
struct OcrRow {
    path: String,
    filename: String,
    media_type: String,
    datetime_ocr: String,
    datetime_raw: String,
    ocr_text: String,
    datetime_format: String,
    datetime_repair: String,
    confidence: String,
    confidence_reason: String,
    status: String,
    error: String,
}

impl OcrRow {
    fn failed(path: &Path, datetime_format: &str, status: &str, error: anyhow::Error) -> Self {
        Self {
            path: path.to_string_lossy().into_owned(),
            filename: path
                .file_name()
                .map(|name| name.to_string_lossy().into_owned())
                .unwrap_or_default(),
            media_type: infer_media_type(path).unwrap_or("").to_string(),
            datetime_ocr: String::new(),
            datetime_raw: String::new(),
            ocr_text: String::new(),
            datetime_format: datetime_format.to_string(),
            datetime_repair: String::new(),
            confidence: "needs_llm".to_string(),
            confidence_reason: status.to_string(),
            status: status.to_string(),
            error: error.to_string(),
        }
    }
}

fn write_ocr_csv(output_dir: &Path, rows: &[OcrRow]) -> anyhow::Result<()> {
    fs::create_dir_all(output_dir)?;
    let mut df = DataFrame::new(
        rows.len(),
        vec![
            Column::new(
                PATH_COLUMN.into(),
                rows.iter().map(|row| row.path.as_str()).collect::<Vec<_>>(),
            ),
            Column::new(
                FILENAME_COLUMN.into(),
                rows.iter()
                    .map(|row| row.filename.as_str())
                    .collect::<Vec<_>>(),
            ),
            Column::new(
                MEDIA_TYPE_COLUMN.into(),
                rows.iter()
                    .map(|row| row.media_type.as_str())
                    .collect::<Vec<_>>(),
            ),
            Column::new(
                "datetime_ocr".into(),
                rows.iter()
                    .map(|row| row.datetime_ocr.as_str())
                    .collect::<Vec<_>>(),
            ),
            Column::new(
                "datetime_raw".into(),
                rows.iter()
                    .map(|row| row.datetime_raw.as_str())
                    .collect::<Vec<_>>(),
            ),
            Column::new(
                "ocr_text".into(),
                rows.iter()
                    .map(|row| row.ocr_text.as_str())
                    .collect::<Vec<_>>(),
            ),
            Column::new(
                "datetime_format".into(),
                rows.iter()
                    .map(|row| row.datetime_format.as_str())
                    .collect::<Vec<_>>(),
            ),
            Column::new(
                "datetime_repair".into(),
                rows.iter()
                    .map(|row| row.datetime_repair.as_str())
                    .collect::<Vec<_>>(),
            ),
            Column::new(
                "confidence".into(),
                rows.iter()
                    .map(|row| row.confidence.as_str())
                    .collect::<Vec<_>>(),
            ),
            Column::new(
                "confidence_reason".into(),
                rows.iter()
                    .map(|row| row.confidence_reason.as_str())
                    .collect::<Vec<_>>(),
            ),
            Column::new(
                "status".into(),
                rows.iter()
                    .map(|row| row.status.as_str())
                    .collect::<Vec<_>>(),
            ),
            Column::new(
                "error".into(),
                rows.iter()
                    .map(|row| row.error.as_str())
                    .collect::<Vec<_>>(),
            ),
        ],
    )?;

    let mut file = fs::File::create(output_dir.join("ocr.csv"))?;
    CsvWriter::new(&mut file)
        .include_bom(true)
        .finish(&mut df)?;
    Ok(())
}

fn collect_media_paths(input_path: PathBuf, sample: Option<usize>) -> anyhow::Result<Vec<PathBuf>> {
    let paths = if input_path.is_file() {
        if ResourceType::Media.is_resource(&input_path) {
            vec![input_path]
        } else {
            return Err(anyhow!("unsupported media path: {}", input_path.display()));
        }
    } else if input_path.is_dir() {
        path_enumerate(input_path, ResourceType::Media)
    } else {
        return Err(anyhow!(
            "input path does not exist: {}",
            input_path.display()
        ));
    };

    Ok(apply_sample_limit(paths, sample))
}

fn process_media(
    engine: &ServalOcrEngine,
    path: &Path,
    output_dir: &Path,
    crop: Option<CropBox>,
    debug_crops: bool,
    datetime_format: &str,
    repair_datetime_enabled: bool,
    year_range: Option<YearRange>,
) -> OcrRow {
    match process_media_inner(
        engine,
        path,
        output_dir,
        crop,
        debug_crops,
        datetime_format,
        repair_datetime_enabled,
        year_range,
    ) {
        Ok(row) => row,
        Err(err) => OcrRow::failed(path, datetime_format, "ocr_failed", err),
    }
}

fn process_media_inner(
    engine: &ServalOcrEngine,
    path: &Path,
    output_dir: &Path,
    crop: Option<CropBox>,
    debug_crops: bool,
    datetime_format: &str,
    repair_datetime_enabled: bool,
    year_range: Option<YearRange>,
) -> anyhow::Result<OcrRow> {
    let media_type = infer_media_type(path)?.to_string();
    let image = if media_type.starts_with("image/") {
        load_image_input(path, crop)?
    } else if media_type.starts_with("video/") {
        load_video_input(path, crop)?
    } else {
        return Err(anyhow!("unsupported media type {media_type}"));
    };

    if debug_crops {
        save_debug_crop(output_dir, path, &image)?;
    }

    let ocr_text = engine.get_text(&image)?;
    let parsed = extract_datetime(&ocr_text, datetime_format, year_range);
    let (datetime_ocr, datetime_raw, datetime_repair, status, error) = match parsed {
        Ok(parsed) => (
            parsed.normalized,
            parsed.raw,
            String::new(),
            "ok".to_string(),
            String::new(),
        ),
        Err(err) => {
            if repair_datetime_enabled
                && let Ok(repaired) = repair_datetime(&ocr_text, datetime_format, year_range)
            {
                (
                    repaired.normalized,
                    repaired.raw,
                    repaired.method,
                    "ok_repaired".to_string(),
                    String::new(),
                )
            } else {
                (
                    String::new(),
                    String::new(),
                    String::new(),
                    "parse_failed".to_string(),
                    err.to_string(),
                )
            }
        }
    };
    let (confidence, confidence_reason) = if status == "ok" {
        ("confident".to_string(), "strict_datetime".to_string())
    } else {
        ("needs_llm".to_string(), status.clone())
    };

    Ok(OcrRow {
        path: path.to_string_lossy().into_owned(),
        filename: path
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_default(),
        media_type,
        datetime_ocr,
        datetime_raw,
        ocr_text,
        datetime_format: datetime_format.to_string(),
        datetime_repair,
        confidence,
        confidence_reason,
        status,
        error,
    })
}

fn parse_normalized_datetime(value: &str) -> Option<NaiveDateTime> {
    NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S").ok()
}

fn sequence_key(row: &OcrRow) -> &str {
    row.filename.as_str()
}

fn is_sequence_outlier(rows: &[OcrRow], index: usize) -> bool {
    let Some(current) = parse_normalized_datetime(&rows[index].datetime_ocr) else {
        return false;
    };

    let prev = rows[..index]
        .iter()
        .rev()
        .take(4)
        .find_map(|row| parse_normalized_datetime(&row.datetime_ocr));
    let next = rows[index + 1..]
        .iter()
        .take(4)
        .find_map(|row| parse_normalized_datetime(&row.datetime_ocr));
    let (Some(prev), Some(next)) = (prev, next) else {
        return false;
    };

    let neighbor_gap = (next - prev).num_seconds().abs();
    let prev_gap = (current - prev).num_seconds().abs();
    let next_gap = (current - next).num_seconds().abs();
    neighbor_gap <= 15 * 60 && prev_gap > 30 * 60 && next_gap > 30 * 60
}

fn apply_sequence_outlier_check(rows: &mut [OcrRow]) {
    rows.sort_by(|left, right| {
        let left_parent = Path::new(&left.path)
            .parent()
            .map(|path| path.to_string_lossy().into_owned())
            .unwrap_or_default();
        let right_parent = Path::new(&right.path)
            .parent()
            .map(|path| path.to_string_lossy().into_owned())
            .unwrap_or_default();
        left_parent
            .cmp(&right_parent)
            .then_with(|| sequence_key(left).cmp(sequence_key(right)))
    });

    let mut group_start = 0;
    while group_start < rows.len() {
        let parent = Path::new(&rows[group_start].path)
            .parent()
            .map(|path| path.to_string_lossy().into_owned())
            .unwrap_or_default();
        let mut group_end = group_start + 1;
        while group_end < rows.len() {
            let row_parent = Path::new(&rows[group_end].path)
                .parent()
                .map(|path| path.to_string_lossy().into_owned())
                .unwrap_or_default();
            if row_parent != parent {
                break;
            }
            group_end += 1;
        }

        let outliers = (0..group_end - group_start)
            .filter(|index| {
                rows[group_start + *index].confidence == "confident"
                    && is_sequence_outlier(&rows[group_start..group_end], *index)
            })
            .collect::<Vec<_>>();
        for index in outliers {
            let row = &mut rows[group_start + index];
            row.confidence = "needs_llm".to_string();
            row.confidence_reason = "sequence_outlier".to_string();
        }

        group_start = group_end;
    }
}

#[derive(Debug, Clone)]
pub struct OcrOptions {
    pub input_path: PathBuf,
    pub output_dir: PathBuf,
    pub crop_box: Option<String>,
    pub debug_crops: bool,
    pub sample: Option<usize>,
    pub datetime_format: String,
    pub allowed_chars: Option<String>,
    pub no_allowed_chars: bool,
    pub repair_datetime: bool,
    pub year_range: Option<String>,
    pub sequence_outlier_check: bool,
}

pub fn run_ocr(options: OcrOptions) -> anyhow::Result<()> {
    let crop = options
        .crop_box
        .as_deref()
        .map(CropBox::parse)
        .transpose()?;
    let media_paths = collect_media_paths(options.input_path, options.sample)?;
    fs::create_dir_all(&options.output_dir)?;
    let year_range = options
        .year_range
        .as_deref()
        .map(YearRange::parse)
        .transpose()?;

    let allowed_chars = effective_allowed_chars(options.allowed_chars, options.no_allowed_chars);
    let engine = ServalOcrEngine::load(allowed_chars)?;
    let pb = ProgressBar::new(media_paths.len() as u64);
    configure_progress_bar(&pb);

    let mut rows = Vec::with_capacity(media_paths.len());
    for path in media_paths {
        let row = process_media(
            &engine,
            &path,
            &options.output_dir,
            crop,
            options.debug_crops,
            &options.datetime_format,
            options.repair_datetime,
            year_range,
        );
        rows.push(row);
        pb.inc(1);
    }
    pb.finish();

    if options.sequence_outlier_check {
        apply_sequence_outlier_check(&mut rows);
    }

    write_ocr_csv(&options.output_dir, &rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;

    #[test]
    fn parses_valid_relative_crop_box() {
        let crop = CropBox::parse("0.70,0.86,0.28,0.10").unwrap();
        assert_eq!(
            crop,
            CropBox {
                x: 0.70,
                y: 0.86,
                width: 0.28,
                height: 0.10
            }
        );
    }

    #[test]
    fn rejects_out_of_range_crop_box() {
        let err = CropBox::parse("0.70,0.86,0.40,0.20")
            .unwrap_err()
            .to_string();
        assert!(err.contains("crop box exceeds image bounds"));
    }

    #[test]
    fn converts_user_datetime_format_to_chrono() {
        assert_eq!(
            datetime_format_to_chrono("YYYY-MM-DD HH:mm:ss"),
            "%Y-%m-%d %H:%M:%S"
        );
        assert_eq!(
            datetime_format_to_chrono("yyyy-mm-dd hh:mm:ss"),
            "%Y-%m-%d %H:%M:%S"
        );
        assert_eq!(
            datetime_format_to_chrono("%Y/%m/%d %H:%M:%S"),
            "%Y/%m/%d %H:%M:%S"
        );
    }

    #[test]
    fn extracts_and_parses_datetime_with_required_format() {
        let parsed =
            extract_datetime("stamp 2026-07-07 13:45:59 end", "YYYY-MM-DD HH:mm:ss", None).unwrap();
        assert_eq!(parsed.raw, "2026-07-07 13:45:59");
        assert_eq!(parsed.normalized, "2026-07-07 13:45:59");
    }

    #[test]
    fn strict_datetime_requires_two_digit_fields() {
        let err = extract_datetime("stamp 2026-07-07 13:45:9 end", "YYYY-MM-DD HH:mm:ss", None)
            .unwrap_err()
            .to_string();
        assert!(err.contains("no datetime matched"));
    }

    #[test]
    fn repairs_datetime_from_separator_variants() {
        for (text, expected) in [
            ("2025 12 10 14:25:15", "2025-12-10 14:25:15"),
            ("2026-01-0106:31:27", "2026-01-01 06:31:27"),
            ("2025-1129 13:39:26", "2025-11-29 13:39:26"),
            ("202512-18 12:25:08", "2025-12-18 12:25:08"),
            ("22025-12-20 13:0103", "2025-12-20 13:01:03"),
            ("2025-12-04 1130:35", "2025-12-04 11:30:35"),
        ] {
            let repaired = repair_datetime(text, "yyyy-mm-dd hh:mm:ss", None).unwrap();
            assert_eq!(repaired.normalized, expected);
            assert_eq!(repaired.method, "first_14_digits_after_year");
        }
    }

    #[test]
    fn parses_year_range() {
        let range = YearRange::parse("2025..2026").unwrap();
        assert!(range.contains(2025));
        assert!(range.contains(2026));
        assert!(!range.contains(2027));
    }

    #[test]
    fn repairs_unique_year_ocr_error_with_year_range() {
        let range = YearRange::parse("2025..2026").unwrap();
        let repaired =
            repair_datetime("2125-11-22 11:34:29", "yyyy-mm-dd hh:mm:ss", Some(range)).unwrap();
        assert_eq!(repaired.normalized, "2025-11-22 11:34:29");
        assert_eq!(repaired.method, "unique_year_range_correction");
    }

    #[test]
    fn repair_datetime_with_year_range_considers_overlapping_year_windows() {
        let range = YearRange::parse("2025..2026").unwrap();
        let repaired =
            repair_datetime("22025 12-10 13:39:45", "yyyy-mm-dd hh:mm:ss", Some(range)).unwrap();
        assert_eq!(repaired.normalized, "2025-12-10 13:39:45");
    }

    #[test]
    fn repair_datetime_rejects_ambiguous_year_range_correction() {
        let range = YearRange::parse("2025..2026").unwrap();
        let err = repair_datetime("2027-11-22 11:34:29", "yyyy-mm-dd hh:mm:ss", Some(range))
            .unwrap_err()
            .to_string();
        assert!(err.contains("no repairable datetime"));
    }

    #[test]
    fn repair_datetime_rejects_missing_year_prefix() {
        let err = repair_datetime("025-10-21 16:37:04", "yyyy-mm-dd hh:mm:ss", None)
            .unwrap_err()
            .to_string();
        assert!(err.contains("no repairable datetime"));
    }

    #[test]
    fn uses_datetime_allowed_chars_by_default() {
        assert_eq!(
            effective_allowed_chars(None, false),
            Some(DEFAULT_DATETIME_ALLOWED_CHARS.to_string())
        );
        assert_eq!(
            effective_allowed_chars(Some("0123".to_string()), false),
            Some("0123".to_string())
        );
        assert_eq!(
            effective_allowed_chars(Some("0123".to_string()), true),
            None
        );
    }

    #[test]
    fn rejects_impossible_datetime() {
        let err = extract_datetime("stamp 2026-99-07 13:45:59", "YYYY-MM-DD HH:mm:ss", None)
            .unwrap_err()
            .to_string();
        assert!(err.contains("no datetime matched"));
    }

    #[test]
    fn crop_box_converts_to_pixel_rect() {
        let crop = CropBox {
            x: 0.25,
            y: 0.50,
            width: 0.50,
            height: 0.25,
        };
        assert_eq!(crop.to_pixel_rect(400, 200), (100, 100, 200, 50));
    }

    #[test]
    fn sample_limit_keeps_first_n_paths() {
        let paths = vec![
            PathBuf::from("a.jpg"),
            PathBuf::from("b.jpg"),
            PathBuf::from("c.jpg"),
        ];
        assert_eq!(
            apply_sample_limit(paths, Some(2)),
            vec![PathBuf::from("a.jpg"), PathBuf::from("b.jpg")]
        );
    }

    fn test_row(filename: &str, datetime_ocr: &str) -> OcrRow {
        OcrRow {
            path: format!("/media/cam/{filename}"),
            filename: filename.to_string(),
            media_type: "image/jpeg".to_string(),
            datetime_ocr: datetime_ocr.to_string(),
            datetime_raw: datetime_ocr.to_string(),
            ocr_text: datetime_ocr.to_string(),
            datetime_format: "YYYY-MM-DD HH:mm:ss".to_string(),
            datetime_repair: String::new(),
            confidence: "confident".to_string(),
            confidence_reason: "strict_datetime".to_string(),
            status: "ok".to_string(),
            error: String::new(),
        }
    }

    #[test]
    fn sequence_outlier_check_downgrades_local_time_outlier() {
        let mut rows = vec![
            test_row("IMG_0796.jpg", "2026-01-03 16:23:16"),
            test_row("IMG_0797.jpg", "2026-01-13 16:23:16"),
            test_row("IMG_0798.jpg", "2026-01-03 16:27:06"),
        ];

        apply_sequence_outlier_check(&mut rows);

        let outlier = rows
            .iter()
            .find(|row| row.filename == "IMG_0797.jpg")
            .unwrap();
        assert_eq!(outlier.datetime_ocr, "2026-01-13 16:23:16");
        assert_eq!(outlier.confidence, "needs_llm");
        assert_eq!(outlier.confidence_reason, "sequence_outlier");
    }

    #[test]
    fn sequence_outlier_check_keeps_regular_time_jump_confident() {
        let mut rows = vec![
            test_row("IMG_0001.jpg", "2025-12-19 18:15:17"),
            test_row("IMG_0002.jpg", "2025-12-20 08:25:34"),
            test_row("IMG_0003.jpg", "2025-12-20 08:25:33"),
        ];

        apply_sequence_outlier_check(&mut rows);

        assert!(rows.iter().all(|row| row.confidence == "confident"));
    }

    #[test]
    fn writes_ocr_csv_with_expected_columns() {
        let temp_dir = std::env::temp_dir().join(format!("serval-ocr-test-{}", std::process::id()));
        let _ = fs::remove_dir_all(&temp_dir);
        fs::create_dir_all(&temp_dir).unwrap();

        let rows = vec![OcrRow {
            path: "/tmp/a.jpg".to_string(),
            filename: "a.jpg".to_string(),
            media_type: "image/jpeg".to_string(),
            datetime_ocr: "2026-07-07 13:45:59".to_string(),
            datetime_raw: "2026-07-07 13:45:59".to_string(),
            ocr_text: "2026-07-07 13:45:59".to_string(),
            datetime_format: "YYYY-MM-DD HH:mm:ss".to_string(),
            datetime_repair: String::new(),
            confidence: "confident".to_string(),
            confidence_reason: "strict_datetime".to_string(),
            status: "ok".to_string(),
            error: String::new(),
        }];

        write_ocr_csv(&temp_dir, &rows).unwrap();
        let csv = fs::read_to_string(temp_dir.join("ocr.csv")).unwrap();
        assert!(csv.contains(
            "path,filename,media_type,datetime_ocr,datetime_raw,ocr_text,datetime_format,datetime_repair,confidence,confidence_reason,status,error"
        ));
        assert!(csv.contains("2026-07-07 13:45:59"));

        fs::remove_dir_all(&temp_dir).unwrap();
    }
}
