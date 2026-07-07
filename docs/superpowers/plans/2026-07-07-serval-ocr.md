# Serval OCR Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `serval ocr <path>` to OCR media watermarks, strictly parse user-specified datetimes, and write an independent `ocr.csv`.

**Architecture:** Add a focused `src/ocr.rs` module that owns OCR config, crop parsing, media frame/image preparation, strict datetime parsing, row construction, and CSV writing. `src/main.rs` only exposes the CLI and calls `ocr::run_ocr`; existing schema and utility helpers provide media type inference, recursive traversal, progress bars, and absolute path handling.

**Tech Stack:** Rust 2024, clap, chrono, regex, polars CSV writer, image, ocrs 0.12.x, rten 0.24.x, FFmpeg for video first-frame extraction.

---

## File Structure

- Create `src/ocr.rs`: OCR command implementation, crop box parser, datetime format conversion, datetime candidate extraction, image/video input preparation, `ocrs` engine wrapper, row output.
- Modify `src/main.rs`: add `mod ocr`, import `run_ocr` and `OcrOptions`, add `Commands::Ocr`, route CLI args to `run_ocr`.
- Modify `src/lib.rs`: export `ocr` for unit tests and library consumers.
- Modify `Cargo.toml`: add `image`, `ocrs`, and `rten` dependencies.
- Use existing `src/schema.rs`: `infer_media_type`, `PATH_COLUMN`, `FILENAME_COLUMN`, `MEDIA_TYPE_COLUMN`.
- Use existing `src/utils.rs`: `ResourceType::Media`, `path_enumerate`, `configure_progress_bar`.

Model files are expected at `assets/text-detection.rten` and `assets/text-recognition.rten` in the first version. Missing model files are command-level errors. Do not implement auto-download.

---

### Task 1: Add Dependencies And Module Skeleton

**Files:**
- Modify: `Cargo.toml`
- Modify: `src/lib.rs`
- Modify: `src/main.rs`
- Create: `src/ocr.rs`

- [ ] **Step 1: Add dependencies**

Edit `Cargo.toml` and add these dependencies under `[dependencies]`:

```toml
image = "0.25.9"
ocrs = "0.12.2"
rten = "0.24.0"
```

- [ ] **Step 2: Create the OCR module skeleton**

Create `src/ocr.rs`:

```rust
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct OcrOptions {
    pub input_path: PathBuf,
    pub output_dir: PathBuf,
    pub crop_box: Option<String>,
    pub debug_crops: bool,
    pub sample: Option<usize>,
    pub datetime_format: String,
    pub allowed_chars: Option<String>,
}

pub fn run_ocr(_options: OcrOptions) -> anyhow::Result<()> {
    Err(anyhow::anyhow!("serval ocr is not implemented yet"))
}
```

- [ ] **Step 3: Export the OCR module**

Edit `src/lib.rs`:

```rust
pub mod ocr;
pub mod schema;
pub mod tags;
pub mod utils;
```

- [ ] **Step 4: Wire the CLI command to the temporary `run_ocr` error**

Edit `src/main.rs`.

Add the module:

```rust
mod ocr;
mod schema;
mod tags;
mod utils;
```

Add imports:

```rust
use ocr::{OcrOptions, run_ocr};
```

Add this match arm before `Commands::Translate`:

```rust
        Commands::Ocr {
            path,
            output,
            box_,
            debug_crops,
            sample,
            datetime_format,
            allowed_chars,
        } => {
            run_ocr(OcrOptions {
                input_path: absolute_path(path)?,
                output_dir: output,
                crop_box: box_,
                debug_crops,
                sample,
                datetime_format,
                allowed_chars,
            })?;
        }
```

Add this enum variant before `Translate`:

```rust
    /// OCR media watermarks and extract visible datetimes
    #[command(arg_required_else_help = true)]
    Ocr {
        /// Media file or directory to scan recursively
        path: PathBuf,
        /// Output directory
        #[arg(
            short,
            long,
            value_name = "OUTPUT_DIR",
            default_value = "./serval_output/serval_ocr"
        )]
        output: PathBuf,
        /// Relative crop box as x,y,w,h, each value in 0..=1
        #[arg(long = "box", value_name = "X,Y,W,H")]
        box_: Option<String>,
        /// Save cropped OCR inputs for inspection
        #[arg(long)]
        debug_crops: bool,
        /// Process only the first N media files
        #[arg(long, value_name = "N")]
        sample: Option<usize>,
        /// Required datetime format, e.g. YYYY-MM-DD HH:mm:ss or %Y-%m-%d %H:%M:%S
        #[arg(long, value_name = "FORMAT", required = true)]
        datetime_format: String,
        /// Optional OCR recognition character whitelist
        #[arg(long, value_name = "CHARS")]
        allowed_chars: Option<String>,
    },
```

- [ ] **Step 5: Verify the skeleton builds**

Run:

```bash
cargo check
```

Expected: build succeeds. If dependency download is blocked by sandbox/network, rerun with approved network access.

- [ ] **Step 6: Commit**

```bash
git add Cargo.toml Cargo.lock src/lib.rs src/main.rs src/ocr.rs
git commit -m "Add OCR command skeleton"
```

---

### Task 2: Implement Crop Box And Datetime Format Parsing

**Files:**
- Modify: `src/ocr.rs`
- Modify: `src/utils.rs`

- [ ] **Step 1: Write failing parser tests**

Append tests to `src/ocr.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_valid_relative_crop_box() {
        let crop = CropBox::parse("0.70,0.86,0.28,0.10").unwrap();
        assert_eq!(crop, CropBox { x: 0.70, y: 0.86, width: 0.28, height: 0.10 });
    }

    #[test]
    fn rejects_out_of_range_crop_box() {
        let err = CropBox::parse("0.70,0.86,0.40,0.20").unwrap_err().to_string();
        assert!(err.contains("crop box exceeds image bounds"));
    }

    #[test]
    fn converts_user_datetime_format_to_chrono() {
        assert_eq!(
            datetime_format_to_chrono("YYYY-MM-DD HH:mm:ss"),
            "%Y-%m-%d %H:%M:%S"
        );
        assert_eq!(
            datetime_format_to_chrono("%Y/%m/%d %H:%M:%S"),
            "%Y/%m/%d %H:%M:%S"
        );
    }

    #[test]
    fn extracts_and_parses_datetime_with_required_format() {
        let parsed = extract_datetime("stamp 2026-07-07 13:45:59 end", "YYYY-MM-DD HH:mm:ss").unwrap();
        assert_eq!(parsed.raw, "2026-07-07 13:45:59");
        assert_eq!(parsed.normalized, "2026-07-07 13:45:59");
    }

    #[test]
    fn rejects_impossible_datetime() {
        let err = extract_datetime("stamp 2026-99-07 13:45:59", "YYYY-MM-DD HH:mm:ss")
            .unwrap_err()
            .to_string();
        assert!(err.contains("no datetime matched"));
    }
}
```

- [ ] **Step 2: Run tests and verify they fail**

Run:

```bash
cargo test ocr::tests --lib
```

Expected: fail because `CropBox`, `datetime_format_to_chrono`, and `extract_datetime` are undefined.

- [ ] **Step 3: Implement parsers**

Add this code above `OcrOptions` in `src/ocr.rs`:

```rust
use anyhow::{Context, anyhow};
use chrono::NaiveDateTime;
use regex::Regex;

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
            return Err(anyhow!("crop box values must be non-negative and width/height must be positive"));
        }
        if crop.x > 1.0 || crop.y > 1.0 || crop.width > 1.0 || crop.height > 1.0 {
            return Err(anyhow!("crop box values must be between 0 and 1"));
        }
        if crop.x + crop.width > 1.0 || crop.y + crop.height > 1.0 {
            return Err(anyhow!("crop box exceeds image bounds"));
        }

        Ok(crop)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ParsedDatetime {
    pub raw: String,
    pub normalized: String,
}

pub fn datetime_format_to_chrono(format: &str) -> String {
    if format.contains('%') {
        return format.to_string();
    }

    format
        .replace("YYYY", "%Y")
        .replace("yyyy", "%Y")
        .replace("MM", "%m")
        .replace("DD", "%d")
        .replace("dd", "%d")
        .replace("HH", "%H")
        .replace("mm", "%M")
        .replace("ss", "%S")
}

fn datetime_candidate_regex(chrono_format: &str) -> anyhow::Result<Regex> {
    let mut pattern = regex::escape(chrono_format);
    for (token, replacement) in [
        ("%Y", r"\d{4}"),
        ("%y", r"\d{2}"),
        ("%m", r"\d{1,2}"),
        ("%d", r"\d{1,2}"),
        ("%H", r"\d{1,2}"),
        ("%M", r"\d{1,2}"),
        ("%S", r"\d{1,2}"),
    ] {
        pattern = pattern.replace(&regex::escape(token), replacement);
    }
    Regex::new(&pattern).context("failed to build datetime candidate regex")
}

fn clean_datetime_candidate(candidate: &str) -> String {
    candidate
        .chars()
        .map(|ch| match ch {
            'O' | 'o' => '0',
            'I' | 'l' => '1',
            _ => ch,
        })
        .collect()
}

pub fn extract_datetime(text: &str, user_format: &str) -> anyhow::Result<ParsedDatetime> {
    let chrono_format = datetime_format_to_chrono(user_format);
    let re = datetime_candidate_regex(&chrono_format)?;

    for mat in re.find_iter(text) {
        let raw = mat.as_str().to_string();
        let cleaned = clean_datetime_candidate(&raw);
        if let Ok(dt) = NaiveDateTime::parse_from_str(&cleaned, &chrono_format) {
            return Ok(ParsedDatetime {
                raw,
                normalized: dt.format("%Y-%m-%d %H:%M:%S").to_string(),
            });
        }
    }

    Err(anyhow!("no datetime matched the supplied format"))
}
```

- [ ] **Step 4: Run parser tests**

Run:

```bash
cargo test ocr::tests --lib
```

Expected: all `ocr::tests` pass.

- [ ] **Step 5: Commit**

```bash
git add src/ocr.rs
git commit -m "Add OCR crop and datetime parsers"
```

---

### Task 3: Implement Media Enumeration, Image Loading, Cropping, And Video Frame Extraction

**Files:**
- Modify: `src/ocr.rs`

- [ ] **Step 1: Write failing media helper tests**

Add these tests inside `mod tests` in `src/ocr.rs`:

```rust
    #[test]
    fn crop_box_converts_to_pixel_rect() {
        let crop = CropBox { x: 0.25, y: 0.50, width: 0.50, height: 0.25 };
        assert_eq!(crop.to_pixel_rect(400, 200), (100, 100, 200, 50));
    }

    #[test]
    fn sample_limit_keeps_first_n_paths() {
        let paths = vec![PathBuf::from("a.jpg"), PathBuf::from("b.jpg"), PathBuf::from("c.jpg")];
        assert_eq!(
            apply_sample_limit(paths, Some(2)),
            vec![PathBuf::from("a.jpg"), PathBuf::from("b.jpg")]
        );
    }
```

- [ ] **Step 2: Run tests and verify they fail**

Run:

```bash
cargo test ocr::tests --lib
```

Expected: fail because `to_pixel_rect` and `apply_sample_limit` are missing.

- [ ] **Step 3: Implement media helper functions**

Add imports at the top of `src/ocr.rs`:

```rust
use image::{DynamicImage, ImageBuffer, Rgb};
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
```

Replace the earlier `use std::path::PathBuf;` with the combined path import above.

Add methods and helpers:

```rust
impl CropBox {
    pub fn to_pixel_rect(self, image_width: u32, image_height: u32) -> (u32, u32, u32, u32) {
        let x = (image_width as f32 * self.x).round() as u32;
        let y = (image_height as f32 * self.y).round() as u32;
        let width = (image_width as f32 * self.width).round().max(1.0) as u32;
        let height = (image_height as f32 * self.height).round().max(1.0) as u32;
        (x, y, width, height)
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

fn load_image_input(path: &Path, crop: Option<CropBox>) -> anyhow::Result<ImageBuffer<Rgb<u8>, Vec<u8>>> {
    let image = image::open(path).with_context(|| format!("failed to read image {}", path.display()))?;
    Ok(crop_image(image, crop))
}

fn extract_first_video_frame(path: &Path) -> anyhow::Result<Vec<u8>> {
    let mut child = Command::new("ffmpeg")
        .args([
            "-hide_banner",
            "-loglevel",
            "error",
            "-i",
            path.to_str().ok_or_else(|| anyhow!("non-UTF-8 video path: {}", path.display()))?,
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

    let mut stdout = child.stdout.take().context("failed to capture ffmpeg stdout")?;
    let mut buffer = Vec::new();
    stdout.read_to_end(&mut buffer)?;
    let status = child.wait()?;
    if !status.success() {
        return Err(anyhow!("ffmpeg failed while extracting first frame from {}", path.display()));
    }
    Ok(buffer)
}

fn load_video_input(path: &Path, crop: Option<CropBox>) -> anyhow::Result<ImageBuffer<Rgb<u8>, Vec<u8>>> {
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
```

- [ ] **Step 4: Run tests**

Run:

```bash
cargo test ocr::tests --lib
```

Expected: all `ocr::tests` pass.

- [ ] **Step 5: Commit**

```bash
git add src/ocr.rs
git commit -m "Add OCR media input helpers"
```

---

### Task 4: Implement OCR Engine Wrapper

**Files:**
- Modify: `src/ocr.rs`

- [ ] **Step 1: Add engine wrapper code**

Add imports:

```rust
use ocrs::{ImageSource, OcrEngine, OcrEngineParams};
use rten::Model;
```

Add constants and wrapper:

```rust
const DETECTION_MODEL_PATH: &str = "assets/text-detection.rten";
const RECOGNITION_MODEL_PATH: &str = "assets/text-recognition.rten";

struct ServalOcrEngine {
    engine: OcrEngine,
}

impl ServalOcrEngine {
    fn load(allowed_chars: Option<String>) -> anyhow::Result<Self> {
        let detection_model = Model::load_file(DETECTION_MODEL_PATH)
            .with_context(|| format!("failed to load OCR detection model from {DETECTION_MODEL_PATH}"))?;
        let recognition_model = Model::load_file(RECOGNITION_MODEL_PATH)
            .with_context(|| format!("failed to load OCR recognition model from {RECOGNITION_MODEL_PATH}"))?;

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
```

- [ ] **Step 2: Verify build**

Run:

```bash
cargo check
```

Expected: build succeeds. This step does not load model files at build time.

- [ ] **Step 3: Commit**

```bash
git add src/ocr.rs
git commit -m "Add OCR engine wrapper"
```

---

### Task 5: Build OCR Rows And CSV Output

**Files:**
- Modify: `src/ocr.rs`

- [ ] **Step 1: Add row model and CSV writer**

Add imports:

```rust
use crate::schema::{FILENAME_COLUMN, MEDIA_TYPE_COLUMN, PATH_COLUMN, infer_media_type};
use crate::utils::{ResourceType, configure_progress_bar, path_enumerate};
use indicatif::ProgressBar;
use polars::prelude::*;
```

Add row types:

```rust
#[derive(Debug, Clone)]
struct OcrRow {
    path: String,
    filename: String,
    media_type: String,
    datetime_ocr: String,
    datetime_raw: String,
    ocr_text: String,
    datetime_format: String,
    status: String,
    error: String,
}

impl OcrRow {
    fn failed(path: &Path, datetime_format: &str, status: &str, error: anyhow::Error) -> Self {
        Self {
            path: path.to_string_lossy().into_owned(),
            filename: path.file_name().map(|name| name.to_string_lossy().into_owned()).unwrap_or_default(),
            media_type: infer_media_type(path).unwrap_or("").to_string(),
            datetime_ocr: String::new(),
            datetime_raw: String::new(),
            ocr_text: String::new(),
            datetime_format: datetime_format.to_string(),
            status: status.to_string(),
            error: error.to_string(),
        }
    }
}

fn write_ocr_csv(output_dir: &Path, rows: &[OcrRow]) -> anyhow::Result<()> {
    fs::create_dir_all(output_dir)?;
    let mut df = DataFrame::new(vec![
        Column::new(PATH_COLUMN.into(), rows.iter().map(|row| row.path.as_str()).collect::<Vec<_>>()),
        Column::new(FILENAME_COLUMN.into(), rows.iter().map(|row| row.filename.as_str()).collect::<Vec<_>>()),
        Column::new(MEDIA_TYPE_COLUMN.into(), rows.iter().map(|row| row.media_type.as_str()).collect::<Vec<_>>()),
        Column::new("datetime_ocr".into(), rows.iter().map(|row| row.datetime_ocr.as_str()).collect::<Vec<_>>()),
        Column::new("datetime_raw".into(), rows.iter().map(|row| row.datetime_raw.as_str()).collect::<Vec<_>>()),
        Column::new("ocr_text".into(), rows.iter().map(|row| row.ocr_text.as_str()).collect::<Vec<_>>()),
        Column::new("datetime_format".into(), rows.iter().map(|row| row.datetime_format.as_str()).collect::<Vec<_>>()),
        Column::new("status".into(), rows.iter().map(|row| row.status.as_str()).collect::<Vec<_>>()),
        Column::new("error".into(), rows.iter().map(|row| row.error.as_str()).collect::<Vec<_>>()),
    ])?;

    let mut file = fs::File::create(output_dir.join("ocr.csv"))?;
    CsvWriter::new(&mut file)
        .include_bom(true)
        .finish(&mut df)?;
    Ok(())
}
```

- [ ] **Step 2: Make media-type resource checking callable from OCR**

Edit `src/utils.rs` and change `ResourceType::is_resource` from private to public:

```rust
    pub fn is_resource(self, path: &Path) -> bool {
        resource_extension(path).is_some_and(|ext| self.extension().contains(&ext.as_str()))
    }
```

- [ ] **Step 3: Implement path collection**

Add:

```rust
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
        return Err(anyhow!("input path does not exist: {}", input_path.display()));
    };

    Ok(apply_sample_limit(paths, sample))
}
```

- [ ] **Step 4: Implement per-file processing**

Add:

```rust
fn process_media(
    engine: &ServalOcrEngine,
    path: &Path,
    output_dir: &Path,
    crop: Option<CropBox>,
    debug_crops: bool,
    datetime_format: &str,
) -> OcrRow {
    match process_media_inner(engine, path, output_dir, crop, debug_crops, datetime_format) {
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
    let parsed = extract_datetime(&ocr_text, datetime_format);
    let (datetime_ocr, datetime_raw, status, error) = match parsed {
        Ok(parsed) => (parsed.normalized, parsed.raw, "ok".to_string(), String::new()),
        Err(err) => (String::new(), String::new(), "parse_failed".to_string(), err.to_string()),
    };

    Ok(OcrRow {
        path: path.to_string_lossy().into_owned(),
        filename: path.file_name().map(|name| name.to_string_lossy().into_owned()).unwrap_or_default(),
        media_type,
        datetime_ocr,
        datetime_raw,
        ocr_text,
        datetime_format: datetime_format.to_string(),
        status,
        error,
    })
}
```

- [ ] **Step 5: Replace temporary `run_ocr` with the batch implementation**

Replace the temporary implementation:

```rust
pub fn run_ocr(options: OcrOptions) -> anyhow::Result<()> {
    let crop = options
        .crop_box
        .as_deref()
        .map(CropBox::parse)
        .transpose()?;
    let media_paths = collect_media_paths(options.input_path, options.sample)?;
    fs::create_dir_all(&options.output_dir)?;

    let engine = ServalOcrEngine::load(options.allowed_chars)?;
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
        );
        rows.push(row);
        pb.inc(1);
    }
    pb.finish();

    write_ocr_csv(&options.output_dir, &rows)
}
```

- [ ] **Step 6: Verify build**

Run:

```bash
cargo check
```

Expected: build succeeds.

- [ ] **Step 7: Commit**

```bash
git add src/ocr.rs src/utils.rs
git commit -m "Write OCR results to CSV"
```

---

### Task 6: Add Integration Coverage For CSV And Failure Behavior

**Files:**
- Modify: `src/ocr.rs`

- [ ] **Step 1: Add a focused CSV unit test that does not require OCR models**

Add this test inside `mod tests`:

```rust
    #[test]
    fn writes_ocr_csv_with_expected_columns() {
        let temp_dir = std::env::temp_dir().join(format!(
            "serval-ocr-test-{}",
            std::process::id()
        ));
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
            status: "ok".to_string(),
            error: String::new(),
        }];

        write_ocr_csv(&temp_dir, &rows).unwrap();
        let csv = fs::read_to_string(temp_dir.join("ocr.csv")).unwrap();
        assert!(csv.contains("path,filename,media_type,datetime_ocr,datetime_raw,ocr_text,datetime_format,status,error"));
        assert!(csv.contains("2026-07-07 13:45:59"));

        fs::remove_dir_all(&temp_dir).unwrap();
    }
```

- [ ] **Step 2: Run tests**

Run:

```bash
cargo test ocr::tests --lib
```

Expected: all OCR unit tests pass without model files.

- [ ] **Step 3: Commit**

```bash
git add src/ocr.rs
git commit -m "Test OCR CSV output"
```

---

### Task 7: Final Verification And Manual Smoke Test

**Files:**
- No planned source edits unless verification finds a defect.

- [ ] **Step 1: Run formatting**

```bash
cargo fmt
```

Expected: completes with no output.

- [ ] **Step 2: Run tests**

```bash
cargo test
```

Expected: all tests pass. If dependency downloads are blocked, rerun with approved network access.

- [ ] **Step 3: Run static build check**

```bash
cargo check
```

Expected: build succeeds.

- [ ] **Step 4: Run CLI help check**

```bash
cargo run -- ocr --help
```

Expected: help text includes `--box`, `--debug-crops`, `--sample`, `--datetime-format`, and `--allowed-chars`.

- [ ] **Step 5: Optional real-media smoke test**

Only run if `assets/text-detection.rten`, `assets/text-recognition.rten`, FFmpeg, and sample media are available:

```bash
cargo run --release -- ocr /path/to/sample-media \
  --sample 5 \
  --debug-crops \
  --box 0.70,0.86,0.28,0.10 \
  --datetime-format "YYYY-MM-DD HH:mm:ss" \
  --allowed-chars "0123456789-/:. "
```

Expected: `serval_output/serval_ocr/ocr.csv` exists and `debug_crops/` contains up to 5 PNG files.

- [ ] **Step 6: Commit final cleanup if needed**

If verification required source changes:

```bash
git add Cargo.toml Cargo.lock src/lib.rs src/main.rs src/ocr.rs src/utils.rs
git commit -m "Finalize OCR command"
```

If no source changes were needed, do not create an empty commit.

---

## Self-Review

- Spec coverage: The plan implements `serval ocr <path>`, recursive media traversal, relative crop box, debug crops, sample mode, required strict datetime format, optional allowed chars, image and video first-frame handling, independent `ocr.csv`, per-file failure rows, and command-level failures for bad arguments or missing models.
- No fallback guessing: The plan uses only user-supplied datetime formats and explicitly keeps fallback guessing out of scope.
- Testability: Parser, crop, sample, and CSV behavior are unit-tested without requiring OCR models. Real OCR is covered by build checks and optional smoke testing because model files and FFmpeg are external runtime requirements.
- Type consistency: `OcrOptions`, `CropBox`, `ParsedDatetime`, `ServalOcrEngine`, `OcrRow`, and function names are introduced before use and reused consistently.
