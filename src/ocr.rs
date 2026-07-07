use anyhow::{Context, anyhow};
use chrono::NaiveDateTime;
use image::{DynamicImage, ImageBuffer, Rgb};
use regex::Regex;
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

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

    let mut stdout = child.stdout.take().context("failed to capture ffmpeg stdout")?;
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

#[cfg(test)]
mod tests {
    use super::*;
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
            datetime_format_to_chrono("%Y/%m/%d %H:%M:%S"),
            "%Y/%m/%d %H:%M:%S"
        );
    }

    #[test]
    fn extracts_and_parses_datetime_with_required_format() {
        let parsed =
            extract_datetime("stamp 2026-07-07 13:45:59 end", "YYYY-MM-DD HH:mm:ss").unwrap();
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
}
