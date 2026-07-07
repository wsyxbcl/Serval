use anyhow::{Context, anyhow};
use chrono::NaiveDateTime;
use regex::Regex;
use std::path::PathBuf;

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
}
