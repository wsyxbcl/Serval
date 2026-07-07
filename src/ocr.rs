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
