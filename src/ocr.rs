use std::path::PathBuf;

use ocrs::{ImageSource, OcrEngine, OcrEngineParams};
use rten::Model;

use crate::utils::{extract_first_frame, crop_image, extract_timestamp};

// #[allow(unused)]
// use rten_tensor::prelude::*;

fn file_path(path: &str) -> PathBuf {
    let mut abs_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    abs_path.push(path);
    abs_path
}

pub fn timmstamp_ocr(media_path: PathBuf) -> anyhow::Result<()> {
    // Use the `download-models.sh` script to download the models.
    let detection_model_path = file_path("assets/text-detection.rten");
    let rec_model_path = file_path("assets/text-recognition.rten");

    let detection_model = Model::load_file(detection_model_path)?;
    let recognition_model = Model::load_file(rec_model_path)?;


    let engine = OcrEngine::new(OcrEngineParams {
        detection_model: Some(detection_model),
        recognition_model: Some(recognition_model),
        ..Default::default()
    })?;

    let img_data = extract_first_frame(media_path)?;
    let mut img = image::load_from_memory(&img_data).map(|image| image.into_rgb8())?;
    let cropped_img = crop_image(&mut img, 0.4, 0.85, 0.6, 0.15);
    let img_source = ImageSource::from_bytes(cropped_img.as_raw(), cropped_img.dimensions())?;
    let ocr_input = engine.prepare_input(img_source)?;

    let ocr_text = engine.get_text(&ocr_input)?;
    // println!("OCR text: {}", ocr_text);
    println!("Extracted timestamp: {}", extract_timestamp(ocr_text)?);

    Ok(())
}
