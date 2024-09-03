use std::{fs, path::PathBuf};

use anyhow::Ok;
use ocrs::{ImageSource, OcrEngine, OcrEngineParams};
use polars::prelude::*;
use rten::Model;

use crate::utils::{crop_image, extract_first_frame, extract_timestamp};
use imageproc::filter::bilateral_filter;

// #[allow(unused)]
// use rten_tensor::prelude::*;

fn file_path(path: &str) -> PathBuf {
    let mut abs_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    abs_path.push(path);
    abs_path
}

pub fn timmstamp_ocr(media_path: PathBuf, debug_mode: bool) -> anyhow::Result<String> {
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

    let img_data = extract_first_frame(media_path.clone())?;
    let img = image::load_from_memory(&img_data)?;

    let cropped_img;
    let sigma_color;
    let sigma_spatial;
    if media_path.to_str().unwrap().contains("Ere") {
        cropped_img = crop_image(&mut img.into_rgb8(), 0.788, 0.93, 0.15, 0.03);
        sigma_color = 10.;
        sigma_spatial = 30.;
    } else {
        cropped_img = crop_image(&mut img.into_rgb8(), 0.4, 0.8, 0.6, 0.2);
        sigma_color = 90.;
        sigma_spatial = 20.;
    }
    // denoise the image
    let cropped_img_gray = imageproc::map::map_colors(&cropped_img, |p| image::Luma([p[0]]));
    // invert
    // image::imageops::invert(&mut cropped_img_gray);

    let denoised_img = bilateral_filter(&cropped_img_gray, 10, sigma_color, sigma_spatial);

    // save the cropped image to disk for debugging
    if debug_mode {
        cropped_img.save("cropped_img_demo.png")?;
        denoised_img.save("denoised_img_demo.png")?;
    }
    // cropped_img.save("cropped_img_demo.jpg")?;

    // let img_data_disk = image::open("cropped_img_demo.jpg")?.to_rgb8();

    // let img_source_disk = ImageSource::from_bytes(img_data_disk.as_raw(), img_data_disk.dimensions())?;
    let img_source_memory =
        ImageSource::from_bytes(denoised_img.as_raw(), denoised_img.dimensions())?;

    // let ocr_input_disk = engine.prepare_input(img_source_disk)?;
    // let ocr_text_disk = engine.get_text(&ocr_input_disk)?;
    // println!("OCR text from disk: {}", ocr_text_disk);

    let ocr_input_memory = engine.prepare_input(img_source_memory)?;
    let ocr_text_memory = engine.get_text(&ocr_input_memory)?;

    // let word_rects = engine.detect_words(&ocr_input_memory)?;
    // let line_rects = engine.find_text_lines(&ocr_input_memory, &word_rects);
    // let ocr_text = engine.recognize_text(&ocr_input_memory, &line_rects)?;
    // let ocr_text_list = ocr_text
    //     .iter()
    //     .flatten()
    //     // Filter likely spurious detections. With future model improvements
    //     // this should become unnecessary.
    //     .filter(|l| l.to_string().len() > 10)
    //     .collect::<Vec<_>>();
    // // find the longest text
    // let ocr_text_memory = ocr_text_list
    //     .iter()
    //     .max_by_key(|l| l.to_string().len())
    //     .unwrap()
    //     .to_string();

    println!("OCR text from memory: {}", ocr_text_memory);

    let ocr_timestamp = extract_timestamp(ocr_text_memory.clone())?;
    println!("Extracted timestamp: {}", ocr_timestamp);

    Ok(ocr_timestamp)
}

// pub fn batch_ocr(media_dir: PathBuf) -> anyhow::Result<()> {
//     let resource_type = ResourceType::Video;
//     let media_files = std::fs::read_dir(media_dir)?
//         .filter_map(|entry| entry.ok())
//         .filter(|entry| entry.path().is_file())
//         // filter ResourceType extension
//         .filter(|entry| {
//             resource_type.is_resource(&entry.path())
//         })
//         .map(|entry| entry.path())
//         .collect::<Vec<_>>();

//     for media_path in media_files {
//         println!("\nProcessing {}", media_path.display());
//         // call timmstamp_ocr for each media file
//         // continue even if there is an error
//         timmstamp_ocr(media_path.clone()).unwrap_or_else(|e| {
//             eprintln!("Error processing {}: {}", media_path.display(), e);
//         });
//     }
//     Ok(())
// }

pub fn batch_ocr_csv(csv_path: PathBuf, output_dir: PathBuf) -> anyhow::Result<()> {
    let mut df = CsvReadOptions::default()
        .with_has_header(true)
        .with_ignore_errors(true)
        .with_parse_options(
            CsvParseOptions::default()
                .with_try_parse_dates(true)
                .with_missing_is_null(true),
        )
        .try_into_reader_with_file_path(Some(csv_path))?
        .finish()?;
    let media_files = df.column("path")?.str()?;
    let mut timestamp_ocr: Vec<String> = Vec::new();
    for path in media_files {
        let media_path = path.unwrap();
        println!("\nProcessing {}", media_path);
        // call timmstamp_ocr for each media file
        // store the extracted timestamp in a polars Series
        // for empty strings, store null
        timestamp_ocr.push(timmstamp_ocr(media_path.into(), false).unwrap_or_else(|e| {
            eprintln!("Error processing {}: {}", media_path, e);
            "".to_string()
        }));
    }
    let s_timestamp_ocr = Series::new("timestamp_ocr", timestamp_ocr);
    df.with_column(s_timestamp_ocr)?;

    fs::create_dir_all(output_dir.clone())?;
    let mut file = std::fs::File::create(output_dir.join("tags_ocr.csv"))?;
    CsvWriter::new(&mut file)
        .include_bom(true)
        .finish(&mut df)?;
    Ok(())
}
