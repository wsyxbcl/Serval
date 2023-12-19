use std::{path::PathBuf, fs, str::FromStr};
use xmp_toolkit::{ OpenFileOptions, XmpFile, XmpMeta, XmpValue};
use indicatif::ProgressBar;
use rayon::prelude::*;
use polars::prelude::*;
use crate::utils::image_path_enumerate;

pub fn write_taglist(taglist_path: PathBuf, image_path: PathBuf) -> Result<(), xmp_toolkit::XmpError> {
    // Write taglist to the dummy image metadata (digiKam.TagsList)
    let mut f = XmpFile::new().unwrap();
    let tag_df = CsvReader::from_path(taglist_path).unwrap().finish().unwrap();
    let tags = tag_df.column("species").unwrap();
    match f.open_file(image_path, OpenFileOptions::default().for_update()) {
        Ok(_) => {
            let ns_digikam = "http://www.digikam.org/ns/1.0/";
            XmpMeta::register_namespace(ns_digikam, "digiKam").unwrap();
            let dummy_xmp = include_str!("../assets/dummy.xmp");
            let mut meta = XmpMeta::from_str(dummy_xmp).unwrap();
            for tag in tags.utf8().unwrap() {
                meta.set_array_item(ns_digikam, "TagsList", 
                    xmp_toolkit::ItemPlacement::InsertBeforeIndex(1), 
                    &XmpValue::new(format!("Species/{}",tag.unwrap()))).unwrap();   
            }
            f.put_xmp(&meta).unwrap();
            f.close();
            Ok(())
        },
        Err(e) => {
            Err(e)
        }
    }
}

fn retrieve_taglist(image_path: &String) -> Result<(Vec<String>, Vec<String>), xmp_toolkit::XmpError> {
    // Retrieve digikam taglist from image xmp metadata
    let mut f = XmpFile::new().unwrap();
    match f.open_file(image_path, OpenFileOptions::default().only_xmp().use_smart_handler()) {
        Ok(_) => {
            let mut species: Vec<String> = Vec::new();
            let mut individuals: Vec<String> = Vec::new();

            let xmp = f.xmp();
            if xmp.is_none() {
                return Ok((species, individuals));
            }
            // Register the digikam namespace
            let ns_digikam = "http://www.digikam.org/ns/1.0/";
            XmpMeta::register_namespace(ns_digikam, "digiKam").unwrap();
        
            for property in xmp.unwrap().property_array(ns_digikam, "TagsList") {
                let tag = property.value;
                if tag.starts_with("Species/") {
                    species.push(tag.strip_prefix("Species/").unwrap().to_string());
                } else if tag.starts_with("Individual/") {
                    individuals.push(tag.strip_prefix("Individual/").unwrap().to_string());
                }
            }
            Ok((species, individuals))
        },
        Err(e) => {
            Err(e)
        }
    }

}

pub fn get_classifications(media_dir: PathBuf, output_dir: PathBuf, parallel: bool) {
    let image_paths = image_path_enumerate(media_dir);

    fs::create_dir_all(output_dir.clone()).unwrap();

    // Get tag info from the old digikam workflow in shanshui
    let image_path_strings: Vec<String> = image_paths.clone()
        .into_iter()
        .map(|x| x.to_string_lossy().into_owned())
        .collect();
    let image_names: Vec<String> = image_paths.clone()
        .into_iter()
        .map(|x| x.file_stem().unwrap().to_string_lossy().into_owned())
        .collect();
    let num_images = image_paths.len();
    println!("{} images in total.", num_images);

    let mut species_tags: Vec<String> = Vec::new();
    let mut individual_tags: Vec<String> = Vec::new();
    let pb = ProgressBar::new(num_images as u64);

    // try parallel with Rayon here
    if parallel {
        let result: Vec<_> = (0..num_images).into_par_iter().map(|i| {
            match retrieve_taglist(&image_paths[i].to_string_lossy().into_owned()) {
                Ok((species, individuals)) => {
                    // println!("{:?} {:?}", species, individuals);
                    pb.inc(1);
                    (species.join(","), individuals.join(","))
                },
                Err(error) => {
                    pb.println(format!("{} in {}", error, image_paths[i].display()));
                    pb.inc(1);
                    ("".to_string(), "".to_string())
                }
            }
        })
        .collect();
        for tag in result {
            species_tags.push(tag.0);
            individual_tags.push(tag.1);
        }

    } else {
        for path in image_paths {
            match retrieve_taglist(&path.to_string_lossy().into_owned()) {
                Ok((species, individuals)) => {
                    // println!("{:?} {:?}", species, individuals);
                    species_tags.push(species.join(","));
                    individual_tags.push(individuals.join(","));
                },
                Err(error) => {
                    pb.println(format!("{} in {}", error, path.display()));
                    species_tags.push("".to_string());
                    individual_tags.push("".to_string());
                }
            }
            pb.inc(1);
        }
    }

    let s_species = Series::new("species_tags", species_tags);
    let s_individuals = Series::new("individual_tags", individual_tags);

    let df_raw = DataFrame::new(vec![
        Series::new("path", image_path_strings),
        Series::new("filename",image_names),
        s_species,
        s_individuals]).unwrap();
    println!("{:?}", df_raw);

    let df_split = df_raw
        .clone()
        .lazy()
        .select([
            col("path"),
            col("filename"),
            col("species_tags").str().split(lit(",")).alias("species"),
            col("individual_tags").str().split(lit(",")).alias("individuals")
        ])
        .collect()
        .unwrap();
    println!("{:?}", df_split);

    // Note that there's only individual info for P. uncia
    let mut df_flatten = df_split
        .clone()
        .lazy()
        .select([col("*")])
        .explode(["individuals"])
        .explode(["species"])
        .collect()
        .unwrap();
    println!("{}", df_flatten);

    let mut file = std::fs::File::create(output_dir.join("tags.csv")).unwrap();
    CsvWriter::new(&mut file).finish(&mut df_flatten).unwrap();
    println!("Saved to {}", output_dir.join("tags.csv").to_string_lossy());

    let mut df_count_species = df_flatten
        .lazy()
        .select([col("species").value_counts(true, true)])
        .unnest(["species"])
        .collect()
        .unwrap();
    println!("{:?}", df_count_species);

    let mut file = std::fs::File::create(output_dir.join("species_stats.csv")).unwrap();
    CsvWriter::new(&mut file).finish(&mut df_count_species).unwrap();
    println!("Saved to {}", output_dir.join("species_stats.csv").to_string_lossy());
}
