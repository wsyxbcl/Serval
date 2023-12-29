use std::{path::{Path, PathBuf}, fs, str::FromStr, io};
use xmp_toolkit::{ OpenFileOptions, XmpFile, XmpMeta, XmpValue, xmp_ns};
use indicatif::ProgressBar;
use rayon::prelude::*;
use polars::prelude::*;
use crate::utils::{ResourceType, TagType, path_enumerate, is_temporal_independent, get_path_seperator};

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

fn retrieve_taglist(file_path: &String) -> Result<(Vec<String>, Vec<String>, String), xmp_toolkit::XmpError> {
    // Retrieve digikam taglist and datetime from file
    let mut f = XmpFile::new().unwrap();
    match f.open_file(file_path, OpenFileOptions::default().only_xmp()) {
        Ok(_) => {
            let mut species: Vec<String> = Vec::new();
            let mut individuals: Vec<String> = Vec::new();
            let mut datetime_original = String::new();

            let xmp = match f.xmp() {
                Some(xmp) => xmp,
                None => return Ok((species, individuals, datetime_original)),
            };
            
            if let Some(value) = xmp.property_date(xmp_ns::EXIF, "DateTimeOriginal") {
                datetime_original = value.value.to_string();
            }

            // Register the digikam namespace
            let ns_digikam = "http://www.digikam.org/ns/1.0/";
            XmpMeta::register_namespace(ns_digikam, "digiKam").unwrap();
        
            for property in xmp.property_array(ns_digikam, "TagsList") {
                let tag = property.value;
                if tag.starts_with("Species/") {
                    species.push(tag.strip_prefix("Species/").unwrap().to_string());
                } else if tag.starts_with("Individual/") {
                    individuals.push(tag.strip_prefix("Individual/").unwrap().to_string());
                }
            }
            Ok((species, individuals, datetime_original))
        },
        Err(e) => {
            Err(e)
        }
    }

}

pub fn get_classifications(file_dir: PathBuf, output_dir: PathBuf, parallel: bool, resource_type: ResourceType, independent: bool) {
    // Get tag info from the old digikam workflow in shanshui
    // by enumerating file_dir and read xmp metadata from resources

    let file_paths = path_enumerate(file_dir, resource_type);
    fs::create_dir_all(output_dir.clone()).unwrap();
    let image_paths: Vec<String> = file_paths.clone()
        .into_iter()
        .map(|x| x.to_string_lossy().into_owned())
        .collect();
    let image_names: Vec<String> = file_paths.clone()
        .into_iter()
        .map(|x| x.file_stem().unwrap().to_string_lossy().into_owned())
        .collect();
    let num_images = file_paths.len();
    println!("Total {}: {}.", resource_type, num_images);
    let pb = ProgressBar::new(num_images as u64);

    let mut species_tags: Vec<String> = Vec::new();
    let mut individual_tags: Vec<String> = Vec::new();
    let mut datetime_originals: Vec<String> = Vec::new();

    // try parallel with Rayon here
    if parallel {
        let result: Vec<_> = (0..num_images).into_par_iter().map(|i| {
            match retrieve_taglist(&file_paths[i].to_string_lossy().into_owned()) {
                Ok((species, individuals, datetime_original)) => {
                    pb.inc(1);
                    (species.join(","), individuals.join(","), datetime_original)
                },
                Err(error) => {
                    pb.println(format!("{} in {}", error, file_paths[i].display()));
                    pb.inc(1);
                    ("".to_string(), "".to_string(), "".to_string())
                }
            }
        })
        .collect();
        for tag in result {
            species_tags.push(tag.0);
            individual_tags.push(tag.1);
            datetime_originals.push(tag.2);
        }
    } else {
        for path in file_paths {
            match retrieve_taglist(&path.to_string_lossy().into_owned()) {
                Ok((species, individuals, datetime_original)) => {
                    species_tags.push(species.join(","));
                    individual_tags.push(individuals.join(","));
                    datetime_originals.push(datetime_original);
                },
                Err(error) => {
                    pb.println(format!("{} in {}", error, path.display()));
                    species_tags.push("".to_string());
                    individual_tags.push("".to_string());
                    datetime_originals.push("".to_string());
                }
            }
            pb.inc(1);
        }
    }

    // Analysis
    let s_species = Series::new("species_tags", species_tags);
    let s_individuals = Series::new("individual_tags", individual_tags);
    let s_datetime = Series::new("datetime_original", datetime_originals);

    let df_raw = DataFrame::new(vec![
        Series::new("path", image_paths),
        Series::new("filename",image_names),
        s_species,
        s_individuals,
        s_datetime]).unwrap();

    let df_split = df_raw
        .clone()
        .lazy()
        .select([
            col("path"),
            col("filename"),
            col("datetime_original"),
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

    let tags_csv_path = output_dir.join("tags.csv");
    let mut file = std::fs::File::create(tags_csv_path.clone()).unwrap();
    CsvWriter::new(&mut file).finish(&mut df_flatten).unwrap();
    println!("Saved to {}", output_dir.join("tags.csv").to_string_lossy());

    let mut df_count_species = df_flatten
        .clone()
        .lazy()
        .select([col("species").value_counts(true, true)])
        .unnest(["species"])
        .collect()
        .unwrap();
    println!("{:?}", df_count_species);

    let mut file = std::fs::File::create(output_dir.join("species_stats.csv")).unwrap();
    CsvWriter::new(&mut file).finish(&mut df_count_species).unwrap();
    println!("Saved to {}", output_dir.join("species_stats.csv").to_string_lossy());

    if independent {
        get_temporal_independence(tags_csv_path, output_dir);
    }
}

pub fn get_temporal_independence(csv_path: PathBuf, output_dir: PathBuf) {
    // Remove non-independent records
    // min_delta_time: the minimum time (minutes) difference betwween two independent captures
    // target: "species"
    // exclude: species individuals to be excluded
    // deploy_path_index: index used to determine deployments from paths
    let df = CsvReader::from_path(csv_path)
        .unwrap()
        .infer_schema(None)
        .has_header(true)
        .with_try_parse_dates(true)
        .finish()
        .unwrap();
    
    // Readlines for parameter setup
    // TODO: improve input parser
    let mut input = String::new();
    println!("Input the Minimum Time Difference (when considering records as independent) in minutes (e.g. 30): ");
    io::stdin().read_line(&mut input).unwrap();
    let min_delta_time = input.trim().parse().expect("Not a valid input");
    input.clear();

    println!("The Minimum Time Difference should be compared with?");
    println!("1) Last independent record 2) Last record");
    println!("Enter a selection (e.g. 1): ");
    io::stdin().read_line(&mut input).unwrap();
    let target_input: i32 = input.trim().parse().expect("Not a valid input");
    let delta_time_compared_to = match target_input {
        1 => "LastIndependentRecord",
        2 => "LastRecord",
        _ => "LastRecord",
    };
    input.clear();

    println!("Perform analysis on:");
    println!("1) species 2) individual");
    println!("Enter a selection (default=1): ");
    io::stdin().read_line(&mut input).unwrap();
    let target_input: i32 = input.trim().parse().expect("Not a valid input");
    let target = match target_input {
        1 => TagType::Species,
        2 => TagType::Individual,
        _ => TagType::Species,
    };
    input.clear();

    let path_sample = df.column("path").unwrap().get(0).unwrap().to_string();
    println!("Here is a sample of the directory layout ({}): ", path_sample);
    for (i, entry) in Path::new(&path_sample).iter().enumerate() {
        if entry.to_string_lossy().len() > 0 {
            println!("{}): {}", i, entry.to_string_lossy().replace('"', ""));
        }
    }
    println!("Select the entry corresponding to deployment:");
    io::stdin().read_line(&mut input).unwrap();
    let deploy_path_index:i32 = input.trim().parse().expect("Not a valid input");

    let exclude = ["", "Blank", "Useless data", "Unidentified", "Human"]; // TODO: make it configurable
    let tag_exclude = Series::new("tag_exclude", exclude);

    // Data processing
    let df_cleaned = df
        .clone()
        .lazy()
        .select([
            col("path").str().split(lit(get_path_seperator())).list().get(lit(deploy_path_index)).alias("deployment"),
            col("filename"),
            col("datetime_original").alias("time"),
            col(target.col_name())])
        .drop_nulls(None)
        .filter(col("species").is_in(lit(tag_exclude)).not())
        .unique(
            Some(vec!["deployment".to_string(), "time".to_string(), target.col_name().to_string()]), 
            UniqueKeepStrategy::Any)
        .collect()
        .unwrap();

    let mut df_sorted = df_cleaned
        .lazy()
        .sort("time", Default::default())
        .sort("species", Default::default())
        .sort("deployment", Default::default())
        .collect()
        .unwrap();
    
    let mut df_capture_independent;
    if delta_time_compared_to == "LastRecord" {
        df_capture_independent = df_sorted
            .clone()
            .lazy()
            .group_by_rolling(
                col("time"),
                [col("deployment"), col("species")],
                RollingGroupOptions {
                    period: Duration::parse(format!("{}m", min_delta_time).as_str()),
                    offset: Duration::parse("0"),
                    ..Default::default()
                },
            )
            .agg([col("species").count().alias("count"), col("filename").last()])
            .filter(col("count").eq(lit(1)))
            .select([col("deployment"), col("filename"), col("time"), col("species")])
            .collect()
            .unwrap();
        println!("{}", df_capture_independent);
    } else {
        df_sorted.as_single_chunk_par();
        let mut iters = df_sorted.columns(["time", "species", "deployment"]).unwrap()
            .iter().map(|s| s.iter()).collect::<Vec<_>>();
    
        let mut capture = Vec::new();
        for _row in 0..df_sorted.height() {
            for iter in &mut iters {
                let value = iter.next().expect("should have as many iterations as rows");
                capture.push(value);
            }
        }
        let capture_time: Vec<&AnyValue<'_>> = capture.iter().step_by(3).collect();
        let capture_species: Vec<&AnyValue<'_>> = capture.iter().skip(1).step_by(3).collect();
        let capture_deployment: Vec<&AnyValue<'_>> = capture.iter().skip(2).step_by(3).collect();
    
        // Get temporal independent records
        let mut capture_independent = Vec::new();
        let mut last_indep_time = capture_time[0].to_string();
        let mut last_indep_species = capture_species[0].to_string();
        let mut last_indep_deployment = capture_deployment[0].to_string();
        for i in 0..capture_time.len() {
            let time = capture_time[i].to_string();
            let species = capture_species[i].to_string();
            let deployment = capture_deployment[i].to_string();
    
            if i == 0 || species != last_indep_species || deployment != last_indep_deployment || is_temporal_independent(last_indep_time.clone(), time, min_delta_time){
                capture_independent.push(true);
                last_indep_time = capture_time[i].to_string();
                last_indep_species = capture_species[i].to_string();
                last_indep_deployment = capture_deployment[i].to_string();
            } else {
                capture_independent.push(false);
            }
        }
    
        df_capture_independent = df_sorted
            .lazy()
            .filter(Series::new("independent", capture_independent).lit())
            .collect()
            .unwrap();
        println!("{}", df_capture_independent);
    }

    fs::create_dir_all(output_dir.clone()).unwrap();
    let filename = format!("{}_temporal_independent.csv", target);
    let mut file = std::fs::File::create(output_dir.join(filename.clone())).unwrap();
    CsvWriter::new(&mut file)
        .with_datetime_format(Option::from("%Y-%m-%d %H:%M:%S".to_string()))
        .finish(&mut df_capture_independent)
        .unwrap();
    println!("Saved to {}", output_dir.join(filename).to_string_lossy());

    let mut df_count_independent = df_capture_independent
        .clone()
        .lazy()
        .group_by_stable([col("deployment"), col("species")])
        .agg([
            col("species").count().alias("count"),
        ])
        .collect()
        .unwrap();
    println!("{}", df_count_independent);

    let filename = format!("{}_temporal_independent_count.csv", target);
    let mut file = std::fs::File::create(output_dir.join(filename.clone())).unwrap();
    CsvWriter::new(&mut file)
        .with_datetime_format(Option::from("%Y-%m-%d %H:%M:%S".to_string()))
        .finish(&mut df_count_independent)
        .unwrap();
    println!("Saved to {}", output_dir.join(filename).to_string_lossy());
}