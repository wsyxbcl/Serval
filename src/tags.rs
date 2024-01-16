use crate::utils::{
    absolute_path, get_path_seperator, is_temporal_independent, path_enumerate, ResourceType,
    TagType, ExtractFilterType
};
use indicatif::ProgressBar;
use polars::prelude::*;
use rayon::prelude::*;
use std::{
    fs,
    path::{Path, PathBuf},
    str::FromStr,
};
use xmp_toolkit::{xmp_ns, OpenFileOptions, XmpFile, XmpMeta, XmpValue};
// use rustyline::error::ReadlineError;
use rustyline::validate::{ValidationContext, ValidationResult, Validator};
use rustyline::{
    Cmd, ConditionalEventHandler, Editor, Event, EventContext, EventHandler, KeyCode, KeyEvent,
    Modifiers, RepeatCount, Result,
};

use rustyline::{Completer, Helper, Highlighter, Hinter};

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
}
impl Validator for NumericSelectValidator {
    fn validate(&self, ctx: &mut ValidationContext) -> Result<ValidationResult> {
        use ValidationResult::{Invalid, Valid};
        let input: i32;
        if ctx.input() == "" {
            return Ok(Invalid(Some(" --< Expect numeric input".to_owned())));
        } else {
            input = ctx.input().parse().unwrap();
        }
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

pub fn write_taglist(taglist_path: PathBuf, image_path: PathBuf) -> anyhow::Result<()> {
    // Write taglist to the dummy image metadata (digiKam.TagsList)
    let mut f = XmpFile::new()?;
    let tag_df = CsvReader::from_path(taglist_path)?.finish()?;
    let tags = tag_df.column("species").unwrap();

    let ns_digikam = "http://www.digikam.org/ns/1.0/";
    XmpMeta::register_namespace(ns_digikam, "digiKam")?;
    let dummy_xmp = include_str!("../assets/dummy.xmp");
    let mut meta = XmpMeta::from_str(dummy_xmp).unwrap();
    for tag in tags.utf8()? {
        meta.set_array_item(
            ns_digikam,
            "TagsList",
            xmp_toolkit::ItemPlacement::InsertBeforeIndex(1),
            &XmpValue::new(format!("Species/{}", tag.unwrap())),
        )?;
    }

    f.open_file(image_path, OpenFileOptions::default().for_update())?;
    f.put_xmp(&meta)?;
    f.close();
    Ok(())
}

fn retrieve_taglist(file_path: &String) -> anyhow::Result<(Vec<String>, Vec<String>, String)> {
    // Retrieve digikam taglist and datetime from file
    let mut f = XmpFile::new()?;
    f.open_file(file_path, OpenFileOptions::default().only_xmp())?;

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
    XmpMeta::register_namespace(ns_digikam, "digiKam")?;

    for property in xmp.property_array(ns_digikam, "TagsList") {
        let tag = property.value;
        if tag.starts_with("Species/") {
            species.push(tag.strip_prefix("Species/").unwrap().to_string());
        } else if tag.starts_with("Individual/") {
            individuals.push(tag.strip_prefix("Individual/").unwrap().to_string());
        }
    }
    Ok((species, individuals, datetime_original))
}

pub fn get_classifications(
    file_dir: PathBuf,
    output_dir: PathBuf,
    parallel: bool,
    resource_type: ResourceType,
    independent: bool,
) -> anyhow::Result<()> {
    // Get tag info from the old digikam workflow in shanshui
    // by enumerating file_dir and read xmp metadata from resources

    let file_paths = path_enumerate(file_dir, resource_type);
    fs::create_dir_all(output_dir.clone())?;
    let image_paths: Vec<String> = file_paths
        .clone()
        .into_iter()
        .map(|x| x.to_string_lossy().into_owned())
        .collect();
    let image_names: Vec<String> = file_paths
        .clone()
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
        let result: Vec<_> = (0..num_images)
            .into_par_iter()
            .map(
                |i| match retrieve_taglist(&file_paths[i].to_string_lossy().into_owned()) {
                    Ok((species, individuals, datetime_original)) => {
                        pb.inc(1);
                        (species.join(","), individuals.join(","), datetime_original)
                    }
                    Err(error) => {
                        pb.println(format!("{} in {}", error, file_paths[i].display()));
                        pb.inc(1);
                        ("".to_string(), "".to_string(), "".to_string())
                    }
                },
            )
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
                }
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
        Series::new("filename", image_names),
        s_species,
        s_individuals,
        s_datetime,
    ])?;

    let df_split = df_raw
        .clone()
        .lazy()
        .select([
            col("path"),
            col("filename"),
            col("datetime_original"),
            col("species_tags").str().split(lit(",")).alias("species"),
            col("individual_tags")
                .str()
                .split(lit(","))
                .alias("individuals"),
        ])
        .collect()?;
    println!("{:?}", df_split);

    // Note that there's only individual info for P. uncia
    let mut df_flatten = df_split
        .clone()
        .lazy()
        .select([col("*")])
        .explode(["individuals"])
        .explode(["species"])
        .collect()?;
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
        .collect()?;
    println!("{:?}", df_count_species);

    let mut file = std::fs::File::create(output_dir.join("species_stats.csv"))?;
    CsvWriter::new(&mut file).finish(&mut df_count_species)?;
    println!(
        "Saved to {}",
        output_dir.join("species_stats.csv").to_string_lossy()
    );

    if independent {
        get_temporal_independence(tags_csv_path, output_dir)?;
    }
    Ok(())
}

pub fn extract_resources(
    filter_value: String,
    filter_type: ExtractFilterType,
    csv_path: PathBuf,
    output_dir: PathBuf,
) -> anyhow::Result<()> {
    let df = CsvReader::from_path(csv_path)?
        .has_header(true)
        .with_ignore_errors(true)
        .with_try_parse_dates(true)
        .finish()?;
    let df_filtered: DataFrame;
    match filter_type {
        ExtractFilterType::Species => {
            df_filtered = df
                .clone()
                .lazy()
                .filter(col("species").eq(lit(filter_value)))
                .select([col("path")])
                .collect()?;
        },
        ExtractFilterType::PathRegex => {
            df_filtered = df
                .clone()
                .lazy()
                .filter(col("path").str().contains_literal(lit(filter_value)))
                .collect()?;
        },
        _ => {
            return Ok(());
        }
    }

    // println!("{}", df_filtered);

    // Get the top level directory (to keep)
    let path_sample = df_filtered["path"].get(0)?.to_string().replace('"', ""); // TODO
    println!("Here is a sample of the file path ({}): ", path_sample);
    let mut num_option = 0;
    for (i, entry) in absolute_path(Path::new(&path_sample).to_path_buf())?
        .parent()
        .unwrap()
        .ancestors()
        .enumerate()
    {
        println!("{}): {}", i + 1, entry.to_string_lossy());
        num_option += 1;
    }

    let mut rl = Editor::new()?;
    let h = NumericSelectValidator {
        min: 1,
        max: num_option,
    };
    rl.set_helper(Some(h));
    let readline = rl.readline("Select the top level directory to keep: ");
    let deploy_path_index = readline?.trim().parse::<usize>()?;
    let path_strip = Path::new(&path_sample)
        .ancestors()
        .nth(deploy_path_index + 1)
        .unwrap();
    let pb = ProgressBar::new(df_filtered["path"].len().try_into().unwrap());

    for path in df_filtered["path"].utf8()?.into_iter() {
        let input_path = if path.unwrap().ends_with(".xmp") {
            path.unwrap().strip_suffix(".xmp").unwrap()
        } else {
            path.unwrap()
        };
        let relative_path_output =
            Path::new(input_path).strip_prefix(path_strip.to_string_lossy().replace('"', ""))?; // Where's quote come from
        let output_path = output_dir.join(relative_path_output);
        fs::create_dir_all(output_path.parent().unwrap())?;
        fs::copy(input_path, output_path.clone())?;
        pb.println(format!("Copied to {}", output_path.to_string_lossy()));
        pb.inc(1);
    }
    pb.finish_with_message("done");
    Ok(())
}

pub fn get_temporal_independence(csv_path: PathBuf, output_dir: PathBuf) -> anyhow::Result<()> {
    // Temporal independence analysis

    let df = CsvReader::from_path(csv_path)?
        .has_header(true)
        .with_ignore_errors(true)
        .with_try_parse_dates(true)
        .finish()?;
    // Readlines for parameter setup
    let mut rl = Editor::new()?;
    rl.bind_sequence(
        Event::Any,
        EventHandler::Conditional(Box::new(NumericFilteringHandler)), // Force numerical input
    );
    // Read min_delta_time
    let readline = rl.readline(
        "Input the Minimum Time Difference (when considering records as independent) in minutes (e.g. 30): ");
    let min_delta_time: i32 = readline?.trim().parse()?;
    // Read delta_time_compared_to
    let h = NumericSelectValidator { min: 1, max: 2 };
    rl.set_helper(Some(h));
    let readline = rl.readline(
        "\nThe Minimum Time Difference should be compared with?\n1) Last independent record 2) Last record\nEnter a selection (e.g. 1): ");
    let delta_time_compared_to = match readline?.trim().parse()? {
        1 => "LastIndependentRecord",
        2 => "LastRecord",
        _ => "LastIndependentRecord",
    };
    // Get target (species/individual)
    let h = NumericSelectValidator { min: 1, max: 2 };
    rl.set_helper(Some(h));
    let readline =
        rl.readline("\nPerform analysis on\n1) species 2) individual\nEnter a selection: ");
    let target = match readline?.trim().parse()? {
        1 => TagType::Species,
        2 => TagType::Individual,
        _ => TagType::Species,
    };
    // Find deployment
    let path_sample = df.column("path")?.get(0)?.to_string().replace('"', "");
    println!("\nHere is a sample of the file path ({})", path_sample);
    let mut num_option = 0;
    for (i, entry) in absolute_path(Path::new(&path_sample).to_path_buf())?
        .parent()
        .unwrap()
        .ancestors()
        .enumerate()
    {
        println!("{}): {}", i + 1, entry.to_string_lossy());
        num_option += 1;
    }
    let h = NumericSelectValidator {
        min: 1,
        max: num_option,
    };
    rl.set_helper(Some(h));
    let readline = rl.readline("Select the path corresponding to the deployment: ");
    let deploy_path_index = readline?.trim().parse::<i32>()?;

    let exclude = ["", "Blank", "Useless data", "Unidentified", "Human"]; // TODO: make it configurable
    let tag_exclude = Series::new("tag_exclude", exclude);

    // Data processing
    let df_cleaned = df
        .clone()
        .lazy()
        .select([
            col("path")
                .str()
                .split(lit(get_path_seperator()))
                .list()
                .get(lit(num_option - deploy_path_index))
                .alias("deployment"),
            col("filename"),
            col("datetime_original").alias("time"),
            col(target.col_name()),
        ])
        .drop_nulls(None)
        .filter(col("species").is_in(lit(tag_exclude)).not())
        .unique(
            Some(vec![
                "deployment".to_string(),
                "time".to_string(),
                target.col_name().to_string(),
            ]),
            UniqueKeepStrategy::Any,
        )
        .collect()?;

    let mut df_sorted = df_cleaned
        .lazy()
        .sort("time", Default::default())
        .sort("species", Default::default())
        .sort("deployment", Default::default())
        .collect()?;

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
            .agg([
                col("species").count().alias("count"),
                col("filename").last(),
            ])
            .filter(col("count").eq(lit(1)))
            .select([
                col("deployment"),
                col("filename"),
                col("time"),
                col("species"),
            ])
            .collect()?;
        println!("{}", df_capture_independent);
    } else {
        df_sorted.as_single_chunk_par();
        let mut iters = df_sorted
            .columns(["time", "species", "deployment"])?
            .iter()
            .map(|s| s.iter())
            .collect::<Vec<_>>();

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

            if i == 0
                || species != last_indep_species
                || deployment != last_indep_deployment
                || is_temporal_independent(last_indep_time.clone(), time, min_delta_time)
            {
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
            .collect()?;
        println!("{}", df_capture_independent);
    }

    fs::create_dir_all(output_dir.clone())?;
    let filename = format!("{}_temporal_independent.csv", target);
    let mut file = std::fs::File::create(output_dir.join(filename.clone()))?;
    CsvWriter::new(&mut file)
        .with_datetime_format(Option::from("%Y-%m-%d %H:%M:%S".to_string()))
        .finish(&mut df_capture_independent)?;
    println!("Saved to {}", output_dir.join(filename).to_string_lossy());

    let mut df_count_independent = df_capture_independent
        .clone()
        .lazy()
        .group_by_stable([col("deployment"), col("species")])
        .agg([col("species").count().alias("count")])
        .collect()?;
    println!("{}", df_count_independent);

    let filename = format!("{}_temporal_independent_count.csv", target);
    let mut file = std::fs::File::create(output_dir.join(filename.clone()))?;
    CsvWriter::new(&mut file)
        .with_datetime_format(Option::from("%Y-%m-%d %H:%M:%S".to_string()))
        .finish(&mut df_count_independent)?;
    println!("Saved to {}", output_dir.join(filename).to_string_lossy());
    Ok(())
}
