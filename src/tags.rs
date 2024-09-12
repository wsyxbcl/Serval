use crate::utils::{
    absolute_path, append_ext, get_path_seperator, ignore_timezone, is_temporal_independent,
    path_enumerate, sync_modified_time, ExtractFilterType, ResourceType, TagType,
};
use chrono::{DateTime, Local};
use indicatif::ProgressBar;
use itertools::izip;
use polars::{lazy::dsl::StrptimeOptions, prelude::*};
use rayon::prelude::*;
use rustyline::{
    validate::{ValidationContext, ValidationResult, Validator},
    Cmd, Completer, ConditionalEventHandler, Editor, Event, EventContext, EventHandler, Helper,
    Highlighter, Hinter, KeyCode, KeyEvent, Modifiers, RepeatCount, Result,
};
use std::{
    fs,
    path::{Path, PathBuf},
    str::FromStr,
};
use xmp_toolkit::{xmp_ns, OpenFileOptions, ToStringOptions, XmpFile, XmpMeta, XmpValue};

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
        let input: i32 = if ctx.input() == "" {
            return Ok(Invalid(Some(" --< Expect numeric input".to_owned())));
        } else {
            ctx.input().parse().unwrap()
        };
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

pub fn write_taglist(
    taglist_path: PathBuf,
    image_path: PathBuf,
    tag_type: TagType,
) -> anyhow::Result<()> {
    // Write taglist to the dummy image metadata (digiKam.TagsList)
    let mut f = XmpFile::new()?;
    let tag_df = CsvReadOptions::default()
        .with_infer_schema_length(Some(0))
        .try_into_reader_with_file_path(Some(taglist_path))?
        .finish()?;
    let tags = tag_df.column(tag_type.col_name())?.unique()?;
    let ns_digikam = "http://www.digikam.org/ns/1.0/";
    XmpMeta::register_namespace(ns_digikam, "digiKam")?;
    let dummy_xmp = include_str!("../assets/dummy.xmp");
    let mut meta = XmpMeta::from_str(dummy_xmp)?;
    for tag in tags.str()? {
        meta.set_array_item(
            ns_digikam,
            "TagsList",
            xmp_toolkit::ItemPlacement::InsertBeforeIndex(1),
            &XmpValue::new(format!("{}{}", tag_type.digikam_tag_prefix(), tag.unwrap())),
        )?;
    }

    f.open_file(image_path, OpenFileOptions::default().for_update())?;
    f.put_xmp(&meta)?;
    f.close();
    Ok(())
}

pub fn init_xmp(working_dir: PathBuf) -> anyhow::Result<()> {
    let media_paths = path_enumerate(working_dir.clone(), ResourceType::Media);
    let media_count = media_paths.len();
    let pb = ProgressBar::new(media_count.try_into()?);
    for media in media_paths {
        let mut media_xmp = XmpFile::new()?;
        if media_xmp
            .open_file(
                media.clone(),
                OpenFileOptions::default().only_xmp().repair_file(),
            )
            .is_ok()
        {
            if let Some(xmp) = media_xmp.xmp() {
                let xmp_path = working_dir.join(append_ext("xmp", media)?);
                // Check existence of xmp file
                if xmp_path.exists() {
                    pb.inc(1);
                    pb.println(format!("XMP file already exists: {}", xmp_path.display()));
                    continue;
                }
                fs::File::create(xmp_path.clone())?;
                let xmp_string = xmp.to_string_with_options(
                    ToStringOptions::default().set_newline("\n".to_string()),
                )?;
                fs::write(xmp_path, xmp_string)?;
                pb.inc(1);
            }
        } else {
            pb.println(format!("Failed to open file: {}", media.display()));
            pb.inc(1);
        }
    }
    Ok(())
}

type Metadata = (
    Vec<String>, // species
    Vec<String>, // individuals
    Vec<String>, // subjects
    String,      // datetime_original
    String,      // datetime_digitized
    String,      // time_modified
    String,      // rating
);

fn retrieve_metadata(
    file_path: &String,
    include_subject: bool,
    include_time_modified: bool,
) -> anyhow::Result<Metadata> {
    // Retrieve metadata from given file
    // digikam taglist (species and individual), subject, datetime_original, datetime_digitized, rating and file modified time

    let mut f = XmpFile::new()?;
    f.open_file(file_path, OpenFileOptions::default().only_xmp())?;

    let mut species: Vec<String> = Vec::new();
    let mut individuals: Vec<String> = Vec::new();
    let mut subjects: Vec<String> = Vec::new(); // for old digikam vesrion?
    let mut datetime_original = String::new();
    let mut datetime_digitized = String::new();
    let mut time_modified = String::new();
    let mut rating = String::new();

    if include_time_modified {
        let file_metadata = fs::metadata(file_path)?;
        let file_modified_time: DateTime<Local> = file_metadata.modified()?.into();
        time_modified = file_modified_time.format("%Y-%m-%dT%H:%M:%S").to_string();
    }
    // Retrieve digikam taglist and datetime from file
    let mut f = XmpFile::new()?;
    if f.open_file(file_path, OpenFileOptions::default().only_xmp())
        .is_ok()
    {
        if let Some(xmp) = f.xmp() {
            if let Some(value) = xmp.property_date(xmp_ns::EXIF, "DateTimeOriginal") {
                datetime_original = ignore_timezone(value.value.to_string())?;
            }
            if let Some(value) = xmp.property_date(xmp_ns::EXIF, "DateTimeDigitized") {
                datetime_digitized = ignore_timezone(value.value.to_string())?;
            }
            if let Some(value) = xmp.property(xmp_ns::XMP, "Rating") {
                rating = value.value.to_string();
            }
            if include_subject {
                for property in xmp.property_array(xmp_ns::DC, "subject") {
                    subjects.push(property.value.to_string());
                }
            }
            // Register the digikam namespace
            let ns_digikam = "http://www.digikam.org/ns/1.0/";
            XmpMeta::register_namespace(ns_digikam, "digiKam")?;

            for property in xmp.property_array(ns_digikam, "TagsList") {
                let tag = property.value;
                if tag.starts_with(TagType::Species.digikam_tag_prefix()) {
                    species.push(
                        tag.strip_prefix(TagType::Species.digikam_tag_prefix())
                            .unwrap()
                            .to_string(),
                    );
                } else if tag.starts_with(TagType::Individual.digikam_tag_prefix()) {
                    individuals.push(
                        tag.strip_prefix(TagType::Individual.digikam_tag_prefix())
                            .unwrap()
                            .to_string(),
                    );
                }
            }
        }
    }
    Ok((
        species,
        individuals,
        subjects,
        datetime_original,
        datetime_digitized,
        time_modified,
        rating,
    ))
}

pub fn get_classifications(
    file_dir: PathBuf,
    output_dir: PathBuf,
    resource_type: ResourceType,
    independent: bool,
    include_subject: bool,
    include_time_modified: bool,
    debug_mode: bool,
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
    let image_filenames: Vec<String> = file_paths
        .clone()
        .into_iter()
        .map(|x| x.file_name().unwrap().to_string_lossy().into_owned())
        .collect();
    let num_images = file_paths.len();
    println!("Total {}: {}.", resource_type, num_images);
    let pb = ProgressBar::new(num_images as u64);

    let mut species_tags: Vec<String> = Vec::new();
    let mut individual_tags: Vec<String> = Vec::new();
    let mut subjects: Vec<String> = Vec::new();
    let mut datetime_originals: Vec<String> = Vec::new();
    let mut datetime_digitizeds: Vec<String> = Vec::new();
    let mut time_modifieds: Vec<String> = Vec::new();
    let mut ratings: Vec<String> = Vec::new();

    let result: Vec<_> = (0..num_images)
        .into_par_iter()
        .map(|i| {
            match retrieve_metadata(
                &file_paths[i].to_string_lossy().into_owned(),
                include_subject,
                include_time_modified,
            ) {
                Ok((
                    species,
                    individuals,
                    subjects,
                    datetime_original,
                    datetime_digitized,
                    time_modified,
                    rating,
                )) => {
                    pb.inc(1);
                    (
                        species.join("|"),
                        individuals.join("|"),
                        subjects.join("|"), // for just human review
                        datetime_original,
                        datetime_digitized,
                        time_modified,
                        rating,
                    )
                }
                Err(error) => {
                    pb.println(format!("{} in {}", error, file_paths[i].display()));
                    pb.inc(1);
                    (
                        "".to_string(),
                        "".to_string(),
                        "".to_string(),
                        "".to_string(),
                        "".to_string(),
                        "".to_string(),
                        "".to_string(),
                    )
                }
            }
        })
        .collect();
    for tag in result {
        species_tags.push(tag.0);
        individual_tags.push(tag.1);
        subjects.push(tag.2);
        datetime_originals.push(tag.3);
        datetime_digitizeds.push(tag.4);
        time_modifieds.push(tag.5);
        ratings.push(tag.6);
    }
    // Analysis
    let s_species = Series::new("species_tags".into(), species_tags);
    let s_individuals = Series::new("individual_tags".into(), individual_tags);
    let s_subjects = Series::new("subjects".into(), subjects);
    let s_datetime_original = Series::new("datetime_original".into(), datetime_originals);
    let s_datetime_digitized = Series::new("datetime_digitized".into(), datetime_digitizeds);
    let s_time_modified = Series::new("time_modified".into(), time_modifieds);
    let s_rating = Series::new("rating".into(), ratings);

    let mut df_raw = DataFrame::new(vec![
        Series::new("path".into(), image_paths),
        Series::new("filename".into(), image_filenames),
        s_species,
        s_individuals,
        s_subjects,
        s_datetime_original,
        s_datetime_digitized,
        s_time_modified,
        s_rating,
    ])?;

    let datetime_options = StrptimeOptions {
        // TODO: Serval does not include timezone info now
        format: Some("%Y-%m-%dT%H:%M:%S".into()),
        strict: false,
        ..Default::default()
    };
    let mut df_split = df_raw
        .clone()
        .lazy()
        .select([
            col("path"),
            col("filename"),
            col("datetime_original").str().strptime(
                DataType::Datetime(TimeUnit::Milliseconds, None),
                datetime_options.clone(),
                lit("raise"),
            ),
            col("datetime_digitized").str().strptime(
                DataType::Datetime(TimeUnit::Milliseconds, None),
                datetime_options.clone(),
                lit("raise"),
            ),
            col("time_modified")
                .str()
                .to_datetime(
                    Some(TimeUnit::Milliseconds),
                    None,
                    datetime_options,
                    lit("raise"),
                )
                .dt()
                .replace_time_zone(None, lit("raise"), NonExistent::Raise),
            col("species_tags")
                .str()
                .split(lit("|"))
                .alias(TagType::Species.col_name()),
            col("individual_tags")
                .str()
                .split(lit("|"))
                .alias(TagType::Individual.col_name()),
            col("subjects"),
            col("rating"),
        ])
        .collect()?;
    println!("{:?}", df_split);

    if !include_subject {
        let _ = df_split.drop_in_place("subjects")?;
    }
    if !include_time_modified {
        let _ = df_split.drop_in_place("time_modified")?;
    }
    if debug_mode {
        println!("{}", df_raw);
        let debug_csv_path = output_dir.join("raw.csv");
        let mut file = std::fs::File::create(debug_csv_path.clone())?;
        CsvWriter::new(&mut file)
            .include_bom(true)
            .with_datetime_format(Option::from("%Y-%m-%d %H:%M:%S".to_string()))
            .finish(&mut df_raw)?;
        println!("Saved to {}", debug_csv_path.to_string_lossy());
    }
    // For multiple tags in a single image (individual only for two species that won't be in the same image)
    let mut df_flatten = df_split
        .clone()
        .lazy()
        .select([col("*")])
        .explode([TagType::Individual.col_name()])
        .explode([TagType::Species.col_name()])
        .collect()?;
    println!("{}", df_flatten);

    let tags_csv_path = output_dir.join("tags.csv");
    let mut file = std::fs::File::create(tags_csv_path.clone())?;
    CsvWriter::new(&mut file)
        .with_datetime_format(Option::from("%Y-%m-%d %H:%M:%S".to_string()))
        .include_bom(true)
        .finish(&mut df_flatten)?;
    println!("Saved to {}", output_dir.join("tags.csv").to_string_lossy());

    let mut df_count_species = df_flatten
        .clone()
        .lazy()
        .select([col(TagType::Species.col_name()).value_counts(true, true, "count", false)])
        .unnest([TagType::Species.col_name()])
        .collect()?;
    println!("{:?}", df_count_species);

    let mut file = std::fs::File::create(output_dir.join("species_stats.csv"))?;
    CsvWriter::new(&mut file)
        .include_bom(true)
        .finish(&mut df_count_species)?;
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
    rename: bool,
    csv_path: PathBuf,
    output_dir: PathBuf,
) -> anyhow::Result<()> {
    let df = CsvReadOptions::default()
        .with_has_header(true)
        .with_infer_schema_length(Some(0)) // parse all columns as string
        .with_ignore_errors(true)
        .with_parse_options(
            CsvParseOptions::default()
                .with_try_parse_dates(true)
                .with_missing_is_null(true),
        )
        .try_into_reader_with_file_path(Some(csv_path))?
        .finish()?;

    let df_filtered: DataFrame = match filter_type {
        ExtractFilterType::Species => df
            .clone()
            .lazy()
            .filter(col(TagType::Species.col_name()).eq(lit(filter_value)))
            // .select([col("path")])
            .collect()?,
        ExtractFilterType::Path => df
            .clone()
            .lazy()
            //TODO use regex?
            .filter(col("path").str().contains_literal(lit(filter_value)))
            .collect()?,
        ExtractFilterType::Individual => df
            .clone()
            .lazy()
            .filter(col(TagType::Individual.col_name()).eq(lit(filter_value)))
            // .select([col("path")])
            .collect()?,
        ExtractFilterType::Rating => df
            .clone()
            .lazy()
            .filter(col("rating").eq(lit(filter_value)))
            // .select([col("path")])
            .collect()?,
        ExtractFilterType::Custom => df
            .clone()
            .lazy()
            .filter(col("custom").eq(lit(filter_value)))
            // .select([col("path")])
            .collect()?,
    };

    // Get the top level directory (to keep)
    let path_sample = df_filtered["path"].get(0)?.to_string().replace('"', ""); // TODO
    println!("Here is a sample of the file path ({}): ", path_sample);
    let mut num_option = 0;
    println!("0): File Only (no directory)");
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
        min: 0,
        max: num_option,
    };
    rl.set_helper(Some(h));
    let readline = rl.readline("Select the top level directory to keep: ");
    let deploy_path_index = readline?.trim().parse::<usize>()?;
    let path_strip = Path::new(&path_sample)
        .ancestors()
        .nth(deploy_path_index + 1)
        .unwrap();
    let pb = ProgressBar::new(df_filtered["path"].len().try_into()?);

    let paths = df_filtered.column("path")?.str()?;
    let species_tags = df_filtered.column(TagType::Species.col_name())?.str()?;
    let individual_tags = df_filtered.column(TagType::Individual.col_name())?.str()?;

    for (path, species_tag, individual_tag) in izip!(paths, species_tags, individual_tags) {
        let input_path = if path.unwrap().ends_with(".xmp") {
            path.unwrap().strip_suffix(".xmp").unwrap()
        } else {
            path.unwrap()
        };
        let output_path = if deploy_path_index == 0 {
            let relative_path_output = Path::new(input_path).file_name().unwrap();
            if rename {
                output_dir.join(format!(
                    "{}-{}-{}",
                    species_tag.unwrap(),
                    individual_tag.unwrap(),
                    relative_path_output.to_string_lossy()
                ))
            } else {
                output_dir.join(relative_path_output)
            }
        } else {
            let relative_path_output = Path::new(input_path)
                .strip_prefix(path_strip.to_string_lossy().replace('"', ""))?; // Where's quote come from
            if rename {
                output_dir
                    .join(relative_path_output.parent().unwrap())
                    .join(format!(
                        "{}-{}-{}",
                        species_tag.unwrap(),
                        individual_tag.unwrap(),
                        relative_path_output.file_name().unwrap().to_string_lossy()
                    ))
            } else {
                output_dir.join(relative_path_output)
            }
        };

        pb.println(format!("Copying to {}", output_path.to_string_lossy()));
        fs::create_dir_all(output_path.parent().unwrap())?;
        // check if the file exists, if so, rename it
        if output_path.exists() {
            let mut i = 1;
            let mut output_path_renamed = output_path.clone();
            while output_path_renamed.exists() {
                output_path_renamed = output_path.with_file_name(format!(
                    "{}_{}.{}",
                    output_path.file_stem().unwrap().to_string_lossy(),
                    i,
                    output_path.extension().unwrap().to_string_lossy()
                ));
                i += 1;
            }
            pb.println(format!(
                "Renamed to {}",
                output_path_renamed.to_string_lossy()
            ));
            fs::copy(input_path, output_path_renamed.clone())?;
            sync_modified_time(input_path.into(), output_path_renamed)?;
        } else {
            fs::copy(input_path, output_path.clone())?;
            sync_modified_time(input_path.into(), output_path)?;
        }
        pb.inc(1);
    }
    pb.finish_with_message("done");
    Ok(())
}

pub fn get_temporal_independence(csv_path: PathBuf, output_dir: PathBuf) -> anyhow::Result<()> {
    // Temporal independence analysis

    let df = CsvReadOptions::default()
        .with_has_header(true)
        .with_ignore_errors(true)
        .with_parse_options(CsvParseOptions::default().with_try_parse_dates(true))
        .try_into_reader_with_file_path(Some(csv_path))?
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

    let exclude = [
        "",
        "Blank",
        "Useless data",
        "Unidentified",
        "Human",
        "Unknown",
        "Blur",
    ]; // TODO: make it configurable
    let tag_exclude = Series::new("tag_exclude".into(), exclude);

    // Data processing
    let df_cleaned = df
        .clone()
        .lazy()
        .select([
            col("path")
                .str()
                .split(lit(get_path_seperator()))
                .list()
                .get(lit(num_option - deploy_path_index), false)
                .alias("deployment"),
            col("filename"),
            col("datetime_original").alias("time"),
            col(target.col_name()),
        ])
        .drop_nulls(None)
        .filter(col(target.col_name()).is_in(lit(tag_exclude)).not())
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
        .sort(["time"], SortMultipleOptions::default())?
        .sort([target.col_name()], SortMultipleOptions::default())?
        .sort(["deployment"], SortMultipleOptions::default())?;

    let mut df_capture_independent;
    if delta_time_compared_to == "LastRecord" {
        df_capture_independent = df_sorted
            .clone()
            .lazy()
            .rolling(
                col("time"),
                [col("deployment"), col(target.col_name())],
                RollingGroupOptions {
                    period: Duration::parse(format!("{}m", min_delta_time).as_str()),
                    offset: Duration::parse("0"),
                    ..Default::default()
                },
            )
            .agg([
                col(target.col_name()).count().alias("count"),
                col("filename").last(),
            ])
            .filter(col("count").eq(lit(1)))
            .select([
                col("deployment"),
                col("filename"),
                col("time"),
                col(target.col_name()),
            ])
            .collect()?;
        println!("{}", df_capture_independent);
    } else {
        df_sorted.as_single_chunk_par();
        let mut iters = df_sorted
            .columns(["time", target.col_name(), "deployment"])?
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
                || is_temporal_independent(last_indep_time.clone(), time, min_delta_time)?
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
            .filter(Series::new("independent".into(), capture_independent).lit())
            .collect()?;
        println!("{}", df_capture_independent);
    }

    fs::create_dir_all(output_dir.clone())?;
    let filename = format!("{}_temporal_independent.csv", target);
    let mut file = std::fs::File::create(output_dir.join(filename.clone()))?;
    CsvWriter::new(&mut file)
        .include_bom(true)
        .with_datetime_format(Option::from("%Y-%m-%d %H:%M:%S".to_string()))
        .finish(&mut df_capture_independent)?;
    println!("Saved to {}", output_dir.join(filename).to_string_lossy());

    let mut df_count_independent = df_capture_independent
        .clone()
        .lazy()
        .group_by_stable([col("deployment"), col(target.col_name())])
        .agg([col(target.col_name()).count().alias("count")])
        .collect()?;
    println!("{}", df_count_independent);

    let filename = format!("{}_temporal_independent_count.csv", target);
    let mut file = std::fs::File::create(output_dir.join(filename.clone()))?;
    CsvWriter::new(&mut file)
        .include_bom(true)
        .with_datetime_format(Option::from("%Y-%m-%d %H:%M:%S".to_string()))
        .finish(&mut df_count_independent)?;
    println!("Saved to {}", output_dir.join(filename).to_string_lossy());
    Ok(())
}
