use crate::utils::{
    absolute_path, append_ext, get_path_levels, ignore_timezone, is_temporal_independent,
    path_enumerate, sync_modified_time, ExtractFilterType, ResourceType, TagType,
};
use chrono::{DateTime, Local};
use indicatif::ProgressBar;
use itertools::izip;
use polars::{lazy::dsl::StrptimeOptions, prelude::*};
use rayon::prelude::*;
use regex::Regex;
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
            .open_file(media.clone(), OpenFileOptions::default().repair_file())
            .is_ok()
        {
            let xmp_path = working_dir.join(append_ext("xmp", media.clone())?);
            // Check existence of xmp file
            if xmp_path.exists() {
                pb.inc(1);
                pb.println(format!("XMP file already exists: {}", xmp_path.display()));
                continue;
            }
            fs::File::create(xmp_path.clone())?;
            let mut xmp_string = "".to_string();
            if let Some(xmp) = media_xmp.xmp() {
                xmp_string = xmp.to_string_with_options(
                    ToStringOptions::default().set_newline("\n".to_string()),
                )?;
            }
            if !xmp_string.contains("exif:DateTimeOriginal")
                && !xmp_string.contains("xmp:MetadataDate")
            {
                if xmp_string.contains("xmp:CreateDate") {
                    // Workaround for video files, as some manufacturer only write to xmp:CreateDate
                    // And timezone is ignored for they write UTC-8 time but label as UTC
                    // i.e. strip the timezone info in xmp:CreateDate and xmp:ModifyDate if there is
                    // and skip the 0 timestamp if manufacturer write it
                    if !xmp_string.contains("1904-01-01") {
                        let re = Regex::new(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})Z").unwrap();
                        xmp_string = re.replace_all(&xmp_string, "$1").to_string();
                    }
                } else {
                    // Get the modified time of the file
                    if let Ok(metadata) = fs::metadata(media) {
                        if let Ok(modified_time) = metadata.modified() {
                            let datetime: DateTime<Local> = DateTime::from(modified_time);
                            let datetime_str = datetime.format("%Y-%m-%dT%H:%M:%S").to_string();
                            xmp_string = format!(
                                r#"<?xpacket begin="﻿" id="W5M0MpCehiHzreSzNTczkc9d"?>
<x:xmpmeta xmlns:x="adobe:ns:meta/" x:xmptk="XMP Core 6.0.0">
<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">
    <rdf:Description rdf:about="" xmlns:exif="http://ns.adobe.com/exif/1.0/">
    <exif:DateTimeOriginal>{}</exif:DateTimeOriginal>
    </rdf:Description>              
</rdf:RDF>
</x:xmpmeta>
<?xpacket end="w"?>
"#,
                                datetime_str
                            );
                        }
                    }
                }
            }
            fs::write(xmp_path, xmp_string)?;
            pb.inc(1);
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
    Vec<String>, // count
    Vec<String>, // sex
    Vec<String>, // subjects
    String,      // datetime
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
    // species, individual, sex count in digikam taglist / adobe hierarchicalsubject (species only), subject (for debugging),
    // datetime, datetime_digitized, rating and file modified time

    let mut f = XmpFile::new()?;
    f.open_file(file_path, OpenFileOptions::default())?;

    let mut species: Vec<String> = Vec::new();
    let mut individuals: Vec<String> = Vec::new();
    let mut count: Vec<String> = Vec::new();
    let mut sex: Vec<String> = Vec::new();
    let mut subjects: Vec<String> = Vec::new(); // for old digikam vesrion?
    let mut datetime = String::new();
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

    if f.open_file(file_path, OpenFileOptions::default()).is_ok() {
        if let Some(xmp) = f.xmp() {
            if let Some(value) = xmp.property_date(xmp_ns::EXIF, "DateTimeOriginal") {
                datetime = ignore_timezone(value.value.to_string())?;
            } else if let Some(value) = xmp.property_date(xmp_ns::XMP, "CreateDate") {
                // Workaround for video files, as some manufacturer only write to xmp:CreateDate
                // And timezone is ignored for they write UTC-8 time but label as UTC
                // i.e. we follow time shown in the picture without considering timezone in metadata
                // Ignore 0 timestamp in QuickTime:CreateDate, i.e. not start with 1904
                if !value.value.to_string().starts_with("1904") {
                    datetime = ignore_timezone(value.value.to_string())?;
                }
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

            // xmp namespace
            let ns_adobe = "http://ns.adobe.com/lightroom/1.0/";

            // use adobe hierarchicalSubject if available (digikam also writes to this field)
            for property in xmp.property_array(ns_adobe, "hierarchicalSubject") {
                let tag = property.value;
                if tag.starts_with(TagType::Species.adobe_tag_prefix()) {
                    species.push(
                        tag.strip_prefix(TagType::Species.adobe_tag_prefix())
                            .unwrap()
                            .to_string(),
                    );
                } else if tag.starts_with(TagType::Individual.adobe_tag_prefix()) {
                    individuals.push(
                        tag.strip_prefix(TagType::Individual.adobe_tag_prefix())
                            .unwrap()
                            .to_string(),
                    );
                } else if tag.starts_with(TagType::Count.adobe_tag_prefix()) {
                    count.push(
                        tag.strip_prefix(TagType::Count.adobe_tag_prefix())
                            .unwrap()
                            .to_string(),
                    );
                } else if tag.starts_with(TagType::Sex.adobe_tag_prefix()) {
                    sex.push(
                        tag.strip_prefix(TagType::Sex.adobe_tag_prefix())
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
        count,
        sex,
        subjects,
        datetime,
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

    let file_paths = path_enumerate(file_dir.clone(), resource_type);
    fs::create_dir_all(output_dir.clone())?;
    // Determine output filename based on parameters
    let output_suffix = format!(
        "_{}_{}{}{}_{}.csv",
        file_dir.file_name().unwrap().to_string_lossy(),
        resource_type.to_string().to_lowercase(),
        if include_subject { "-s" } else { "" },
        if include_time_modified { "-m" } else { "" },
        Local::now().format("%Y%m%d%H%M%S"),
    );

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
    let mut count_tags: Vec<String> = Vec::new();
    let mut sex_tags: Vec<String> = Vec::new();
    let mut subjects: Vec<String> = Vec::new();
    let mut datetimes: Vec<String> = Vec::new();
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
                    count,
                    sex,
                    subjects,
                    datetime,
                    datetime_digitized,
                    time_modified,
                    rating,
                )) => {
                    pb.inc(1);
                    (
                        species.join("|"),
                        individuals.join("|"),
                        count.join("|"),
                        sex.join("|"),
                        subjects.join("|"), // for just human review
                        datetime,
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
        count_tags.push(tag.2);
        sex_tags.push(tag.3);
        subjects.push(tag.4);
        datetimes.push(tag.5);
        datetime_digitizeds.push(tag.6);
        time_modifieds.push(tag.7);
        ratings.push(tag.8);
    }
    // Analysis
    let s_species = Column::new("species_tags".into(), species_tags);
    let s_individuals = Column::new("individual_tags".into(), individual_tags);
    let s_count = Column::new("count_tags".into(), count_tags);
    let s_sex = Column::new("sex_tags".into(), sex_tags);
    let s_subjects = Column::new("subjects".into(), subjects);
    let s_datetime = Column::new("datetime".into(), datetimes);
    let s_datetime_digitized = Column::new("datetime_digitized".into(), datetime_digitizeds);
    let s_time_modified = Column::new("time_modified".into(), time_modifieds);
    let s_rating = Column::new("rating".into(), ratings);

    let mut df_raw = DataFrame::new(vec![
        Column::new("path".into(), image_paths),
        Column::new("filename".into(), image_filenames),
        s_species,
        s_individuals,
        s_count,
        s_sex,
        s_subjects,
        s_datetime,
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
            col("datetime").str().strptime(
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
            col("count_tags").alias(TagType::Count.col_name()),
            col("sex_tags").alias(TagType::Sex.col_name()),
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

    let tags_csv_path = output_dir.join(format!("tags{}", output_suffix));
    let mut file = std::fs::File::create(tags_csv_path.clone())?;
    CsvWriter::new(&mut file)
        .with_datetime_format(Option::from("%Y-%m-%d %H:%M:%S".to_string()))
        .include_bom(true)
        .finish(&mut df_flatten)?;
    println!("Saved to {}", tags_csv_path.to_string_lossy());

    let mut df_count_species = df_flatten
        .clone()
        .lazy()
        .select([col(TagType::Species.col_name()).value_counts(true, true, "count", false)])
        .unnest([TagType::Species.col_name()])
        .collect()?;
    println!("{:?}", df_count_species);

    let species_stats_path = output_dir.join(format!("species_stats{}", output_suffix));
    let mut file = std::fs::File::create(species_stats_path.clone())?;
    CsvWriter::new(&mut file)
        .include_bom(true)
        .finish(&mut df_count_species)?;
    println!(
        "Saved to {}",
        output_dir.join(species_stats_path).to_string_lossy()
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
    // Create default values for missing columns
    // TODO: https://github.com/pola-rs/polars/issues/18372, wait for polars ergonomic improve
    let column_names = df.get_column_names_str();
    let required_columns = [TagType::Species.col_name(), TagType::Individual.col_name()];

    let mut df = required_columns.iter().fold(df.clone(), |acc_df, col| {
        if !column_names.contains(col) {
            acc_df
                .lazy()
                .with_columns([lit("").alias(*col)])
                .collect()
                .unwrap()
        } else {
            acc_df
        }
    });
    println!("{}", df);

    // Fill null values for columns that will be used for file naming
    if rename {
        df = df
            .clone()
            .lazy()
            .with_column(col(TagType::Species.col_name()).fill_null(lit("")))
            .with_column(col(TagType::Individual.col_name()).fill_null(lit("")))
            .collect()?;
    }

    let df_filtered: DataFrame = if filter_value == "ALL_VALUES" {
        match filter_type {
            ExtractFilterType::Species => df
                .clone()
                .lazy()
                .filter(col(TagType::Species.col_name()).is_not_null())
                .collect()?,
            ExtractFilterType::Path => df
                .clone()
                .lazy()
                .filter(col("path").is_not_null())
                .collect()?,
            ExtractFilterType::Individual => df
                .clone()
                .lazy()
                .filter(col(TagType::Individual.col_name()).is_not_null())
                .collect()?,
            ExtractFilterType::Rating => df
                .clone()
                .lazy()
                .filter(col("rating").is_not_null())
                .collect()?,
            ExtractFilterType::Custom => df
                .clone()
                .lazy()
                .filter(col("custom").is_not_null())
                .collect()?,
        }
    } else {
        match filter_type {
            ExtractFilterType::Species => df
                .clone()
                .lazy()
                .filter(col(TagType::Species.col_name()).eq(lit(filter_value)))
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
                .collect()?,
            ExtractFilterType::Rating => df
                .clone()
                .lazy()
                .filter(col("rating").eq(lit(filter_value)))
                .collect()?,
            ExtractFilterType::Custom => df
                .clone()
                .lazy()
                .filter(col("custom").eq(lit(filter_value)))
                .collect()?,
        }
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
        let input_path_xmp: String;
        let input_path_media: String;
        if path.unwrap().ends_with(".xmp") {
            input_path_xmp = path.unwrap().to_string();
            input_path_media = path.unwrap().strip_suffix(".xmp").unwrap().to_string();
        } else {
            input_path_xmp = path.unwrap().to_string() + ".xmp";
            input_path_media = path.unwrap().to_string();
        }

        let (mut output_path_xmp, mut output_path_media) = if deploy_path_index == 0 {
            let relative_path_output_xmp = Path::new(&input_path_xmp).file_name().unwrap();
            let relative_path_output_media = Path::new(&input_path_media).file_name().unwrap();
            if rename {
                (
                    output_dir.join(format!(
                        "{}-{}-{}",
                        species_tag.unwrap(),
                        individual_tag.unwrap(),
                        relative_path_output_xmp.to_string_lossy()
                    )),
                    output_dir.join(format!(
                        "{}-{}-{}",
                        species_tag.unwrap(),
                        individual_tag.unwrap(),
                        relative_path_output_media.to_string_lossy()
                    )),
                )
            } else {
                (
                    output_dir.join(relative_path_output_xmp),
                    output_dir.join(relative_path_output_media),
                )
            }
        } else {
            let relative_path_output_xmp = Path::new(&input_path_xmp)
                .strip_prefix(path_strip.to_string_lossy().replace('"', ""))?; // Where's quote come from
            let relative_path_output_media = Path::new(&input_path_media)
                .strip_prefix(path_strip.to_string_lossy().replace('"', ""))?; // Where's quote come from
            if rename {
                // TODO let user define the pattern
                (
                    output_dir
                        .join(relative_path_output_xmp.parent().unwrap())
                        .join(format!(
                            "{}-{}-{}",
                            species_tag.unwrap(),
                            individual_tag.unwrap(),
                            relative_path_output_xmp
                                .file_name()
                                .unwrap()
                                .to_string_lossy()
                        )),
                    output_dir
                        .join(relative_path_output_media.parent().unwrap())
                        .join(format!(
                            "{}-{}-{}",
                            species_tag.unwrap(),
                            individual_tag.unwrap(),
                            relative_path_output_media
                                .file_name()
                                .unwrap()
                                .to_string_lossy()
                        )),
                )
            } else {
                (
                    output_dir.join(relative_path_output_xmp),
                    output_dir.join(relative_path_output_media),
                )
            }
        };

        pb.println(format!(
            "Copying to {}",
            output_path_media.to_string_lossy()
        ));
        fs::create_dir_all(output_path_media.parent().unwrap())?;
        // check if the file exists, if so, rename it
        if output_path_media.exists() {
            let mut i = 1;
            let mut output_path_media_renamed = output_path_media.clone();
            while output_path_media_renamed.exists() {
                output_path_media_renamed = output_path_media.with_file_name(format!(
                    "{}_{}.{}",
                    output_path_media.file_stem().unwrap().to_string_lossy(),
                    i,
                    output_path_media.extension().unwrap().to_string_lossy()
                ));
                i += 1;
            }
            // get the xmp file from output_path_media_renamed
            let output_path_xmp_renamed =
                output_path_media_renamed.to_string_lossy().into_owned() + ".xmp";
            pb.println(format!(
                "Renamed to {}",
                output_path_media_renamed.to_string_lossy()
            ));
            output_path_media = output_path_media_renamed.clone();
            output_path_xmp = output_path_xmp_renamed.into();
        }

        fs::copy(input_path_media.clone(), output_path_media.clone())?;
        if let Err(_err) = fs::copy(input_path_xmp, output_path_xmp) {
            pb.println("Missing XMP file ,tag info for certain video files may be lost.");
            // eprintln!("Error: {}", err);
        }
        sync_modified_time(input_path_media.into(), output_path_media)?;

        pb.inc(1);
    }
    pb.finish_with_message("done");
    Ok(())
}

pub fn get_temporal_independence(csv_path: PathBuf, output_dir: PathBuf) -> anyhow::Result<()> {
    // Temporal independence analysis

    let mut df = match CsvReadOptions::default()
        .with_has_header(true)
        .with_ignore_errors(false)
        .with_parse_options(CsvParseOptions::default().with_try_parse_dates(true))
        .try_into_reader_with_file_path(Some(csv_path))
        .and_then(|reader| reader.finish())
    {
        Ok(df) => df,
        Err(e) => {
            // if e.to_string().contains("could not parse") {
            //     eprint!("{}", e);
            //     eprintln!("Error: Could not parse datetime in the CSV file.");
            //     eprintln!("This is likely caused by saving the CSV in software like Excel, which can result in loss of data precision.");
            //     eprintln!("The program will not parse the datetime until the format is corrected");
            //     eprintln!("\x1b[1;33mHint: Ensure the datetime format in your file matches the pattern 'yyyy-MM-dd HH:mm:ss'.\x1b[0m");
            // } 
            eprintln!("Error: {}", e);
            std::process::exit(1);
        }
    };

    // Rename datetime_original to datetime, adapts to old tags.csv
    let df = match df.rename("datetime_original", "datetime".into()) {
        Ok(renamed_df) => renamed_df,
        Err(_) => &mut df,
    };

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
    let path_levels = get_path_levels(path_sample);
    for (i, entry) in path_levels.iter().enumerate() {
        println!("{}): {}", i + 1, entry);
    }
    let h = NumericSelectValidator {
        min: 1,
        max: path_levels.len().try_into()?,
    };
    rl.set_helper(Some(h));
    let readline = rl.readline("Select the number corresponding to the deployment: ");
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
            col("path"),
            col("path")
                .str()
                .replace_all(lit("\\"), lit("/"), true)
                .str()
                .split(lit("/"))
                .list()
                .get(lit(deploy_path_index), false)
                .alias("deployment"),
            col("datetime").alias("time"),
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
                col("path").last(),
            ])
            .filter(col("count").eq(lit(1)))
            .select([
                col("deployment"),
                col("path"),
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
            .map(|s| s.as_materialized_series().iter())
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

    // Include parameters in the output filename, LIR: Last Independent Record, LR: Last Record
    let output_suffix = format!(
        "_{}_{}m_{}.csv",
        target.to_string().to_lowercase(),
        min_delta_time,
        if delta_time_compared_to == "LastIndependentRecord" {
            "LIR"
        } else {
            "LR"
        },
    );
    fs::create_dir_all(output_dir.clone())?;
    let filename = format!("temporal-independence{}", output_suffix);
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

    let filename = "count_by_deployment.csv";
    let mut file = std::fs::File::create(output_dir.join(filename))?;
    CsvWriter::new(&mut file)
        .include_bom(true)
        .with_datetime_format(Option::from("%Y-%m-%d %H:%M:%S".to_string()))
        .finish(&mut df_count_independent)?;
    println!("Saved to {}", output_dir.join(filename).to_string_lossy());

    if target == TagType::Species {
        let mut df_count_independent_species = df_capture_independent
            .clone()
            .lazy()
            .group_by_stable([col(TagType::Species.col_name())])
            .agg([col(TagType::Species.col_name()).count().alias("count")])
            .collect()?;
        println!("{}", df_count_independent_species);

        let filename = "count_all.csv";
        let mut file = std::fs::File::create(output_dir.join(filename))?;
        CsvWriter::new(&mut file)
            .include_bom(true)
            .with_datetime_format(Option::from("%Y-%m-%d %H:%M:%S".to_string()))
            .finish(&mut df_count_independent_species)?;
        println!("Saved to {}", output_dir.join(filename).to_string_lossy());
    }
    Ok(())
}
