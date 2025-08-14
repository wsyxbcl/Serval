use chrono::NaiveDateTime;
use core::fmt;
use polars::prelude::*;
use std::collections::HashSet;
use std::ffi::{OsStr, OsString};
use std::fs::{File, FileTimes};
use std::io;
use std::{
    env, fs,
    path::{Path, PathBuf},
};
use walkdir::{DirEntry, WalkDir};

#[derive(clap::ValueEnum, Clone, Copy, Debug)]
pub enum ResourceType {
    Xmp,
    Image,
    Video,
    Media, // Image or Video
    All,   // All resources (for serval align)
}

impl fmt::Display for ResourceType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl ResourceType {
    fn extension(self) -> Vec<&'static str> {
        match self {
            ResourceType::Image => vec!["jpg", "jpeg", "png"],
            ResourceType::Video => vec!["avi", "mp4", "mov"],
            ResourceType::Xmp => vec!["xmp"],
            ResourceType::Media => vec!["jpg", "jpeg", "png", "avi", "mp4", "mov"],
            ResourceType::All => vec!["jpg", "jpeg", "png", "avi", "mp4", "mov", "xmp"],
        }
    }

    fn is_resource(self, path: &Path) -> bool {
        match path.extension() {
            None => false,
            Some(x) => self
                .extension()
                .contains(&x.to_str().unwrap().to_lowercase().as_str()),
        }
    }
}

#[derive(clap::ValueEnum, PartialEq, Clone, Copy, Debug)]
pub enum TagType {
    Species,
    Individual,
    Count,
    Sex,
    Bodypart,
}

impl fmt::Display for TagType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl TagType {
    pub fn col_name(self) -> &'static str {
        match self {
            TagType::Individual => "individual",
            TagType::Species => "species",
            TagType::Count => "count",
            TagType::Sex => "sex",
            TagType::Bodypart => "bodypart",
        }
    }
    pub fn digikam_tag_prefix(self) -> &'static str {
        match self {
            TagType::Individual => "Individual/",
            TagType::Species => "Species/",
            TagType::Count => "Count/",
            TagType::Sex => "Sex/",
            TagType::Bodypart => "Bodypart/",
        }
    }
    pub fn adobe_tag_prefix(self) -> &'static str {
        match self {
            TagType::Individual => "Individual|",
            TagType::Species => "Species|",
            TagType::Count => "Count|",
            TagType::Sex => "Sex|",
            TagType::Bodypart => "Bodypart|",
        }
    }
}

#[derive(clap::ValueEnum, Clone, Copy, Debug)]
pub enum ExtractFilterType {
    Species,
    Path,
    Individual,
    Rating,
    Custom,
}

fn is_ignored(entry: &DirEntry) -> bool {
    entry
        .file_name()
        .to_str()
        .map(|s| s.starts_with('.') || s.contains("精选")) // ignore 精选 and .dtrash
        .unwrap_or(false)
}

// workaround for https://github.com/rust-lang/rust/issues/42869
// ref. https://github.com/sharkdp/fd/pull/72/files
fn path_to_absolute(path: PathBuf) -> io::Result<PathBuf> {
    if path.is_absolute() {
        return Ok(path);
    }
    let path = path.strip_prefix(".").unwrap_or(&path);
    env::current_dir().map(|current_dir| current_dir.join(path))
}

pub fn absolute_path(path: PathBuf) -> io::Result<PathBuf> {
    let path_buf = path_to_absolute(path)?;
    #[cfg(windows)]
    let path_buf = Path::new(
        path_buf
            .as_path()
            .to_string_lossy()
            .trim_start_matches(r"\\?\"),
    )
    .to_path_buf();
    Ok(path_buf)
}

pub fn path_enumerate(root_dir: PathBuf, resource_type: ResourceType) -> Vec<PathBuf> {
    let mut paths: Vec<PathBuf> = vec![];
    for entry in WalkDir::new(root_dir)
        .into_iter()
        .filter_entry(|e| !is_ignored(e))
        .filter_map(Result::ok)
        .filter(|e| resource_type.is_resource(e.path()))
    {
        paths.push(entry.into_path());
    }
    paths
}

pub fn resources_flatten(
    deploy_dir: PathBuf,
    working_dir: PathBuf,
    resource_type: ResourceType,
    dry_run: bool,
    move_mode: bool,
) -> anyhow::Result<()> {
    let deploy_id = deploy_dir.file_name().unwrap();
    let deploy_path = deploy_dir.to_str();

    let output_dir = working_dir.join(deploy_id);
    fs::create_dir_all(output_dir.clone())?;

    let resource_paths = path_enumerate(deploy_dir.clone(), resource_type);
    let num_resource = resource_paths.len();
    println!(
        "{} {}(s) found in {}",
        num_resource,
        resource_type,
        deploy_dir.to_str().unwrap()
    );

    let mut visited_path: HashSet<String> = HashSet::new();
    for resource in resource_paths {
        let mut output_path = PathBuf::new();
        let resource_parent = resource.parent().unwrap();
        let mut parent_names: Vec<OsString> = Vec::new();

        let mut resource_name = deploy_id.to_os_string();
        let mut current_parent = resource.parent();
        while let Some(parent) = current_parent {
            if parent.to_str() == deploy_path {
                break;
            }
            parent_names.push(parent.file_name().unwrap().to_os_string());
            current_parent = parent.parent();
        }

        parent_names.reverse();
        for parent_name in parent_names {
            resource_name.push("-");
            resource_name.push(&parent_name);
        }
        resource_name.push("-");
        resource_name.push(resource.file_name().unwrap());

        output_path.push(output_dir.join(resource_name));

        if !dry_run {
            let pb = indicatif::ProgressBar::new(num_resource as u64);

            if move_mode {
                fs::rename(resource, output_path)?;
                pb.inc(1);
            } else {
                fs::copy(resource, output_path)?;
                pb.inc(1);
            }
        } else if !visited_path.contains(resource_parent.to_str().unwrap()) {
            visited_path.insert(resource_parent.to_str().unwrap().to_string());
            println!(
                "DRYRUN sample: From {} to {}",
                resource.display(),
                output_path.display()
            );
        }
    }
    Ok(())
}

pub fn deployments_align(
    project_dir: PathBuf,
    output_dir: PathBuf,
    deploy_table: PathBuf,
    resource_type: ResourceType,
    dry_run: bool,
    move_mode: bool,
) -> anyhow::Result<()> {
    let deploy_df = CsvReadOptions::default()
        .try_into_reader_with_file_path(Some(deploy_table))?
        .finish()?;
    let deploy_array = deploy_df["deploymentID"].str()?;

    let deploy_iter = deploy_array.into_iter();
    let num_iter = deploy_iter.len();
    let pb = indicatif::ProgressBar::new(num_iter as u64);
    for deploy_id in deploy_iter {
        let (_, collection_name) = deploy_id.unwrap().rsplit_once('_').unwrap();
        let deploy_dir = project_dir.join(collection_name).join(deploy_id.unwrap());
        let collection_output_dir = output_dir.join(collection_name);
        resources_flatten(
            deploy_dir,
            collection_output_dir.clone(),
            resource_type,
            dry_run,
            move_mode,
        )?;
        pb.inc(1);
    }
    Ok(())
}

pub fn deployments_rename(project_dir: PathBuf, dry_run: bool) -> anyhow::Result<()> {
    // rename deployment path name to <deployment_name>_<collection_name>
    let mut count = 0;
    for entry in project_dir.read_dir()? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            let mut collection_dir = path;
            let original_collection_name = collection_dir.file_name().unwrap().to_str().unwrap();
            let collection_name_lower = original_collection_name.to_lowercase();
            if original_collection_name != collection_name_lower {
                let mut new_collection_dir = collection_dir.clone();
                new_collection_dir.set_file_name(&collection_name_lower);
                if dry_run {
                    println!(
                        "Will rename collection {original_collection_name} to {collection_name_lower}"
                    );
                } else {
                    println!(
                        "Renaming collection {} to {}",
                        collection_dir.display(),
                        new_collection_dir.display()
                    );
                    fs::rename(&collection_dir, &new_collection_dir)?;
                    collection_dir = new_collection_dir;
                }
            }
            let collection_name = collection_dir.file_name().unwrap().to_str().unwrap();
            for deploy in collection_dir.read_dir()? {
                let deploy_dir = deploy.unwrap().path();
                if deploy_dir.is_file() {
                    continue;
                }
                count += 1;
                let deploy_name = deploy_dir.file_name().unwrap().to_str().unwrap();
                if !deploy_name.contains(collection_name) {
                    if dry_run {
                        println!(
                            "Will rename {} to {}_{}",
                            deploy_name,
                            deploy_name.to_lowercase(),
                            collection_name.to_lowercase()
                        );
                    } else {
                        let mut deploy_id_dir = deploy_dir.clone();
                        deploy_id_dir.set_file_name(format!(
                            "{}_{}",
                            deploy_name.to_lowercase(),
                            collection_name.to_lowercase()
                        ));
                        println!(
                            "Renaming {} to {}",
                            deploy_dir.display(),
                            deploy_id_dir.display()
                        );
                        fs::rename(deploy_dir, deploy_id_dir)?;
                    }
                }
            }
        }
    }
    println!("Total directories: {count}");
    Ok(())
}

// copy xmp files to output_dir and keep the directory structure
pub fn copy_xmp(source_dir: PathBuf, output_dir: PathBuf) -> anyhow::Result<()> {
    let xmp_paths = path_enumerate(source_dir.clone(), ResourceType::Xmp);
    let num_xmp = xmp_paths.len();
    println!("{num_xmp} xmp files found");
    let pb = indicatif::ProgressBar::new(num_xmp as u64);

    for xmp in xmp_paths {
        let mut output_path = output_dir.clone();
        let relative_path = xmp.strip_prefix(&source_dir).unwrap();
        output_path.push(relative_path);
        fs::create_dir_all(output_path.parent().unwrap())?;
        fs::copy(xmp, output_path)?;
        pb.inc(1);
    }
    pb.finish();
    Ok(())
}

// Remove all XMP files recursively from a directory
pub fn remove_xmp_files(source_dir: PathBuf) -> anyhow::Result<()> {
    let xmp_paths = path_enumerate(source_dir.clone(), ResourceType::Xmp);
    let num_xmp = xmp_paths.len();
    
    if num_xmp == 0 {
        println!("No XMP files found in {}", source_dir.display());
        return Ok(());
    }
    
    println!("Found {} XMP files in {}", num_xmp, source_dir.display());
     
    let pb = indicatif::ProgressBar::new(num_xmp as u64);
    let mut removed_count = 0;
    
    for xmp in xmp_paths {
        match fs::remove_file(&xmp) {
            Ok(_) => {
                removed_count += 1;
                pb.inc(1);
            }
            Err(e) => {
                eprintln!("Failed to remove {}: {}", xmp.display(), e);
            }
        }
    }
    
    pb.finish();
    println!("Successfully removed {} XMP files", removed_count);
    Ok(())
}

pub fn is_temporal_independent(
    time_ref: String,
    time: String,
    min_delta_time: i32,
) -> anyhow::Result<bool> {
    // TODO Timezone
    let dt_ref = NaiveDateTime::parse_from_str(time_ref.as_str(), "%Y-%m-%d %H:%M:%S").unwrap();
    let dt = NaiveDateTime::parse_from_str(time.as_str(), "%Y-%m-%d %H:%M:%S").unwrap();
    let diff = dt - dt_ref;

    Ok(diff >= chrono::Duration::try_minutes(min_delta_time.into()).unwrap())
}

pub fn get_path_levels(path: String) -> Vec<String> {
    // Abandoned for performance
    // let normalized_path = PathBuf::from(path.replace('\\', "/"));
    // let levels: Vec<String> = normalized_path
    //     .components()
    //     .filter_map(|comp| match comp {
    //         Component::Normal(part) => Some(part.to_string_lossy().into_owned()),
    //         Component::Prefix(prefix) => Some(prefix.as_os_str().to_string_lossy().into_owned()), // For windows path prefixes
    //         _ => None, // Skip root and other components
    //     })
    //     .collect();

    let normalized_path = path.replace('\\', "/");
    let levels: Vec<String> = normalized_path
        .split('/')
        .map(|comp| comp.to_string())
        .collect();
    levels[1..levels.len() - 1].to_vec()
}

pub fn ignore_timezone(time: String) -> anyhow::Result<String> {
    let time_remove_designator = time.replace('Z', "");
    let time_ignore_zone = time_remove_designator.split('+').collect::<Vec<&str>>()[0];
    Ok(time_ignore_zone.to_string())
}

pub fn append_ext(ext: impl AsRef<OsStr>, path: PathBuf) -> anyhow::Result<PathBuf> {
    let mut os_string: OsString = path.into();
    os_string.push(".");
    os_string.push(ext.as_ref());
    Ok(os_string.into())
}

pub fn sync_modified_time(source: PathBuf, target: PathBuf) -> anyhow::Result<()> {
    let src = fs::metadata(source)?;
    let dest = File::options().write(true).open(target)?;
    let times = FileTimes::new()
        .set_accessed(src.accessed()?)
        .set_modified(src.modified()?);
    dest.set_times(times)?;
    Ok(())
}

pub fn tags_csv_translate(
    source_csv: PathBuf,
    taglist_csv: PathBuf,
    output_dir: PathBuf,
    from: &str,
    to: &str,
) -> anyhow::Result<()> {
    let source_df = CsvReadOptions::default()
        .try_into_reader_with_file_path(Some(source_csv.clone()))?
        .finish()?;
    let taglist_df = CsvReadOptions::default()
        .try_into_reader_with_file_path(Some(taglist_csv))?
        .finish()?;

    let mut result = source_df
        .clone()
        .lazy()
        .join(
            taglist_df.clone().lazy(),
            [col(TagType::Species.col_name())],
            [col(from)],
            JoinArgs::new(JoinType::Left),
        )
        .drop(cols([TagType::Species.col_name()]))
        .rename(vec![to], vec![TagType::Species.col_name()], true)
        // .with_column(col(to).alias("species"))
        .collect()?;

    let output_csv = output_dir.join(format!(
        "{}_translated.csv",
        source_csv.file_stem().unwrap().to_str().unwrap()
    ));
    fs::create_dir_all(output_dir.clone())?;
    let mut file = std::fs::File::create(&output_csv)?;
    CsvWriter::new(&mut file)
        .include_bom(true)
        .finish(&mut result)?;

    println!("Saved to {}", output_csv.display());
    Ok(())
}
