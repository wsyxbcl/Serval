use chrono::NaiveDateTime;
use core::fmt;
use polars::prelude::*;
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
}

impl fmt::Display for ResourceType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl ResourceType {
    fn extension(self) -> Vec<&'static str> {
        match self {
            ResourceType::Image => vec!["jpg", "jpeg", "png"],
            ResourceType::Video => vec!["avi", "mp4", "mov"],
            ResourceType::Xmp => vec!["xmp"],
            ResourceType::Media => vec!["jpg", "jpeg", "png", "avi", "mp4", "mov"],
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

#[derive(clap::ValueEnum, Clone, Copy, Debug)]
pub enum TagType {
    Species,
    Individual,
}

impl fmt::Display for TagType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl TagType {
    pub fn col_name(self) -> &'static str {
        match self {
            TagType::Individual => "individual",
            TagType::Species => "species",
        }
    }
    pub fn digikam_tag_prefix(self) -> &'static str {
        match self {
            TagType::Individual => "Individual/",
            TagType::Species => "Species/",
        }
    }
}

#[derive(clap::ValueEnum, Clone, Copy, Debug)]
pub enum ExtractFilterType {
    Species,
    PathRegex,
    Individual,
    Rating,
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

pub fn resources_align(
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
    println!(
        "{} {}(s) found in {}",
        resource_paths.len(),
        resource_type,
        deploy_dir.to_str().unwrap()
    );

    for resource in resource_paths {
        let mut output_path = PathBuf::new();
        let resource_name = if resource.parent().unwrap().to_str() == deploy_path {
            let mut resource_name = deploy_id.to_os_string();
            resource_name.push("-");
            resource_name.push(resource.file_name().unwrap());
            resource_name
        } else {
            let mut resource_name = deploy_id.to_os_string();
            resource_name.push("-");
            resource_name.push(resource.parent().unwrap().file_name().unwrap());
            resource_name.push("-");
            resource_name.push(resource.file_name().unwrap());
            resource_name
        };
        output_path.push(output_dir.join(resource_name));
        if !dry_run {
            if move_mode {
                println!("move {} to {}", resource.display(), output_path.display());
                fs::rename(resource, output_path)?;
            } else {
                println!("copy {} to {}", resource.display(), output_path.display());
                fs::copy(resource, output_path)?;
            }
        } else if move_mode {
            println!(
                "DRYRUN: move {} to {}",
                resource.display(),
                output_path.display()
            );
        } else {
            println!(
                "DRYRUN: copy {} to {}",
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
    let deploy_df = CsvReader::from_path(deploy_table)?.finish()?;
    let deploy_array = deploy_df["deploymentID"].str()?;

    // deploy_array.into_iter()
    //     .for_each(|deploy| println!("{:?}", deploy))

    let deploy_iter = deploy_array.into_iter();
    for deploy_id in deploy_iter {
        let (_, collection_name) = deploy_id.unwrap().rsplit_once('_').unwrap();
        let deploy_dir = project_dir.join(collection_name).join(deploy_id.unwrap());
        let collection_output_dir = output_dir.join(collection_name);
        resources_align(
            deploy_dir,
            collection_output_dir.clone(),
            resource_type,
            dry_run,
            move_mode,
        )?;
    }
    // for entry in project_dir.read_dir().unwrap() {
    //     let collection_path = entry.unwrap().path();
    //         for entry in collection_path.read_dir().unwrap() {
    //             let deploy_path = entry.unwrap().path();
    //             // let deploy_name = deploy_path.file_name().unwrap();
    //             // let output_deploy_path = output_dir.join(deploy_name);

    //             // TODO: Fix directory layout (output to <project_name>/<collection_name>/<deployment_id>)
    //             resources_align(deploy_path, output_dir.clone());
    //         }
    // }
    Ok(())
}

pub fn deployments_rename(project_dir: PathBuf, dry_run: bool) -> anyhow::Result<()> {
    // rename deployment path name to <deployment_name>_<collection_name>
    let mut count = 0;
    for entry in project_dir.read_dir()? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            let collection = path;
            for deploy in collection.read_dir()? {
                let deploy_dir = deploy.unwrap().path();
                if deploy_dir.is_file() {
                    continue;
                }
                count += 1;
                let collection_name = deploy_dir
                    .parent()
                    .unwrap()
                    .file_name()
                    .unwrap()
                    .to_str()
                    .unwrap();
                let deploy_name = deploy_dir.file_name().unwrap().to_str().unwrap();
                if !deploy_name.contains(collection_name) {
                    if dry_run {
                        println!(
                            "Will rename {} to {}_{}",
                            deploy_name, deploy_name, collection_name
                        );
                    } else {
                        let mut deploy_id_dir = deploy_dir.clone();
                        deploy_id_dir.set_file_name(format!("{}_{}", deploy_name, collection_name));
                        fs::rename(deploy_dir, deploy_id_dir)?;
                    }
                }
            }
        }
    }
    println!("Total directories: {}", count);
    Ok(())
}

// copy xmp files to output_dir and keep the directory structure
pub fn copy_xmp(source_dir: PathBuf, output_dir: PathBuf) -> anyhow::Result<()> {
    let xmp_paths = path_enumerate(source_dir.clone(), ResourceType::Xmp);
    let num_xmp = xmp_paths.len();
    println!("{} xmp files found", num_xmp);
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

pub fn is_temporal_independent(time_ref: String, time: String, min_delta_time: i32) -> bool {
    // TODO Timezone
    let dt_ref = NaiveDateTime::parse_from_str(time_ref.as_str(), "%Y-%m-%d %H:%M:%S").unwrap();
    let dt = NaiveDateTime::parse_from_str(time.as_str(), "%Y-%m-%d %H:%M:%S").unwrap();
    let diff = dt - dt_ref;

    diff >= chrono::Duration::try_minutes(min_delta_time.into()).unwrap()
}

pub fn get_path_seperator() -> &'static str {
    if env::consts::OS == "windows" {
        r"\"
    } else {
        r"/"
    }
}
