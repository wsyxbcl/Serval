use core::fmt;
use chrono::NaiveDateTime;
use std::{path::{PathBuf, Path}, fs, env};
use walkdir::WalkDir;
use polars::prelude::*;

#[derive(Clone, Copy, Debug)]
pub enum ResourceType {
    Xmp,
    Image,
    _Video, // TODO: video support
}

impl fmt::Display for ResourceType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)        
    }
}

impl ResourceType {
    fn extension(self) -> Vec<&'static str>{
        match self {
            ResourceType::Image => vec!["jpg", "jpeg", "png"],
            ResourceType::_Video => vec!["avi", "mp4"],
            ResourceType::Xmp => vec!["xmp"],
        }
    }

    fn is_resource(self, path: &Path) -> bool {
        match path.extension() {
            None => false,
            Some(x) => self.extension().contains(&x.to_str().unwrap().to_lowercase().as_str()),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub enum TagType {
    Species,
    Individual,
}

impl fmt::Display for TagType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)        
    }
}

pub fn path_enumerate(root_dir: PathBuf, resource_type: ResourceType) -> Vec<PathBuf> {
    let mut paths: Vec<PathBuf> = vec![];
    for entry in WalkDir::new(root_dir)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|e| resource_type.is_resource(e.path())) {
            paths.push(entry.into_path());
        }
        paths
}


pub fn resources_align(deploy_dir: PathBuf, working_dir: PathBuf, dry_run: bool, move_mode: bool) { 
    let deploy_id = deploy_dir.file_name().unwrap();
    let deploy_path = deploy_dir.to_str();

    let collection_name = working_dir.file_name().unwrap();
    let output_dir = working_dir.join(deploy_id);
    fs::create_dir_all(output_dir.clone()).unwrap();

    let resource_paths = path_enumerate(deploy_dir.clone(), ResourceType::Image);
    println!("{} images found: ", resource_paths.len());
    // println!("{:?}", resource_paths);

    for resource in resource_paths {
        let mut output_path = PathBuf::new();
        let resource_name = if resource.parent().unwrap().to_str() == deploy_path {
            let mut resource_name = collection_name.to_os_string();
            resource_name.push("-");
            resource_name.push(resource.file_name().unwrap());
            resource_name
        } else {
            let mut resource_name = collection_name.to_os_string();
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
                fs::rename(resource, output_path).unwrap();
            } else {
                println!("copy {} to {}", resource.display(), output_path.display());
                fs::copy(resource, output_path).unwrap();
            }
        } else if move_mode {
                println!("DRYRUN: move {} to {}", resource.display(), output_path.display());
        } else {
                println!("DRYRUN: copy {} to {}", resource.display(), output_path.display());
        }
    }

}

pub fn deployments_align(project_dir: PathBuf, output_dir: PathBuf, deploy_table: PathBuf, dry_run: bool, move_mode: bool) {
    // TODO: add file/path filter
    let deploy_df = CsvReader::from_path(deploy_table).unwrap().finish().unwrap();
    let deploy_array = deploy_df["deploymentID"].utf8().unwrap();
    
    // deploy_array.into_iter()
    //     .for_each(|deploy| println!("{:?}", deploy))

    let deploy_iter = deploy_array.into_iter();
    for deploy_id in deploy_iter {
        let (_, collection_name) = deploy_id.unwrap().rsplit_once('_').unwrap();
        let deploy_dir = project_dir.join(collection_name).join(deploy_id.unwrap());
        let collection_output_dir = output_dir.join(collection_name);
        resources_align(deploy_dir, collection_output_dir.clone(), dry_run, move_mode);
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
}


pub fn deployments_rename(project_dir: PathBuf, dry_run: bool) {
    // rename deployment path name to <deployment_name>_<collection_name>
    let mut count = 0;
    for entry in project_dir.read_dir().unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        if path.is_dir() {
            let collection = path;
            for deploy in collection.read_dir().unwrap() {
                let deploy_dir = deploy.unwrap().path();
                if deploy_dir.is_file() {
                    continue;
                }
                count += 1;
                let collection_name = deploy_dir.parent().unwrap().file_name().unwrap().to_str().unwrap();
                let deploy_name = deploy_dir.file_name().unwrap().to_str().unwrap();
                if !deploy_name.contains(collection_name) {
                    if dry_run {
                        println!("Will rename {} to {}_{}", deploy_name, deploy_name, collection_name);
                    } else {
                        let mut deploy_id_dir = deploy_dir.clone();
                        deploy_id_dir.set_file_name(
                            format!("{}_{}", deploy_name, collection_name)
                        );
                        fs::rename(deploy_dir, deploy_id_dir).unwrap();
                    }
                }
            }
        }
    }
    println!("Total directories: {}", count);
}

pub fn is_temporal_independent(time_ref: String, time: String, min_delta_time: i32) -> bool {
    // TODO Timezone
    let dt_ref = NaiveDateTime::parse_from_str(time_ref.as_str(), "%Y-%m-%d %H:%M:%S").unwrap();
    let dt = NaiveDateTime::parse_from_str(time.as_str(), "%Y-%m-%d %H:%M:%S").unwrap();
    let diff = dt - dt_ref;
    
    diff.num_minutes() > min_delta_time.into()
}

pub fn is_windows() -> bool {
    env::consts::OS == "windows"
}