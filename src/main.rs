use std::{path::{PathBuf, Path}, fs};

use clap::{Parser, Subcommand};
use indicatif::ProgressBar;
use polars::prelude::*;
use walkdir::WalkDir;
use xmp_toolkit::{ OpenFileOptions, XmpFile, XmpMeta};

fn main() -> std::io::Result<()> {
    let args = Cli::parse();

    match args.command {
        Commands::Align { path, output, project,deploy_table, dryrun} => {
            if project {
                println!("Aligning deployments in {}", path.display());
                deployments_align(path, output, deploy_table, dryrun);
            } else {
                println!("Aligning resources in {}", path.display());
                resources_align(path, output, dryrun);
            }
        }
        Commands::Observe { media_dir ,output} => {
            get_classifications(media_dir, output);
        }
        Commands::Rename { project_dir, dryrun} => {
            rename_deployments(project_dir, dryrun);
        }

    }
    Ok(())
}


#[derive(Parser, Debug)]
#[command(name = "Serval")]
#[command(author, version, about)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Align resources in given directory for deployment or project(recursive)
    #[command(arg_required_else_help = true)]
    Align {
        path: PathBuf,

        /// Directory for output(aligned) resources
        #[arg(short, long, value_name = "OUTPUT_DIR", required = true)]
        output: PathBuf,

        /// If the given path is a Project
        #[arg(short, long)]
        project: bool,

        /// Path for the deployments table (deployments.csv)
        #[arg(short, long, value_name = "FILE", required = true)]
        deploy_table: PathBuf,

        /// Dry run
        #[arg(long)]
        dryrun: bool,
    },
    /// Read media EXIF for observation data
    #[command(arg_required_else_help = true)]
    Observe {
        media_dir: PathBuf,

        /// Directory for output(tags.csv)
        #[arg(short, long, value_name = "OUTPUT_DIR", required = true)]
        output: PathBuf,
    },
    /// Rename deployment directory to deployment_id, in the manner of combining collection_name of deployment_name
    #[command(arg_required_else_help = true)]
    Rename {
        project_dir: PathBuf,

        /// Dry run
        #[arg(long)]
        dryrun: bool,
    }

}


fn is_image(path: &Path) -> bool {
    match path.extension() {
        None => false,
        Some(x) => ["jpg", "jpeg", "png"].contains(&x.to_str().unwrap().to_lowercase().as_str()),
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

fn image_path_enumerate(root_dir: PathBuf) -> Vec<PathBuf> {
    let mut image_paths: Vec<PathBuf> = vec![];
    for entry in WalkDir::new(root_dir)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|e| is_image(e.path())) {
            image_paths.push(entry.into_path());
        }
    image_paths
}

fn resources_align(deploy_dir: PathBuf, working_dir: PathBuf, dry_run: bool) { 
    let deploy_id = deploy_dir.file_name().unwrap();
    let deploy_path = deploy_dir.to_str();

    let collection_name = working_dir.file_name().unwrap();
    let output_dir = working_dir.join(deploy_id);
    fs::create_dir_all(output_dir.clone()).unwrap();

    let resource_paths = image_path_enumerate(deploy_dir.clone());
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
            println!("copy {} to {}", resource.display(), output_path.display());
            fs::copy(resource, output_path).unwrap();
        } else {
            println!("DRYRUN: copy {} to {}", resource.display(), output_path.display());
        }
    }

}

fn deployments_align(project_dir: PathBuf, output_dir: PathBuf, deploy_table: PathBuf, dry_run: bool) {
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
        resources_align(deploy_dir, collection_output_dir.clone(), dry_run);
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


fn get_classifications(media_dir: PathBuf, output_dir: PathBuf) {
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
    println!("Saved to {}/tags.csv", output_dir.to_string_lossy());

    let mut df_count_species = df_flatten
        .lazy()
        .select([col("species").value_counts(true, true)])
        .unnest(["species"])
        .collect()
        .unwrap();
    println!("{:?}", df_count_species);

    let mut file = std::fs::File::create(output_dir.join("species_stats.csv")).unwrap();
    CsvWriter::new(&mut file).finish(&mut df_count_species).unwrap();
    println!("Saved to {}/species_stats.csv", output_dir.to_string_lossy());
}


fn rename_deployments(project_dir: PathBuf, dry_run: bool) {
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
// fn digikam_tag_parser(tags: String) 
