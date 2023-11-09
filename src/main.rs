use std::{path::{PathBuf, Path}, fs};

use clap::{Parser, Subcommand};
use polars::prelude::*;

const TAG: &str = "Xmp.digiKam.TagsList";

fn main() -> std::io::Result<()> {
    let args = Cli::parse();

    match args.command {
        Commands::Align { path, output, project} => {
            if project {
                println!("Aligning deployments in {}", path.display());
                deployments_align(path, output);
            } else {
                println!("Aligning resources in {}", path.display());
                resources_align(path, output);
            }
        }
        Commands::Observe { media_dir ,output} => {
            get_classifications(image_path_enumerate(media_dir), output);
        }
        Commands::Rename { project_dir, dryrun} => {
            rename_deployments(project_dir, dryrun);
        }

    }
    Ok(())
}


#[derive(Parser, Debug)]
#[command(name = "Serval")]
#[command(version = "0.1.0")]
#[command(about = "Serval helps you prepare data for trapper", long_about = None )]
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

        /// Directory for output files
        #[arg(short, long, value_name = "FILE", required = true)]
        output: PathBuf,

        /// Aligh resources for entire project
        #[arg(short, long)]
        project: bool,
    },
    /// Read media EXIF for observation data
    #[command(arg_required_else_help = true)]
    Observe {
        media_dir: PathBuf,

        /// Directory for output files
        #[arg(short, long, value_name = "FILE", required = true)]
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

fn image_path_enumerate(root_dir: PathBuf) -> Vec<PathBuf> {
    // Find all image in given dir recursivly
    if root_dir.is_file() {
        if is_image(&root_dir) {
            vec![root_dir]
        } else {
            vec![]
        }
    } else {
        let mut image_paths: Vec<PathBuf> = vec![];

        for entry in root_dir.read_dir().unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
    
            if path.is_dir() {
                // println!("{:?}", path.to_str());
                image_paths.extend(image_path_enumerate(path));
            } else if path.is_file() && is_image(&path) {
                image_paths.push(path);
            }
        }

        image_paths
    }
}

fn resources_align(deploy_dir: PathBuf, working_dir: PathBuf) { 
    let deploy_id = deploy_dir.file_name().unwrap();
    let deploy_path = deploy_dir.to_str();

    let output_dir = working_dir.join(deploy_id);
    fs::create_dir_all(output_dir.clone()).unwrap();

    let resource_paths = image_path_enumerate(deploy_dir.clone());
    println!("{} images found: ", resource_paths.len());
    // println!("{:?}", resource_paths);

    for resource in resource_paths {
        let mut output_path = PathBuf::new();
        let resource_name = if resource.parent().unwrap().to_str() == deploy_path {
            resource.file_name().unwrap().to_os_string()
        } else {
            let mut resource_name = resource.parent().unwrap().file_name().unwrap().to_owned();
            resource_name.push("-");
            resource_name.push(resource.file_name().unwrap());
            resource_name
        };
        output_path.push(output_dir.join(resource_name));
        println!("copy {} to {}", resource.display(), output_path.display());
        fs::copy(resource, output_path).unwrap();
    }

}

fn deployments_align(project_dir: PathBuf, output_dir: PathBuf) {
    for entry in project_dir.read_dir().unwrap() {
        let deploy_path = entry.unwrap().path();
        // let deploy_name = deploy_path.file_name().unwrap();
        // let output_deploy_path = output_dir.join(deploy_name);
        resources_align(deploy_path, output_dir.clone());
    }
}


fn get_classifications(image_paths: Vec<PathBuf>, output_dir: PathBuf) {
    // Get tag info from the old digikam workflow in shanshui
    let image_names: Vec<String> = image_paths.clone().into_iter().map(|x| x.file_name().unwrap().to_string_lossy().into_owned()).collect();
    let mut image_tags: Vec<Option<String>> = Vec::new();
    let s_filenames = Series::new("filename", image_names.clone());
    for path in image_paths {
        let meta = rexiv2::Metadata::new_from_path(path.clone()).unwrap();
        match meta.get_tag_string(TAG) {
            Ok(tag) => image_tags.push(Some(tag)),
            Err(error) => {
                println!("{:?} in {:?}", error, path.display());
                image_tags.push(None)
            },

        };
        // println!("{:?}: {:?}", path.file_name().unwrap(), meta.get_tag_multiple_strings(tag).unwrap());
    }
    let s_tags = Series::new("tags", image_tags);
    let df_raw = DataFrame::new(vec![s_filenames, s_tags]).unwrap();
    
    let df_extract_all = df_raw
        .clone()
        .lazy()
        // .with_columns([col("tags").str().split(lit(",")),])
        .with_columns([col("tags")
            .str()
            .extract_all(lit(r"Species\/(.*?)(?:,|$)"))
            // extract_all can't select regex groups: https://github.com/pola-rs/polars/issues/11857
            // so using manual strip here
            .list()
            .eval(col("").str().strip_prefix(lit("Species/")).str().strip_suffix(lit(",")), true)
            .alias("species")])
        .with_columns([col("tags")
            .str()
            .extract_all(lit(r"Individual\/(.*?)(?:,|$)"))
            .list()
            .eval(col("").str().strip_prefix(lit("Individual/")).str().strip_suffix(lit(",")), true)
            .alias("individuals")])
        .collect()
        .unwrap();
    println!("{}", df_extract_all);

    // Note that there's only individual info for P. uncia
    let mut df_flatten = df_extract_all
        .clone()
        .lazy()
        .select([col("*").exclude(["tags"])])
        .explode(["individuals"])
        .explode(["species"])
        .collect()
        .unwrap();
    println!("{}", df_flatten);

    let mut file = std::fs::File::create(output_dir.join("tags.csv")).unwrap();
    CsvWriter::new(&mut file).finish(&mut df_flatten).unwrap();
    
    // extract_groups, issue also described in #11857
    // let df_extract_groups = df_raw
    //     .clone()
    //     .lazy()
    //     // .with_columns([col("tags").str().split(lit(",")),])
    //     .with_columns([col("tags")
    //         .str()
    //         .extract_groups(r"Species\/(.*?)(?:,|$)")
    //         .unwrap()
    //         .alias("Species")])
    //     .with_columns([col("tags")
    //         .str()
    //         .extract_groups(r"Individual\/(.*?)(?:,|$)")
    //         .unwrap()
    //         .alias("Individuals")])
    //     .collect()
    //     .unwrap();

}


fn rename_deployments(project_dir: PathBuf, dry_run: bool) {
    for entry in project_dir.read_dir().unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        if path.is_dir() {
            let collection = path;
            for deployment in collection.read_dir().unwrap() {
                let deployment_dir = deployment.unwrap().path();
                if deployment_dir.is_file() {
                    continue;
                }
                let collection_name = deployment_dir.parent().unwrap().file_name().unwrap().to_str().unwrap();
                let deployment_name = deployment_dir.file_name().unwrap().to_str().unwrap();
                if deployment_name.contains(collection_name) == false {
                    if dry_run {
                        println!("Will rename {} to {}_{}", deployment_name, deployment_name, collection_name);
                    } else {
                        let mut deployment_id_dir = deployment_dir.clone();
                        deployment_id_dir.set_file_name(
                            format!("{}_{}", deployment_name, collection_name)
                        );
                        fs::rename(deployment_dir, deployment_id_dir).unwrap();
                    }
                }
            }
        }
    }
}
// fn digikam_tag_parser(tags: String) 
