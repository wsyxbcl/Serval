mod tags;
mod utils;

use clap::{Parser, Subcommand};
use std::path::PathBuf;
use tags::{
    extract_resources, get_classifications, get_temporal_independence, init_xmp, write_taglist,
};
use utils::{
    absolute_path, copy_xmp, deployments_align, deployments_rename, resources_align,
    tags_csv_translate, ExtractFilterType, ResourceType, TagType,
};

fn main() -> anyhow::Result<()> {
    let args = Cli::parse();

    match args.command {
        Commands::Align {
            path,
            output,
            project,
            deploy_table,
            type_resource,
            dryrun,
            move_mode,
        } => {
            if project {
                println!("Aligning deployments in {}", path.display());
                deployments_align(
                    absolute_path(path)?,
                    output,
                    deploy_table,
                    type_resource,
                    dryrun,
                    move_mode,
                )?;
            } else {
                println!("Aligning resources in {}", path.display());
                resources_align(
                    absolute_path(path)?,
                    output,
                    type_resource,
                    dryrun,
                    move_mode,
                )?;
            }
        }
        Commands::Observe {
            media_dir,
            output,
            xmp,
            mut subject,
            mut modified_time,
            video,
            image,
            independent,
            debug,
        } => {
            let resource_type = if xmp {
                utils::ResourceType::Xmp
            } else if video {
                if image {
                    utils::ResourceType::Media
                } else {
                    utils::ResourceType::Video
                }
            } else if image {
                utils::ResourceType::Image
            } else {
                utils::ResourceType::Media
            };
            if debug {
                subject = true;
                modified_time = true;
            }
            get_classifications(
                absolute_path(media_dir)?,
                output,
                resource_type,
                independent,
                subject,
                modified_time,
                debug,
            )?;
        }
        Commands::Rename {
            project_dir,
            dryrun,
        } => {
            deployments_rename(absolute_path(project_dir)?, dryrun)?;
        }
        Commands::Tags2img {
            taglist_path,
            image_path,
            tag_type,
        } => {
            write_taglist(
                absolute_path(taglist_path)?,
                absolute_path(image_path)?,
                tag_type,
            )?;
        }
        Commands::Capture { csv_path, output } => {
            get_temporal_independence(absolute_path(csv_path)?, output)?;
        }
        Commands::Extract {
            csv_path,
            value,
            filter_type,
            rename,
            output,
            use_subdir,
            subdir_type,
        } => {
            extract_resources(
                value,
                filter_type,
                rename,
                csv_path,
                output,
                use_subdir,
                subdir_type,
            )?;
        }
        Commands::Xmp {
            source_dir,
            output_dir,
            init,
        } => {
            if init {
                init_xmp(absolute_path(source_dir)?)?;
            } else {
                copy_xmp(absolute_path(source_dir)?, output_dir)?;
            }
        }
        Commands::Translate {
            csv_path,
            taglist_path,
            output,
        } => {
            println!("Translate tags in {}", csv_path.display());
            tags_csv_translate(
                absolute_path(csv_path)?,
                absolute_path(taglist_path)?,
                output,
            )?;
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
    /// Align resources in given Deployment or Project (recursively)
    #[command(arg_required_else_help = true)]
    Align {
        path: PathBuf,
        /// Directory for output(aligned) resources
        #[arg(short, long, value_name = "OUTPUT_DIR", required = true)]
        output: PathBuf,
        /// If the given path is a Project
        #[arg(short, long)]
        project: bool,
        /// Path for deployments table (deployments.csv)
        #[arg(short, long, value_name = "FILE", required = true)]
        deploy_table: PathBuf,
        /// Resource type
        #[arg(short, long, value_name = "TYPE", required = true, value_enum)]
        type_resource: ResourceType,
        /// Dry run
        #[arg(long)]
        dryrun: bool,
        /// Move mode (instead of copy)
        #[arg(short, long)]
        move_mode: bool,
    },
    /// Retrieve tags from media metadata
    #[command(arg_required_else_help = true)]
    Observe {
        media_dir: PathBuf,
        /// Output directory
        #[arg(
            short,
            long,
            value_name = "OUTPUT_DIR",
            default_value = "./serval_output/serval_observe"
        )]
        output: PathBuf,
        /// Read from XMP files
        #[arg(short, long)]
        xmp: bool,
        #[arg(short, long)]
        /// Include Subject metadata
        subject: bool,
        #[arg(short, long)]
        /// Include file modified time
        modified_time: bool,
        /// Video only
        #[arg(long)]
        video: bool,
        /// Image only
        #[arg(long)]
        image: bool,
        /// Debug mode
        #[arg(short, long)]
        debug: bool,
        /// Temporal independence analysis after retrieving
        #[arg(short, long)]
        independent: bool,
    },
    /// Rename a deployment directory from deployment_name to deployment_id
    #[command(arg_required_else_help = true)]
    Rename {
        project_dir: PathBuf,
        /// Dry run
        #[arg(long)]
        dryrun: bool,
    },
    /// Generate a (dummy) image file containing a list of tags
    #[command(arg_required_else_help = true)]
    Tags2img {
        /// Path for the taglist csv file
        taglist_path: PathBuf,
        /// Path for the dummy image
        image_path: PathBuf,
        /// Tag type: species or individual
        #[arg(short, long, value_name = "TYPE", required = true, value_enum)]
        tag_type: TagType,
    },
    /// Temporal independence analysis on a CSV file
    #[command(arg_required_else_help = true)]
    Capture {
        /// Path for tags.csv
        csv_path: PathBuf,
        /// Output directory
        #[arg(
            short,
            long,
            value_name = "OUTPUT_DIR",
            default_value = "./serval_output/serval_capture"
        )]
        output: PathBuf,
    },
    /// Extract and copy resources by filtering target values (based on tags.csv)
    #[command(arg_required_else_help = true)]
    Extract {
        /// Path for tags.csv
        csv_path: PathBuf,
        /// Specify the filter type
        #[arg(short, long, value_name = "FILTER", required = true, value_enum)]
        filter_type: ExtractFilterType,
        /// The target value (or substring for the path filter), use "ALL_VALUES" for all non-empty values
        #[arg(short, long, value_name = "VALUE", required = true)]
        value: String,
        /// Enable rename rename mode (including tags in filenames)
        #[arg(long)]
        rename: bool,
        /// Use subdirectories to organize resources
        #[arg(long, default_value_t = false)]
        use_subdir: bool,
        /// Specify the type used when creating subdirectories
        #[arg(long, default_value_t = ExtractFilterType::Species, value_enum)]
        subdir_type: ExtractFilterType,
        /// Set the output directory
        #[arg(
            short,
            long,
            value_name = "OUTPUT_DIR",
            default_value = "./serval_output/serval_extract"
        )]
        output: PathBuf,
    },
    /// Copy all XMP files to a directory while keeping the directory structure
    #[command(arg_required_else_help = true)]
    Xmp {
        /// Path for the source directory
        source_dir: PathBuf,
        /// Output directory
        #[arg(
            short,
            long,
            value_name = "OUTPUT_DIR",
            default_value = "./serval_output/serval_xmp"
        )]
        output_dir: PathBuf,
        /// Init mode (initialize xmp files)
        #[arg(short, long)]
        init: bool,
    },
    /// Translate tags in csv to different languages
    Translate {
        /// Path for tags.csv
        csv_path: PathBuf,
        /// Path for the taglist csv file
        #[arg(short, long, value_name = "TAGLIST", required = true)]
        taglist_path: PathBuf,
        /// Output directory
        #[arg(
            short,
            long,
            value_name = "OUTPUT_DIR",
            default_value = "./serval_output/serval_translate"
        )]
        output: PathBuf,
    },
}
