mod tags;
mod utils;
mod ocr;

use clap::{Parser, Subcommand};
use std::path::PathBuf;
use tags::{extract_resources, get_classifications, get_temporal_independence, write_taglist};
use utils::{
    absolute_path, copy_xmp, deployments_align, deployments_rename, resources_align,
    ExtractFilterType, ResourceType, TagType,
};
use ocr::{timmstamp_ocr, batch_ocr_csv};

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
            video,
            image,
            independent,
        } => {
            if xmp {
                get_classifications(
                    absolute_path(media_dir)?,
                    output,
                    utils::ResourceType::Xmp,
                    independent,
                )?;
            } else if video {
                if image {
                    get_classifications(
                        absolute_path(media_dir)?,
                        output,
                        utils::ResourceType::Media,
                        independent,
                    )?;
                } else {
                    get_classifications(
                        absolute_path(media_dir)?,
                        output,
                        utils::ResourceType::Video,
                        independent,
                    )?;
                }
            } else if image {
                get_classifications(
                    absolute_path(media_dir)?,
                    output,
                    utils::ResourceType::Image,
                    independent,
                )?;
            } else {
                get_classifications(
                    absolute_path(media_dir)?,
                    output,
                    utils::ResourceType::Media,
                    independent,
                )?;
            }
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
            write_taglist(absolute_path(taglist_path)?, image_path, tag_type)?;
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
        } => {
            extract_resources(value, filter_type, rename, csv_path, output)?;
        }
        Commands::Xmp {
            source_dir,
            output_dir,
        } => {
            copy_xmp(absolute_path(source_dir)?, output_dir)?;
        }
        Commands::Ocr { media_path, batch, output } => {
            if batch {
                batch_ocr_csv(media_path, output)?;
            } else {
                timmstamp_ocr(media_path, true)?;
            }
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
    /// Analyze media metadata
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
        /// Read from XMP
        #[arg(short, long)]
        xmp: bool,
        /// Video only
        #[arg(long)]
        video: bool,
        /// Image only
        #[arg(long)]
        image: bool,
        /// Temporal independence analysis after retrieving
        #[arg(short, long)]
        independent: bool,
    },
    /// Rename deployment directory name, from deployment_name to deployment_id
    #[command(arg_required_else_help = true)]
    Rename {
        project_dir: PathBuf,
        /// Dry run
        #[arg(long)]
        dryrun: bool,
    },
    /// Write taglist to a (dummy) image file
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
    /// Perform temporal independence analysis (on csv file)
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
    /// Extract and copy resources by filtering target value (based on tags.csv)
    #[command(arg_required_else_help = true)]
    Extract {
        /// Path for tags.csv
        csv_path: PathBuf,
        /// Filter on
        #[arg(short, long, value_name = "FILTER", required = true, value_enum)]
        filter_type: ExtractFilterType,
        /// Target value (or regex for the path)
        #[arg(short, long, value_name = "VALUE", required = true)]
        value: String,
        /// Rename mode (including tags in filenames)
        #[arg(long)]
        rename: bool,
        /// Output directory
        #[arg(
            short,
            long,
            value_name = "OUTPUT_DIR",
            default_value = "./serval_output/serval_extract"
        )]
        output: PathBuf,
    },
    /// Copy all xmp files to a directory while keeping the directory structure
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
    },
    /// Perform OCR on timestamped video
    #[command(arg_required_else_help = true)]
    Ocr {
        /// Path for the media file
        media_path: PathBuf,
        /// Batch mode
        #[arg(long)]
        batch: bool,
        /// Output directory
        #[arg(
            short,
            long,
            value_name = "OUTPUT_DIR",
            default_value = "./serval_output/serval_ocr"
        )]
        output: PathBuf,
    },
}
