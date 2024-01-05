mod tags;
mod utils;

use clap::{Parser, Subcommand};
use std::path::PathBuf;
use tags::{extract_species, get_classifications, get_temporal_independence, write_taglist};
use utils::{deployments_align, deployments_rename, resources_align, absolute_path};

fn main() -> anyhow::Result<()> {
    let args = Cli::parse();

    match args.command {
        Commands::Align {
            path,
            output,
            project,
            deploy_table,
            dryrun,
            move_mode,
        } => {
            if project {
                println!("Aligning deployments in {}", path.display());
                deployments_align(
                    absolute_path(path)?,
                    output,
                    deploy_table,
                    dryrun,
                    move_mode,
                )?;
            } else {
                println!("Aligning resources in {}", path.display());
                resources_align(absolute_path(path)?, output, dryrun, move_mode)?;
            }
        }
        Commands::Observe {
            media_dir,
            output,
            parallel,
            xmp,
            independent,
        } => {
            if xmp {
                get_classifications(
                    absolute_path(media_dir)?,
                    output,
                    parallel,
                    utils::ResourceType::Xmp,
                    independent,
                )?;
            } else {
                // Image only currently
                get_classifications(
                    absolute_path(media_dir)?,
                    output,
                    parallel,
                    utils::ResourceType::Image,
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
        } => {
            write_taglist(absolute_path(taglist_path)?, image_path)?;
        }
        Commands::Capture { csv_path, output } => {
            get_temporal_independence(absolute_path(csv_path)?, output)?;
        }
        Commands::Extract {
            csv_path,
            species,
            output,
        } => {
            extract_species(species, csv_path, output)?;
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
        #[arg(short, long, value_name = "OUTPUT_DIR", required = true)]
        output: PathBuf,

        /// Parallel mode
        #[arg(short, long)]
        parallel: bool,

        /// Read from XMP
        #[arg(long)]
        xmp: bool,

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
    },
    /// Perform temporal independence analysis (on csv file)
    #[command(arg_required_else_help = true)]
    Capture {
        /// Path for tags.csv
        csv_path: PathBuf,
        /// Output directory
        #[arg(short, long, value_name = "OUTPUT_DIR", required = true)]
        output: PathBuf,
    },
    /// Extract and copy images containing target species (based on tags.csv)
    #[command(arg_required_else_help = true)]
    Extract {
        /// Path for tags.csv
        csv_path: PathBuf,
        /// Target species
        #[arg(long, value_name = "SPECIES", required = true)]
        species: String,
        /// Output directory
        #[arg(short, long, value_name = "OUTPUT_DIR", required = true)]
        output: PathBuf,
    },
}
