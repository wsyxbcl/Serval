mod utils;
mod tags;

use std::path::PathBuf;
use clap::{Parser, Subcommand};
use utils::{
    deployments_align, resources_align, deployments_rename
};
use tags::{
    get_classifications, write_taglist
};

fn main() -> std::io::Result<()> {
    let args = Cli::parse();

    match args.command {
        Commands::Align { path, output, project,deploy_table, dryrun, move_mode} => {
            if project {
                println!("Aligning deployments in {}", path.display());
                deployments_align(path, output, deploy_table, dryrun, move_mode);
            } else {
                println!("Aligning resources in {}", path.display());
                resources_align(path, output, dryrun, move_mode);
            }
        }
        Commands::Observe { media_dir ,output, parallel} => {
            get_classifications(media_dir, output, parallel);
        }
        Commands::Rename { project_dir, dryrun} => {
            deployments_rename(project_dir, dryrun);
        }
        Commands::Tags2img { taglist_path, image_path } => {
            write_taglist(taglist_path, image_path).unwrap();
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
    }
}
