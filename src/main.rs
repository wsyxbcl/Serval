mod schema;
mod tags;
mod transfer;
mod ui;
mod utils;

use clap::{Parser, Subcommand};
use std::path::PathBuf;
use tags::{
    CaptureOptions, CaptureTarget, ExtractOptions, MeasureFrom, WriteField, extract_resources,
    get_classifications, get_temporal_independence, init_xmp, update_datetime, update_tags,
    write_taglist, write_tags,
};
use transfer::OnConflict;
use utils::{
    AlignOptions, ExtractFilterType, ResourceType, SubdirType, TagType, XmpUpdateType,
    absolute_path, copy_xmp, deployments_align, deployments_rename, init_run_log, log_line,
    remove_xmp_files, resources_flatten, run_log_path, sync_xmp_directory, sync_xmp_from_csv,
    tags_csv_translate,
};

/// Exit codes: 0 success (possibly with warnings), 1 failed, 2 usage error (clap), 3 stopped for a decision
/// before writing anything (e.g. existing targets without --on-existing).
const EXIT_NEEDS_DECISION: i32 = 3;

fn main() -> anyhow::Result<()> {
    let args = Cli::parse();
    if args.progress == ProgressMode::Json {
        ui::enable_json(args.command.name());
    }

    let result = run(args.command);
    match &result {
        Ok(()) => log_line("Run completed"),
        Err(err) => log_line(&format!("Run failed: {err:#}")),
    }
    if let Some(log_path) = run_log_path() {
        println!("Log saved to {}", log_path.display());
    }
    ui::flush_progress();
    if let Err(err) = &result {
        ui::event(serde_json::json!({"serval": "error", "message": format!("{err:#}")}));
        if err.downcast_ref::<ui::NeedsDecision>().is_some() {
            eprintln!("Error: {err:#}");
            std::process::exit(EXIT_NEEDS_DECISION);
        }
    }
    result
}

#[derive(Clone, Copy, Debug, PartialEq, clap::ValueEnum)]
enum ProgressMode {
    /// Progress bars and plain lines for a person
    Terminal,
    /// JSON events on stderr, one per line, for a program such as Waxbill
    Json,
}

impl Commands {
    fn name(&self) -> &'static str {
        match self {
            Commands::Align { .. } => "align",
            Commands::Observe { .. } => "observe",
            Commands::Rename { .. } => "rename",
            Commands::Tags2img { .. } => "tags2img",
            Commands::Capture { .. } => "capture",
            Commands::Extract { .. } => "extract",
            Commands::Translate { .. } => "translate",
            Commands::Xmp(XmpCommands::Copy { .. }) => "xmp copy",
            Commands::Xmp(XmpCommands::Init { .. }) => "xmp init",
            Commands::Xmp(XmpCommands::Update { .. }) => "xmp update",
            Commands::Xmp(XmpCommands::Write { .. }) => "xmp write",
            Commands::Xmp(XmpCommands::Remove { .. }) => "xmp remove",
            Commands::Xmp(XmpCommands::Sync { .. }) => "xmp sync",
        }
    }
}

fn run(command: Commands) -> anyhow::Result<()> {
    match command {
        Commands::Align {
            path,
            output,
            deploy_table,
            type_resource,
            dryrun,
            move_mode,
            keep_first_subdir,
            on_existing,
        } => {
            if !dryrun {
                init_run_log("align", Some(&output));
            }
            let options = AlignOptions {
                resource_type: type_resource,
                dry_run: dryrun,
                move_mode,
                keep_first_subdir,
                on_existing,
            };
            if let Some(deploy_table) = deploy_table {
                println!("Aligning deployments in {}", path.display());
                deployments_align(absolute_path(path)?, output.clone(), deploy_table, &options)?;
            } else {
                println!("Flatten resources in {}", path.display());
                resources_flatten(absolute_path(path)?, output.clone(), &options)?;
            }
            if !dryrun {
                ui::output("folder", &output, None);
            }
        }
        Commands::Observe {
            media_dir,
            output,
            xmp,
            video,
            image,
            debug,
            deployment_level,
            id_species,
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
            get_classifications(
                absolute_path(media_dir)?,
                output,
                resource_type,
                debug,
                false,
                deployment_level,
                (!id_species.is_empty()).then_some(id_species),
            )?;
        }
        Commands::Rename {
            project_dir,
            dryrun,
        } => {
            if !dryrun {
                init_run_log("rename", None);
            }
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
        Commands::Capture {
            csv_path,
            output,
            event,
            no_exclude,
            camtrap_dp,
            min_gap,
            measure_from,
            by,
            deployment_level,
        } => {
            get_temporal_independence(
                absolute_path(csv_path)?,
                output,
                CaptureOptions {
                    event,
                    no_exclude,
                    camtrap_dp,
                    min_gap,
                    measure_from,
                    by,
                    deployment_level,
                },
            )?;
        }
        Commands::Extract {
            csv_path,
            value,
            filter_type,
            rename,
            skip_existing,
            output,
            use_subdir,
            subdir_type,
            keep_dirs,
            on_existing,
        } => {
            init_run_log("extract", Some(&output));
            extract_resources(
                csv_path,
                output,
                ExtractOptions {
                    filter_type,
                    filter_value: value,
                    rename,
                    skip_existing,
                    use_subdir,
                    subdir_type,
                    keep_dirs,
                    on_existing,
                },
            )?;
        }
        Commands::Xmp(xmp_cmd) => match xmp_cmd {
            XmpCommands::Copy {
                source_dir,
                output_dir,
                on_existing,
            } => {
                init_run_log("xmp_copy", Some(&output_dir));
                copy_xmp(absolute_path(source_dir)?, output_dir, on_existing)?;
            }
            XmpCommands::Init {
                source_dir,
                output,
                info,
            } => {
                init_run_log("xmp_init", None);
                if info {
                    println!("Note: --info is no longer needed, the table is always written.");
                }
                init_xmp(absolute_path(source_dir)?, output)?;
            }
            XmpCommands::Update {
                csv_path,
                tag_type,
                datetime,
            } => {
                init_run_log("xmp_update", None);
                if datetime {
                    update_datetime(absolute_path(csv_path)?)?;
                } else {
                    let tag_type =
                        tag_type.ok_or_else(|| anyhow::anyhow!("Tag type is required"))?;
                    update_tags(absolute_path(csv_path)?, tag_type)?;
                }
            }
            XmpCommands::Write {
                csv_path,
                fields,
                dry_run,
                create_missing,
            } => {
                if !dry_run {
                    init_run_log("xmp_write", None);
                }
                write_tags(absolute_path(csv_path)?, &fields, dry_run, create_missing)?;
            }
            XmpCommands::Remove { source_dir } => {
                init_run_log("xmp_remove", None);
                remove_xmp_files(absolute_path(source_dir)?)?;
            }
            XmpCommands::Sync { dir, csv } => {
                init_run_log("xmp_sync", None);
                if let Some(dir) = dir {
                    sync_xmp_directory(absolute_path(dir)?)?;
                } else if let Some(csv) = csv {
                    sync_xmp_from_csv(absolute_path(csv)?)?;
                } else {
                    return Err(anyhow::anyhow!(
                        "Either --csv or directory path must be specified"
                    ));
                }
            }
        },
        Commands::Translate {
            csv_path,
            taglist_path,
            output,
            from,
            to,
        } => {
            println!("Translate tags in {}", csv_path.display());
            tags_csv_translate(
                absolute_path(csv_path)?,
                absolute_path(taglist_path)?,
                output,
                &from,
                &to,
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
    /// How progress and results are reported
    #[arg(long, global = true, value_enum, default_value = "terminal")]
    progress: ProgressMode,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Align & clean resources in a Project (when deploy_table is provided) or flatten a directory
    #[command(arg_required_else_help = true)]
    Align {
        /// Path to the Project directory (align mode) or target directory (flatten mode)
        path: PathBuf,
        /// Directory for output (aligned) resources
        #[arg(short, long, value_name = "OUTPUT_DIR", required = true)]
        output: PathBuf,
        /// Path for deployments table (deployments.csv). If provided, align deployments, else flatten resources
        #[arg(short, long, value_name = "FILE")]
        deploy_table: Option<PathBuf>,
        /// Resource type
        #[arg(short, long, value_name = "TYPE", required = true, value_enum)]
        type_resource: ResourceType,
        /// Dry run
        #[arg(long)]
        dryrun: bool,
        /// Move mode (instead of copy)
        #[arg(short, long)]
        move_mode: bool,
        /// Keep the first subdirectory as an output folder (flatten mode)
        #[arg(long)]
        keep_first_subdir: bool,
        /// What to do with targets that already hold a different file (asked when not given)
        #[arg(long, value_enum, value_name = "ACTION")]
        on_existing: Option<OnConflict>,
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
        /// Video only
        #[arg(long)]
        video: bool,
        /// Image only
        #[arg(long)]
        image: bool,
        /// Debug mode: also write raw.csv with deployment (prompted) and
        /// media modified time, usable as input for `xmp update --datetime`
        #[arg(short, long)]
        debug: bool,
        /// Path level of the deployment for the debug table (asked when not given)
        #[arg(long, value_name = "N")]
        deployment_level: Option<i32>,
        /// Individually identified species: in images with several species and
        /// individual IDs, the IDs go to the one listed species (repeatable)
        #[arg(long, value_name = "NAME")]
        id_species: Vec<String>,
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
        /// Create event ID
        #[arg(long)]
        event: bool,
        /// Do not exclude default tags (Blank, Useless data, Unidentified, Unknown, Blur) from temporal independence analysis
        #[arg(long)]
        no_exclude: bool,
        /// Use observation table from camtrap-dp data package
        #[arg(long)]
        camtrap_dp: bool,
        /// Minimum time gap in minutes for two records to count as independent (asked when not given; default 30)
        #[arg(long, value_name = "MINUTES")]
        min_gap: Option<i32>,
        /// Measure the gap from the previous record or the previous independent record (asked when not given)
        #[arg(long, value_enum, value_name = "FROM")]
        measure_from: Option<MeasureFrom>,
        /// Analyze independence by species or individual ID (asked when not given)
        #[arg(long, value_enum, value_name = "TARGET")]
        by: Option<CaptureTarget>,
        /// Path level of the deployment (asked when not given; detected without a terminal)
        #[arg(long, value_name = "N")]
        deployment_level: Option<i32>,
        // TODO custom exclude tags
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
    #[command(
        long_about = "Extract and copy resources by filtering target values (based on tags.csv)\n\n\
    # Basic Filtering\n\
    Use simple filter types for single-field queries:\n\
    serval extract tags.csv -f species -v \"Snow leopard\"\n\
    serval extract tags.csv -f rating -v \"4-5\"\n\n\
    # Advanced Filtering\n\
    Use `-f advanced` for complex multi-field queries with logical operators:\n\n\
    Same Species AND (images with BOTH species):\n\
    -f advanced -v \"species:Blue sheep and species:Snow leopard\"\n\n\
    AND conditions:\n\
    -f advanced -v \"species:Serval and rating:4-5\"\n\n\
    OR conditions:\n\
    -f advanced -v \"species:Serval or species:White-lipped deer\"\n\n\
    Complex combinations:\n\
    -f advanced -v \"(species:Serval and rating:4-5) or (species:Snow leopard and rating:5)\"\n\n\
    # Field Aliases\n\
    species: sp, s  |  individual: ind, i  |  rating: rate, r\n\
    path: p  |  event: e  |  custom: c\n\n\
    # Quoting\n\
    Quote values that contain \" and \", \" or \" or a parenthesis:\n\
    -f advanced -v \"species:'Black and white colobus' or species:Fox\"\n\n\
    # Operators\n\
    Exact match:     species:Fox\n\
    Range:           rating:3-5\n\
    Comparisons:     rating:>=4, rating:>4, rating:<5, rating:<=5"
    )]
    Extract {
        /// Path for tags.csv
        csv_path: PathBuf,
        /// Specify the filter type
        #[arg(short, long, value_name = "FILTER", required = true, value_enum)]
        filter_type: ExtractFilterType,
        /// The target value (or substring for the path filter), use "ALL_VALUES" for all non-empty values
        #[arg(short, long, value_name = "VALUE", required = true)]
        value: String,
        /// Name copies after all tags of the image:
        /// {species}__{individuals}__{original name}, values joined by "+"
        /// (e.g. Fox+Pika__F03__IMG_0001.JPG)
        #[arg(long)]
        rename: bool,
        /// Skip targets that already hold a different file, without asking.
        /// Kept for old commands: finished copies are now recognized and skipped
        /// anyway, so an interrupted extract can simply be rerun.
        #[arg(long, default_value_t = false, hide = true)]
        skip_existing: bool,
        /// Use subdirectories to organize resources
        #[arg(long, default_value_t = false)]
        use_subdir: bool,
        /// Specify the type used when creating subdirectories
        #[arg(long, default_value_t = SubdirType::Species, value_enum)]
        subdir_type: SubdirType,
        /// Folders kept above each file in the output, 0 = file only (asked when not given)
        #[arg(long, value_name = "N")]
        keep_dirs: Option<usize>,
        /// What to do with targets that already hold a different file (asked when not given)
        #[arg(long, value_enum, value_name = "ACTION")]
        on_existing: Option<OnConflict>,
        /// Set the output directory
        #[arg(
            short,
            long,
            value_name = "OUTPUT_DIR",
            default_value = "./serval_output/serval_extract"
        )]
        output: PathBuf,
    },
    /// XMP file operations
    #[command(subcommand)]
    Xmp(XmpCommands),
    /// Translate species column in csv according to taglist
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
        /// Column name (in taglist) to translate from
        #[arg(long, value_name = "FROM", required = true)]
        from: String,
        /// Column name (in taglist) to translate to
        #[arg(long, value_name = "TO", required = true)]
        to: String,
    },
}

#[derive(Debug, Subcommand)]
enum XmpCommands {
    /// Copy XMP files to output directory
    Copy {
        source_dir: PathBuf,
        output_dir: PathBuf,
        /// What to do with targets that already hold a different file (asked when not given)
        #[arg(long, value_enum, value_name = "ACTION")]
        on_existing: Option<OnConflict>,
    },
    /// Initialize XMP files for media files, and write a table of every media
    /// file's datetime and GPS (for review in Caracal, and as input for
    /// `xmp update --datetime`). Existing XMP files are not changed.
    Init {
        source_dir: PathBuf,
        /// Output directory for the table
        #[arg(
            short,
            long,
            value_name = "OUTPUT_DIR",
            default_value = "./serval_output/serval_init"
        )]
        output: PathBuf,
        /// No longer needed: the table is always written
        #[arg(short, long, hide = true)]
        info: bool,
    },
    /// Update XMP files from CSV.
    /// Tag mode uses: `xmp_update`, plus `species`, `individual`, or `rating` according to `--tag-type`.
    /// Datetime mode (`--datetime`) uses: `xmp_update_datetime` (format: yyyy-MM-dd HH:mm:ss).
    Update {
        csv_path: PathBuf,
        /// Tag type for tag mode (`species`, `individual`, or `rating`).
        #[arg(short, long, value_name = "TYPE", required_unless_present = "datetime")]
        tag_type: Option<XmpUpdateType>,
        /// Use datetime mode (reads `xmp_update_datetime` instead of xmp_update).
        #[arg(long)]
        datetime: bool,
    },
    /// Write the labels in a table into the XMP files as they are: species, individual,
    /// rating and datetime in one pass. For each file, each written field becomes exactly
    /// the table's value (species and individuals from all the file's rows); empty cells
    /// leave the field unchanged. Use `xmp update` to change old values into new ones.
    Write {
        csv_path: PathBuf,
        /// Only these fields (comma-separated; default: all four)
        #[arg(long, value_enum, value_delimiter = ',', value_name = "FIELDS")]
        fields: Vec<WriteField>,
        /// Check every file and report what would change, without writing
        #[arg(long)]
        dry_run: bool,
        /// Create missing sidecars from the media files (as `xmp init` does)
        #[arg(long)]
        create_missing: bool,
    },
    /// Remove all XMP files recursively from a directory
    Remove { source_dir: PathBuf },
    /// Sync XMP metadata to corresponding media files
    Sync {
        /// Directory containing XMP files to sync
        #[arg(value_name = "DIR")]
        dir: Option<PathBuf>,
        /// CSV file with paths to XMP files to sync
        #[arg(long, value_name = "CSV_PATH")]
        csv: Option<PathBuf>,
    },
}
