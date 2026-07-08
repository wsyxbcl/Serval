use anyhow::Result;
use std::env;

use serval::utils::copy_xmp;

fn main() -> Result<()> {
    let source_dir = env::current_dir()?;

    let mut output_dir = source_dir.clone();
    output_dir.push("xmp");

    copy_xmp(source_dir, output_dir)?;
    Ok(())
}
