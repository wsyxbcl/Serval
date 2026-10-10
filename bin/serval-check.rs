use anyhow::Result;
use std::env;

use serval::{tags::get_classifications, utils::ResourceType};

fn main() -> Result<()> {
    let source_dir = env::current_dir()?;
    get_classifications(
        source_dir.clone(),
        source_dir,
        ResourceType::Xmp,
        false,
        true,
        None,
        None,
    )?;
    Ok(())
}
