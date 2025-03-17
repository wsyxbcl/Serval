use anyhow::Result;
use std::env;

use serval::{tags::get_classifications, utils::ResourceType};

fn main() -> Result<()> {
    let source_dir = env::current_dir()?;
    let _ = get_classifications(
        source_dir.clone(),
        source_dir,
        ResourceType::Xmp,
        false,
        false,
        false,
        false,
        true,
    );
    Ok(())
}
