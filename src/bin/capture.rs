use chrono::NaiveDateTime;
use polars::prelude::*;

fn main() {
    let index = 6;
    let tag_ignored = Series::new("tag_to_ignore", ["", "Blank", "Useless data", "Human"]);
    let min_delta_time = 30;

    let df = CsvReader::from_path("./test/DQ_1026_tags.csv")
        .unwrap()
        .infer_schema(None)
        .has_header(true)
        .with_try_parse_dates(true)
        .finish()
        .unwrap();
    println!("{}", df);

    let df_cleaned = df
        .clone()
        .lazy()
        .select([
            col("path").str().split(lit("/")).list().get(lit(index)).alias("deployment"),
            col("filename"),
            col("datetime_original").alias("time"),
            col("species")])
        .drop_nulls(None)
        .filter(col("species").is_in(lit(tag_ignored)).not())
        .collect()
        .unwrap();
    println!("{}", df_cleaned);
    
    let mut df_sorted = df_cleaned
        .lazy()
        .sort("time", Default::default())
        .sort("species", Default::default())
        .sort("deployment", Default::default())
        // .with_columns([cols(["deployment", "time", "species"])
        //     .sort_by(["time"], [false])
        //     .over(["deployment", "species"])])
        .collect()
        .unwrap();

    df_sorted.as_single_chunk_par();
    let mut iters = df_sorted.columns(["time", "species", "deployment"]).unwrap()
        .iter().map(|s| s.iter()).collect::<Vec<_>>();

    // Row iterator as a temporary solution
    let mut capture = Vec::new();
    for _row in 0..df_sorted.height() {
        for iter in &mut iters {
            let value = iter.next().expect("should have as many iterations as rows");
            capture.push(value);
        }
    }
    let capture_time: Vec<&AnyValue<'_>> = capture.iter().step_by(3).collect();
    let capture_species: Vec<&AnyValue<'_>> = capture.iter().skip(1).step_by(3).collect();
    let capture_deployment: Vec<&AnyValue<'_>> = capture.iter().skip(2).step_by(3).collect();

    let mut capture_independence = Vec::new();
    let mut last_indep_time = capture_time[0].to_string();
    let mut last_indep_species = capture_species[0].to_string();
    let mut last_indep_deployment = capture_deployment[0].to_string();
    for i in 0..capture_time.len() {
        let time = capture_time[i].to_string();
        let species = capture_species[i].to_string();
        let deployment = capture_deployment[i].to_string();

        if i == 0 || species != last_indep_species || deployment != last_indep_deployment || is_temporal_independent(last_indep_time.clone(), time, min_delta_time){
            capture_independence.push(true);
            last_indep_time = capture_time[i].to_string();
            last_indep_species = capture_species[i].to_string();
            last_indep_deployment = capture_deployment[i].to_string();
        } else {
            capture_independence.push(false);
        }
    }
    
    let mut df_capture_independence = df_sorted
        .lazy()
        .filter(Series::new("independent", capture_independence).lit())
        .collect()
        .unwrap();

    println!("{}", df_capture_independence);

    let mut file = std::fs::File::create("./test/DQ_test.csv").unwrap();
    CsvWriter::new(&mut file).finish(&mut df_capture_independence).unwrap();
}

fn is_temporal_independent(time_ref: String, time: String, min_delta_time: i32) -> bool {
    // TODO Timezone
    let dt_ref = NaiveDateTime::parse_from_str(time_ref.as_str(), "%Y-%m-%d %H:%M:%S").unwrap();
    let dt = NaiveDateTime::parse_from_str(time.as_str(), "%Y-%m-%d %H:%M:%S").unwrap();
    let diff = dt - dt_ref;
    
    diff.num_minutes() > min_delta_time.into()
}