static USAGE: &str = r#"
Convert between various spatial formats and CSV including GeoJSON, SHP, and more.

For example to convert a GeoJSON file into CSV data:

qsv geoconvert file.geojson geojson csv

Usage:
    qsv geoconvert [options] [<input>] <input-format> <output-format>
    qsv geoconvert --help

Common options:
    -h, --help             Display this message
"#;

use std::path::PathBuf;

use geozero::{ProcessToCsv, ProcessToSvg, csv::CsvWriter, geojson::GeoJsonWriter};
use serde::Deserialize;

use crate::{CliResult, config::Config, util};

#[derive(Deserialize)]
struct Args {
    arg_input:         Option<String>,
    arg_input_format:  String,
    arg_output_format: String,
    // flag_output:   Option<String>,
}

pub fn run(argv: &[&str]) -> CliResult<()> {
    let args: Args = util::get_args(USAGE, argv)?;

    // TODO: Implement handling stdin
    let input = args.arg_input;
    let input_format = args.arg_input_format;
    let output_format = args.arg_output_format;

    // Construct a spatial geometry based on the input format
    let output_string = match input_format.as_str() {
        "geojson" => {
            let input_string = match input {
                Some(path) => std::fs::read_to_string(path).unwrap(),
                _ => return fail_clierror!("Could not identify file path."),
            };
            let mut geometry = geozero::geojson::GeoJson(&input_string);
            match output_format.as_str() {
                "csv" => geometry.to_csv().unwrap(),
                "svg" => geometry.to_svg().unwrap(),
                _ => return fail_clierror!("Could not identify valid output format."),
            }
        },
        "shp" => {
            let reader = geozero::shp::ShpReader::from_path(input.unwrap()).unwrap();
            match output_format.as_str() {
                "geojson" => {
                    let mut json: Vec<u8> = Vec::new();
                    reader
                        .iter_features(&mut GeoJsonWriter::new(&mut json))
                        .unwrap();
                    String::from_utf8(json).unwrap()
                },
                "csv" => {
                    let mut csv: Vec<u8> = Vec::new();
                    reader.iter_features(&mut CsvWriter::new(&mut csv)).unwrap();
                    String::from_utf8(csv).unwrap()
                },
                _ => return fail_clierror!("Could not identify valid output format."),
            }
        },
        _ => return fail_clierror!("Could not identify valid input format."),
    };

    print!("{output_string}");

    Ok(())
}
