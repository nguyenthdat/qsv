static USAGE: &str = r#"
Convert between various spatial formats and CSV/SVG including GeoJSON, SHP, and more.

For example to convert a GeoJSON file into CSV data:

qsv geoconvert file.geojson geojson csv

Usage:
    qsv geoconvert [options] (<input>) (<input-format>) (<output-format>)
    qsv geoconvert --help

geoconvert REQUIRED arguments:
    <input>           The spatial file to convert. Does not support stdin.
    <input-format>    Valid values are "geojson" and "shp"
    <output-format>   Valid values are:
                      - For GeoJSON input: "csv" and "svg"
                      - For SHP input: "csv" and "geojson"

Common options:
    -h, --help             Display this message
    -o, --output <file>    Write output to <file> instead of stdout.
"#;

use std::{
    fs::File,
    io::{self, BufWriter, Write},
    path::Path,
};

use geozero::{ProcessToCsv, ProcessToSvg, csv::CsvWriter, geojson::GeoJsonWriter};
use serde::Deserialize;

use crate::{CliError, CliResult, util};

/// Supported input formats for spatial data conversion
#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
enum InputFormat {
    Geojson,
    Shp,
}

/// Supported output formats for spatial data conversion
#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
enum OutputFormat {
    Csv,
    Svg,
    Geojson,
}

#[derive(Deserialize)]
struct Args {
    arg_input:         Option<String>,
    arg_input_format:  InputFormat,
    arg_output_format: OutputFormat,
    flag_output:       Option<String>,
}

impl From<geozero::error::GeozeroError> for CliError {
    fn from(err: geozero::error::GeozeroError) -> CliError {
        CliError::Other(format!("Geozero error: {err:?}"))
    }
}

impl From<geozero::shp::Error> for CliError {
    fn from(err: geozero::shp::Error) -> CliError {
        CliError::Other(format!("Geozero SHP error: {err:?}"))
    }
}

/// Validates that the input file exists and is readable
fn validate_input_file(path: &str) -> CliResult<()> {
    if !Path::new(path).exists() {
        return fail_clierror!("Input file '{}' does not exist", path);
    }
    Ok(())
}

pub fn run(argv: &[&str]) -> CliResult<()> {
    let args: Args = util::get_args(USAGE, argv)?;

    let input_path = args
        .arg_input
        .ok_or_else(|| CliError::Other("No input file specified".to_string()))?;

    validate_input_file(&input_path)?;

    // Create buffered writer for output
    let stdout = io::stdout();
    let mut wtr: Box<dyn Write> = if let Some(output_path) = args.flag_output {
        Box::new(BufWriter::new(File::create(output_path)?))
    } else {
        Box::new(BufWriter::new(stdout.lock()))
    };

    // Construct a spatial geometry based on the input format
    let output_string = match args.arg_input_format {
        InputFormat::Geojson => {
            let input_string = std::fs::read_to_string(&input_path)?;
            let mut geometry = geozero::geojson::GeoJson(&input_string);
            match args.arg_output_format {
                OutputFormat::Csv => geometry.to_csv()?,
                OutputFormat::Svg => geometry.to_svg()?,
                OutputFormat::Geojson => {
                    return fail_clierror!("Converting GeoJSON to GeoJSON is not supported");
                },
            }
        },
        InputFormat::Shp => {
            let reader = geozero::shp::ShpReader::from_path(&input_path)
                .map_err(|e| CliError::Other(format!("Failed to read SHP file: {e}")))?;
            match args.arg_output_format {
                OutputFormat::Geojson => {
                    let mut json: Vec<u8> = Vec::new();
                    reader.iter_features(&mut GeoJsonWriter::new(&mut json))?;
                    String::from_utf8(json)
                        .map_err(|e| CliError::Other(format!("Invalid UTF-8 in output: {e}")))?
                },
                OutputFormat::Csv => {
                    let mut csv: Vec<u8> = Vec::new();
                    reader.iter_features(&mut CsvWriter::new(&mut csv))?;
                    String::from_utf8(csv)
                        .map_err(|e| CliError::Other(format!("Invalid UTF-8 in output: {e}")))?
                },
                OutputFormat::Svg => {
                    return fail_clierror!("Converting SHP to SVG is not supported");
                },
            }
        },
    };

    wtr.write_all(output_string.as_bytes())?;
    Ok(wtr.flush()?)
}
