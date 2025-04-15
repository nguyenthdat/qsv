static USAGE: &str = r#"
Convert between various spatial formats and CSV/SVG including GeoJSON, SHP, and more.

For example to convert a GeoJSON file into CSV data:

qsv geoconvert file.geojson geojson csv

To convert a CSV file into GeoJSON data, specify the WKT geometry column with the --geometry flag:

qsv geoconvert file.csv csv geojson --geometry geometry

Usage:
    qsv geoconvert [options] (<input>) (<input-format>) (<output-format>)
    qsv geoconvert --help

geoconvert REQUIRED arguments:
    <input>           The spatial file to convert. Does not support stdin.
    <input-format>    Valid values are "geojson", "shp", and "csv"
    <output-format>   Valid values are:
                      - For GeoJSON input: "csv", "svg", and "geojsonl"
                      - For SHP input: "csv", "geojson", and "geojsonl"
                      - For CSV input: "geojson", "geojsonl", and "svg"

geoconvert options:
                                 REQUIRED FOR CSV INPUT:
    -g, --geometry <geometry>    The name of the column that has WKT geometry.

Common options:
    -h, --help                   Display this message
    -o, --output <file>          Write output to <file> instead of stdout.
"#;

use std::{
    fs::File,
    io::{self, BufReader, BufWriter, Write},
    path::Path,
};

use geozero::{
    GeozeroDatasource,
    csv::CsvWriter,
    geojson::{GeoJsonLineWriter, GeoJsonWriter},
    svg::SvgWriter,
};
use serde::Deserialize;

use crate::{CliError, CliResult, util};

/// Supported input formats for spatial data conversion
#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
enum InputFormat {
    Geojson,
    // Geojsonl,
    Shp,
    Csv,
}

/// Supported output formats for spatial data conversion
#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
enum OutputFormat {
    Csv,
    Svg,
    Geojson,
    Geojsonl,
}

#[derive(Deserialize)]
struct Args {
    arg_input:         Option<String>,
    arg_input_format:  InputFormat,
    arg_output_format: OutputFormat,
    flag_geometry:     Option<String>,
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
    let mut buf_reader = BufReader::new(File::open(&input_path)?);
    // let mut buf_reader = if let Some(input_path) = args.arg_input {
    //     if input_path == "-"  {
    //         BufReader::new(std::io::stdin())
    //     }
    // } else {
    //     BufReader::new(std::io::stdin())
    // };
    // Construct a spatial geometry based on the input format
    match args.arg_input_format {
        InputFormat::Geojson => {
            let mut geometry = geozero::geojson::GeoJsonReader(&mut buf_reader);
            match args.arg_output_format {
                OutputFormat::Csv => {
                    let mut processor = CsvWriter::new(&mut wtr);
                    geometry.process(&mut processor)?;
                },
                OutputFormat::Svg => {
                    let mut processor = SvgWriter::new(&mut wtr, false);
                    geometry.process(&mut processor)?;
                },
                OutputFormat::Geojsonl => {
                    let mut processor = GeoJsonLineWriter::new(&mut wtr);
                    geometry.process(&mut processor)?;
                },
                OutputFormat::Geojson => {
                    return fail_clierror!("Converting GeoJSON to GeoJSON is not supported");
                },
            }
        },
        // InputFormat::Geojsonl => {
        //     let mut geometry = geozero::geojson::GeoJsonLineReader::new(&mut buf_reader);
        //     match args.arg_output_format {
        //         OutputFormat::Csv => {
        //             let mut processor = CsvWriter::new(&mut wtr);
        //             geometry.process(&mut processor)?
        //         },
        //         OutputFormat::Svg => {
        //             let mut processor = SvgWriter::new(&mut wtr, false);
        //             geometry.process(&mut processor)?
        //         },
        //         OutputFormat::Geojson => {
        //             let mut processor = GeoJsonWriter::new(&mut wtr);
        //             geometry.process(&mut processor)?
        //         },
        //         OutputFormat::Geojsonl => {
        //             return fail_clierror!("Converting GeoJSON Lines to GeoJSON Lines is not
        // supported");         }
        //     };
        // },
        InputFormat::Shp => {
            let mut reader = geozero::shp::ShpReader::new(&mut buf_reader)?;
            let mut input_reader = BufReader::new(File::open(input_path.replace(".shp", ".shx"))?);
            let mut dbf_reader = BufReader::new(File::open(input_path.replace(".shp", ".dbf"))?);
            reader.add_index_source(&mut input_reader)?;
            reader.add_dbf_source(&mut dbf_reader)?;
            let output_string = match args.arg_output_format {
                OutputFormat::Geojson => {
                    let mut json: Vec<u8> = Vec::new();
                    let _ = reader
                        .iter_features(&mut GeoJsonWriter::new(&mut json))?
                        .collect::<Vec<_>>();
                    String::from_utf8(json)
                        .map_err(|e| CliError::Other(format!("Invalid UTF-8 in output: {e}")))?
                },
                OutputFormat::Geojsonl => {
                    let mut json: Vec<u8> = Vec::new();
                    let _ = reader
                        .iter_features(&mut GeoJsonLineWriter::new(&mut json))?
                        .collect::<Vec<_>>();
                    String::from_utf8(json)
                        .map_err(|e| CliError::Other(format!("Invalid UTF-8 in output: {e}")))?
                },
                OutputFormat::Csv => {
                    let mut csv: Vec<u8> = Vec::new();
                    let _ = reader
                        .iter_features(&mut CsvWriter::new(&mut csv))?
                        .collect::<Vec<_>>();
                    String::from_utf8(csv)
                        .map_err(|e| CliError::Other(format!("Invalid UTF-8 in output: {e}")))?
                },
                OutputFormat::Svg => {
                    return fail_clierror!("Converting SHP to SVG is not supported");
                },
            };
            wtr.write_all(output_string.as_bytes())?;
        },
        InputFormat::Csv => {
            if let Some(geometry_col) = args.flag_geometry {
                let mut csv = geozero::csv::CsvReader::new(&geometry_col, buf_reader);
                match args.arg_output_format {
                    OutputFormat::Geojson => {
                        let mut processor = GeoJsonWriter::new(&mut wtr);
                        csv.process(&mut processor)?;
                    },
                    OutputFormat::Geojsonl => {
                        let mut processor = GeoJsonLineWriter::new(&mut wtr);
                        csv.process(&mut processor)?;
                    },
                    OutputFormat::Svg => {
                        let mut processor = SvgWriter::new(&mut wtr, false);
                        csv.process(&mut processor)?;
                    },
                    OutputFormat::Csv => {
                        return fail_clierror!("Converting CSV to CSV is not supported");
                    },
                }
            } else {
                return fail_clierror!(
                    "Please specify a geometry column with the --geometry option"
                );
            }
        },
    }

    // wtr.write_all(output_string.as_bytes())?;
    Ok(wtr.flush()?)
}
