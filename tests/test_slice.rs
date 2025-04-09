use std::{borrow::ToOwned, process};

use crate::workdir::Workdir;

macro_rules! slice_tests {
    ($name:ident, $start:expr_2021, $end:expr_2021, $expected:expr_2021) => {
        mod $name {
            use super::test_slice;

            #[test]
            fn headers_no_index() {
                let name = concat!(stringify!($name), "headers_no_index");
                test_slice(name, $start, $end, $expected, true, false, false, false);
            }

            #[test]
            fn no_headers_no_index() {
                let name = concat!(stringify!($name), "no_headers_no_index");
                test_slice(name, $start, $end, $expected, false, false, false, false);
            }

            #[test]
            fn no_headers_no_index_json() {
                let name = concat!(stringify!($name), "no_headers_no_index_json");
                test_slice(name, $start, $end, $expected, false, false, false, true);
            }

            #[test]
            fn headers_index() {
                let name = concat!(stringify!($name), "headers_index");
                test_slice(name, $start, $end, $expected, true, true, false, false);
            }

            #[test]
            fn no_headers_index() {
                let name = concat!(stringify!($name), "no_headers_index");
                test_slice(name, $start, $end, $expected, false, true, false, false);
            }

            #[test]
            fn headers_index_json() {
                let name = concat!(stringify!($name), "headers_index_json");
                test_slice(name, $start, $end, $expected, true, true, false, true);
            }

            #[test]
            fn no_headers_index_json() {
                let name = concat!(stringify!($name), "no_headers_index_json");
                test_slice(name, $start, $end, $expected, false, true, false, true);
            }

            #[test]
            fn headers_no_index_len() {
                let name = concat!(stringify!($name), "headers_no_index_len");
                test_slice(name, $start, $end, $expected, true, false, true, false);
            }

            #[test]
            fn no_headers_no_index_len() {
                let name = concat!(stringify!($name), "no_headers_no_index_len");
                test_slice(name, $start, $end, $expected, false, false, true, false);
            }

            #[test]
            fn headers_no_index_len_json() {
                let name = concat!(stringify!($name), "headers_no_index_len_json");
                test_slice(name, $start, $end, $expected, true, false, true, true);
            }

            #[test]
            fn no_headers_no_index_len_json() {
                let name = concat!(stringify!($name), "no_headers_no_index_len_json");
                test_slice(name, $start, $end, $expected, false, false, true, true);
            }

            #[test]
            fn headers_index_len() {
                let name = concat!(stringify!($name), "headers_index_len");
                test_slice(name, $start, $end, $expected, true, true, true, false);
            }

            #[test]
            fn no_headers_index_len() {
                let name = concat!(stringify!($name), "no_headers_index_len");
                test_slice(name, $start, $end, $expected, false, true, true, false);
            }

            #[test]
            fn headers_index_len_json() {
                let name = concat!(stringify!($name), "headers_index_len_json");
                test_slice(name, $start, $end, $expected, true, true, true, true);
            }

            #[test]
            fn no_headers_index_len_json() {
                let name = concat!(stringify!($name), "no_headers_index_len_json");
                test_slice(name, $start, $end, $expected, false, true, true, true);
            }
        }
    };
}

fn setup(name: &str, headers: bool, use_index: bool) -> (Workdir, process::Command) {
    let wrk = Workdir::new(name);
    let mut data = vec![svec!["a"], svec!["b"], svec!["c"], svec!["d"], svec!["e"]];
    if headers {
        data.insert(0, svec!["header"]);
    }
    if use_index {
        wrk.create_indexed("in.csv", data);
    } else {
        wrk.create("in.csv", data);
    }

    let mut cmd = wrk.command("slice");
    cmd.arg("in.csv");

    (wrk, cmd)
}

fn test_slice(
    name: &str,
    start: Option<isize>,
    end: Option<usize>,
    expected: &[&str],
    headers: bool,
    use_index: bool,
    as_len: bool,
    json_output: bool,
) {
    let (wrk, mut cmd) = setup(name, headers, use_index);
    if let Some(start) = start {
        cmd.arg("--start").arg(&start.to_string());
    }
    if let Some(end) = end {
        if as_len {
            let start = start.unwrap_or(0);
            if start < 0 {
                cmd.arg("--len").arg(&end.to_string());
            } else {
                cmd.arg("--len")
                    .arg(&(end - start.unsigned_abs()).to_string());
            }
        } else {
            cmd.arg("--end").arg(&end.to_string());
        }
    }
    if !headers {
        cmd.arg("--no-headers");
    }
    if json_output {
        let output_file = wrk.path("output.json").to_string_lossy().to_string();

        cmd.arg("--json").args(&["--output", &output_file]);

        wrk.assert_success(&mut cmd);

        let gots = wrk.read_to_string(&output_file).unwrap();
        let gotj: serde_json::Value = serde_json::from_str(&gots).unwrap();
        let got = gotj.to_string();

        let expected_vec = expected
            .iter()
            .map(|&s| {
                if headers {
                    format!("{{\"header\":\"{}\"}}", s)
                } else {
                    format!("{{\"0\":\"{}\"}}", s)
                }
            })
            .collect::<Vec<String>>();
        let expected = format!("[{}]", expected_vec.join(","));

        similar_asserts::assert_eq!(got, expected);
    } else {
        let got: Vec<Vec<String>> = wrk.read_stdout(&mut cmd);
        let mut expected = expected
            .iter()
            .map(|&s| vec![s.to_owned()])
            .collect::<Vec<Vec<String>>>();
        if headers {
            expected.insert(0, svec!["header"]);
        }
        similar_asserts::assert_eq!(got, expected);
    }
}

fn test_index(name: &str, idx: isize, expected: &str, headers: bool, use_index: bool) {
    let (wrk, mut cmd) = setup(name, headers, use_index);
    cmd.arg("--index").arg(&idx.to_string());
    if !headers {
        cmd.arg("--no-headers");
    }

    let got: Vec<Vec<String>> = wrk.read_stdout(&mut cmd);
    let mut expected = vec![vec![expected.to_owned()]];
    if headers {
        expected.insert(0, svec!["header"]);
    }
    similar_asserts::assert_eq!(got, expected);
}

slice_tests!(slice_simple, Some(0), Some(1), &["a"]);
slice_tests!(slice_simple_2, Some(1), Some(3), &["b", "c"]);
slice_tests!(slice_no_start, None, Some(1), &["a"]);
slice_tests!(slice_no_end, Some(3), None, &["d", "e"]);
slice_tests!(slice_all, None, None, &["a", "b", "c", "d", "e"]);
slice_tests!(slice_negative_start, Some(-2), None, &["d", "e"]);

#[test]
fn slice_negative_with_len() {
    test_slice(
        "slice_negative_start_headers_index_len",
        Some(-4),
        Some(2),
        &["b", "c"],
        true,
        true,
        true,
        false,
    );
    test_slice(
        "slice_negative_start_no_headers_index_len",
        Some(-4),
        Some(2),
        &["b", "c"],
        false,
        true,
        true,
        false,
    );
    test_slice(
        "slice_negative_start_headers_no_index_len",
        Some(-4),
        Some(2),
        &["b", "c"],
        true,
        false,
        true,
        false,
    );
}

#[test]
fn slice_negative_with_len_json() {
    test_slice(
        "slice_negative_start_headers_index_len_json",
        Some(-4),
        Some(2),
        &["b", "c"],
        true,
        true,
        true,
        true,
    );
    test_slice(
        "slice_negative_start_no_headers_index_len_json",
        Some(-4),
        Some(2),
        &["b", "c"],
        false,
        true,
        true,
        true,
    );
    test_slice(
        "slice_negative_start_headers_no_index_len_json",
        Some(-4),
        Some(2),
        &["b", "c"],
        true,
        false,
        true,
        true,
    );
}

#[test]
fn slice_index() {
    test_index("slice_index", 1, "b", true, false);
}
#[test]
fn slice_index_no_headers() {
    test_index("slice_index_no_headers", 1, "b", false, false);
}
#[test]
fn slice_index_withindex() {
    test_index("slice_index_withindex", 1, "b", true, true);
}
#[test]
fn slice_index_no_headers_withindex() {
    test_index("slice_index_no_headers_withindex", 1, "b", false, true);
}

#[test]
fn slice_neg_index() {
    test_index("slice_neg_index", -1, "e", true, false);
}
#[test]
fn slice_neg_index_no_headers() {
    test_index("slice_neg_index_no_headers", -1, "e", false, false);
}
#[test]
fn slice_neg_index_withindex() {
    test_index("slice_neg_index_withindex", -2, "d", true, true);
}
#[test]
fn slice_neg_index_no_headers_withindex() {
    test_index("slice_neg_index_no_headers_withindex", -2, "d", false, true);
}

fn test_slice_invert(
    name: &str,
    start: Option<isize>,
    end: Option<usize>,
    expected: &[&str],
    headers: bool,
    use_index: bool,
    as_len: bool,
    json_output: bool,
) {
    let (wrk, mut cmd) = setup(name, headers, use_index);
    if let Some(start) = start {
        cmd.arg("--start").arg(&start.to_string());
    }
    if let Some(end) = end {
        if as_len {
            let start = start.unwrap_or(0);
            if start < 0 {
                cmd.arg("--len").arg(&end.to_string());
            } else {
                cmd.arg("--len")
                    .arg(&(end - start.unsigned_abs()).to_string());
            }
        } else {
            cmd.arg("--end").arg(&end.to_string());
        }
    }
    if !headers {
        cmd.arg("--no-headers");
    }
    cmd.arg("--invert");

    if json_output {
        let output_file = wrk.path("output.json").to_string_lossy().to_string();

        cmd.arg("--json").args(&["--output", &output_file]);

        wrk.assert_success(&mut cmd);

        let gots = wrk.read_to_string(&output_file).unwrap();
        let gotj: serde_json::Value = serde_json::from_str(&gots).unwrap();
        let got = gotj.to_string();

        let expected_vec = expected
            .iter()
            .map(|&s| {
                if headers {
                    format!("{{\"header\":\"{}\"}}", s)
                } else {
                    format!("{{\"0\":\"{}\"}}", s)
                }
            })
            .collect::<Vec<String>>();
        let expected = format!("[{}]", expected_vec.join(","));

        similar_asserts::assert_eq!(got, expected);
    } else {
        let got: Vec<Vec<String>> = wrk.read_stdout(&mut cmd);
        let mut expected = expected
            .iter()
            .map(|&s| vec![s.to_owned()])
            .collect::<Vec<Vec<String>>>();
        if headers {
            expected.insert(0, svec!["header"]);
        }
        similar_asserts::assert_eq!(got, expected);
    }
}

#[test]
fn slice_invert_simple() {
    test_slice_invert(
        "slice_invert_simple",
        Some(0),
        Some(1),
        &["b", "c", "d", "e"],
        true,
        false,
        false,
        false,
    );
}

#[test]
fn slice_invert_middle() {
    test_slice_invert(
        "slice_invert_middle",
        Some(1),
        Some(3),
        &["a", "d", "e"],
        true,
        false,
        false,
        false,
    );
}

#[test]
fn slice_invert_with_index() {
    test_slice_invert(
        "slice_invert_with_index",
        Some(1),
        Some(3),
        &["a", "d", "e"],
        true,
        true,
        false,
        false,
    );
}

#[test]
fn slice_invert_json() {
    test_slice_invert(
        "slice_invert_json",
        Some(1),
        Some(3),
        &["a", "d", "e"],
        true,
        false,
        false,
        true,
    );
}

#[test]
fn slice_invert_negative() {
    test_slice_invert(
        "slice_invert_negative",
        Some(-2),
        None,
        &["a", "b", "c"],
        true,
        false,
        false,
        false,
    );
}

#[test]
fn slice_invert_with_len() {
    test_slice_invert(
        "slice_invert_with_len",
        Some(1),
        Some(2),
        &["a", "c", "d", "e"],
        true,
        false,
        true,
        false,
    );
}

#[test]
#[cfg(feature = "polars")]
fn slice_from_parquet() {
    let wrk = Workdir::new("slice_from_parquet");
    let test_file = wrk.load_test_file("NYC311-5.parquet");
    let mut cmd = wrk.command("slice");
    cmd.arg(test_file).arg("--index").arg("2").arg("--json");

    wrk.assert_success(&mut cmd);

    let got: String = wrk.stdout(&mut cmd);
    let expected = r#"[{"Unique Key":"20520945","Created Date":"05/27/2011 12:00:00 AM","Closed Date":null,"Agency":"HPD","Agency Name":"Department of Housing Preservation and Development","Complaint Type":"PAINT - PLASTER","Descriptor":"WALLS","Location Type":"RESIDENTIAL BUILDING","Incident Zip":"11225","Incident Address":"1700 BEDFORD AVENUE","Street Name":"BEDFORD AVENUE","Cross Street 1":"MONTGOMERY STREET","Cross Street 2":"SULLIVAN PLACE","Intersection Street 1":null,"Intersection Street 2":null,"Address Type":"ADDRESS","City":"BROOKLYN","Landmark":null,"Facility Type":"N/A","Status":"Open","Due Date":null,"Resolution Description":"The following complaint conditions are still open.HPD may attempt to contact you to verify the correction of the condition or may conduct an inspection.","Resolution Action Updated Date":"06/15/2011 12:00:00 AM","Community Board":"09 BROOKLYN","BBL":"3013020001","Borough":"BROOKLYN","X Coordinate (State Plane)":"996197","Y Coordinate (State Plane)":"181752","Open Data Channel Type":"UNKNOWN","Park Facility Name":"Unspecified","Park Borough":"BROOKLYN","Vehicle Type":null,"Taxi Company Borough":null,"Taxi Pick Up Location":null,"Bridge Highway Name":null,"Bridge Highway Direction":null,"Road Ramp":null,"Bridge Highway Segment":null,"Latitude":null,"Longitude":null,"Location":null}]"#;
    similar_asserts::assert_eq!(got, expected);
}

#[cfg(feature = "polars")]
#[test]
fn slice_from_json() {
    let wrk = Workdir::new("slice_from_json");
    let test_file = wrk.load_test_file("NYC311-5.json");
    let mut cmd = wrk.command("slice");
    cmd.arg(test_file).arg("--index").arg("2").arg("--json");

    wrk.assert_success(&mut cmd);

    let got: String = wrk.stdout(&mut cmd);
    let expected = r#""Unique Key":"20520945""#;
    assert!(got.contains(expected));
}

#[cfg(feature = "polars")]
#[test]
fn slice_from_jsonl() {
    let wrk = Workdir::new("slice_from_jsonl");
    let test_file = wrk.load_test_file("NYC311-5.jsonl");
    let mut cmd = wrk.command("slice");
    cmd.arg(test_file).arg("--index").arg("2").arg("--json");

    wrk.assert_success(&mut cmd);

    let got: String = wrk.stdout(&mut cmd);
    let expected = r#""Unique Key":"20520945""#;
    assert!(got.contains(expected));
}

#[cfg(feature = "polars")]
#[test]
fn slice_from_avro() {
    let wrk = Workdir::new("slice_from_avro");
    let test_file = wrk.load_test_file("NYC311-5.avro");
    let mut cmd = wrk.command("slice");
    cmd.arg(test_file).arg("--index").arg("2");

    wrk.assert_success(&mut cmd);

    let got: String = wrk.stdout(&mut cmd);
    let expected = "03/30/2019 04:06:23 AM";
    assert!(got.contains(expected));
}

#[cfg(feature = "polars")]
#[test]
fn slice_from_arrow() {
    let wrk = Workdir::new("slice_from_arrow");
    let test_file = wrk.load_test_file("NYC311-5.arrow");
    let mut cmd = wrk.command("slice");
    cmd.arg(test_file).arg("--index").arg("2").arg("--json");

    wrk.assert_success(&mut cmd);

    let got: String = wrk.stdout(&mut cmd);
    let expected = r#""Unique Key":"20520945""#;
    assert!(got.contains(expected));
}

#[cfg(feature = "polars")]
#[test]
fn slice_from_csvgz() {
    let wrk = Workdir::new("slice_from_csvgz");
    let test_file = wrk.load_test_file("NYC311-5.csv.gz");
    let mut cmd = wrk.command("slice");
    cmd.arg(test_file).arg("--index").arg("2").arg("--json");

    wrk.assert_success(&mut cmd);

    let got: String = wrk.stdout(&mut cmd);
    let expected = "20520945";
    assert!(got.contains(expected));
}

#[cfg(feature = "polars")]
#[test]
fn slice_from_csvzst() {
    let wrk = Workdir::new("slice_from_csvzst");
    let test_file = wrk.load_test_file("NYC311-5.csv.zst");
    let mut cmd = wrk.command("slice");
    cmd.arg(test_file).arg("--index").arg("2").arg("--json");

    wrk.assert_success(&mut cmd);

    let got: String = wrk.stdout(&mut cmd);
    let expected = "20520945";
    assert!(got.contains(expected));
}

#[cfg(feature = "polars")]
#[test]
fn slice_from_csvzlib() {
    let wrk = Workdir::new("slice_from_csvzlib");
    let test_file = wrk.load_test_file("NYC311-5.csv.zlib");
    let mut cmd = wrk.command("slice");
    cmd.arg(test_file).arg("--index").arg("2").arg("--json");

    wrk.assert_success(&mut cmd);

    let got: String = wrk.stdout(&mut cmd);
    let expected = "20520945";
    assert!(got.contains(expected));
}

#[cfg(feature = "polars")]
#[test]
fn slice_from_tsvgz() {
    let wrk = Workdir::new("slice_from_tsvgz");
    let test_file = wrk.load_test_file("NYC311-5.tsv.gz");
    let mut cmd = wrk.command("slice");
    cmd.arg(test_file).arg("--index").arg("2").arg("--json");

    wrk.assert_success(&mut cmd);

    let got: String = wrk.stdout(&mut cmd);
    let expected = "20520945";
    assert!(got.contains(expected));
}

#[cfg(feature = "polars")]
#[test]
fn slice_from_tsvzst() {
    let wrk = Workdir::new("slice_from_tsvzst");
    let test_file = wrk.load_test_file("NYC311-5.tsv.zst");
    let mut cmd = wrk.command("slice");
    cmd.arg(test_file).arg("--index").arg("2").arg("--json");

    wrk.assert_success(&mut cmd);

    let got: String = wrk.stdout(&mut cmd);
    let expected = "20520945";
    assert!(got.contains(expected));
}

#[cfg(feature = "polars")]
#[test]
fn slice_from_tsvzlib() {
    let wrk = Workdir::new("slice_from_tsvzlib");
    let test_file = wrk.load_test_file("NYC311-5.tsv.zlib");
    let mut cmd = wrk.command("slice");
    cmd.arg(test_file).arg("--index").arg("2").arg("--json");

    wrk.assert_success(&mut cmd);

    let got: String = wrk.stdout(&mut cmd);
    let expected = "20520945";
    assert!(got.contains(expected));
}

#[test]
fn slice_from_tsv() {
    let wrk = Workdir::new("slice_from_tsv");
    let test_file = wrk.load_test_file("NYC311-5.tsv");
    let mut cmd = wrk.command("slice");
    cmd.arg(test_file).arg("--index").arg("2").arg("--json");

    wrk.assert_success(&mut cmd);

    let got: String = wrk.stdout(&mut cmd);
    let expected = "20520945";
    assert!(got.contains(expected));
}
