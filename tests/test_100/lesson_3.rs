// Lesson 3: qsv and JSON
// https://100.dathere.com/lessons/3

use std::{io::Write, process};

use crate::workdir::Workdir;

// Task 1
#[test]
#[cfg(not(feature = "datapusher_plus"))]
fn flowers_json_to_csv() {
    let wrk = Workdir::new("flowers_json_to_csv");
    let flowers_json_file = wrk.load_test_file("flowers.json");

    let mut json_cmd = process::Command::new(wrk.qsv_bin());
    json_cmd.args(vec!["json", flowers_json_file.as_str()]);
    let json_stdout: String = wrk.stdout(&mut json_cmd);

    let mut table_child = process::Command::new(wrk.qsv_bin())
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .args(vec!["table"])
        .spawn()
        .unwrap();
    let mut table_stdin = table_child.stdin.take().unwrap();
    let handle = std::thread::spawn(move || {
        table_stdin.write_all(json_stdout.as_bytes()).unwrap();
    });
    // Wait for the writing thread to complete
    handle.join().unwrap();
    let output = table_child.wait_with_output().unwrap();
    let got = String::from_utf8_lossy(&output.stdout);

    let expected = r#"name       primary_color  available  quantity
tulip      purple         true       4
rose       red            true       6
sunflower  yellow         false      0
"#;
    similar_asserts::assert_eq!(got, expected);
}

// Task 2
#[test]
#[cfg(not(feature = "datapusher_plus"))]
fn flowers_nested_json_to_csv() {
    let wrk = Workdir::new("flowers_nested_json_to_csv");
    let flowers_nested_json_file = wrk.load_test_file("flowers_nested.json");

    let mut json_cmd = process::Command::new(wrk.qsv_bin());
    json_cmd.args(vec![
        "json",
        flowers_nested_json_file.as_str(),
        "--jaq",
        ".roses",
    ]);
    let json_stdout: String = wrk.stdout(&mut json_cmd);

    let mut table_child = process::Command::new(wrk.qsv_bin())
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .args(vec!["table"])
        .spawn()
        .unwrap();
    let mut table_stdin = table_child.stdin.take().unwrap();
    let handle = std::thread::spawn(move || {
        table_stdin.write_all(json_stdout.as_bytes()).unwrap();
    });
    // Wait for the writing thread to complete
    handle.join().unwrap();
    let output = table_child.wait_with_output().unwrap();
    let got = String::from_utf8_lossy(&output.stdout);

    let expected = r#"color  quantity
red    4
white  1
pink   1
"#;
    similar_asserts::assert_eq!(got, expected);
}

// Task 3
#[test]
#[cfg(not(feature = "datapusher_plus"))]
fn buses_csv_to_json() {
    let wrk = Workdir::new("buses_csv_to_json");
    let buses_csv_file = wrk.load_test_file("buses.csv");
    let mut cmd = process::Command::new(wrk.qsv_bin());
    cmd.args(vec!["slice", buses_csv_file.as_str(), "--json"]);

    let got: String = wrk.stdout(&mut cmd);
    let expected = r#"[{"id":"001","primary_color":"black","secondary_color":"blue","length":"full","air_conditioner":"true","amenities":"wheelchair ramp, tissue boxes, cup holders, USB ports"},{"id":"002","primary_color":"black","secondary_color":"red","length":"full","air_conditioner":"true","amenities":"wheelchair ramp, tissue boxes, USB ports"},{"id":"003","primary_color":"white","secondary_color":"blue","length":"half","air_conditioner":"true","amenities":"wheelchair ramp, tissue boxes"},{"id":"004","primary_color":"orange","secondary_color":"blue","length":"full","air_conditioner":"false","amenities":"wheelchair ramp, tissue boxes, USB ports"},{"id":"005","primary_color":"black","secondary_color":"blue","length":"full","air_conditioner":"true","amenities":"wheelchair ramp, tissue boxes, cup holders, USB ports"}]"#;
    similar_asserts::assert_eq!(got, expected);
}
