// Lesson 3: qsv and JSON
// https://100.dathere.com/lessons/3

use crate::workdir::Workdir;

// Task 1
#[test]
#[cfg(not(feature = "datapusher_plus"))]
fn flowers_json_to_csv() {
    let wrk = Workdir::new("flowers_json_to_csv");
    let flowers_json_file = wrk.load_test_file("flowers.json");

    let temp_csv = wrk.path("temp.csv").to_string_lossy().to_string();

    let mut json_cmd = wrk.command("json");
    json_cmd.args(vec![flowers_json_file.as_str(), "--output", &temp_csv]);
    wrk.assert_success(&mut json_cmd);

    let mut table_cmd = wrk.command("table");
    table_cmd.arg(&temp_csv);
    let got: String = wrk.stdout(&mut table_cmd);

    let expected = r#"name       primary_color  available  quantity
tulip      purple         true       4
rose       red            true       6
sunflower  yellow         false      0"#;
    similar_asserts::assert_eq!(got, expected);
}

// Task 2
#[test]
#[cfg(not(feature = "datapusher_plus"))]
fn flowers_nested_json_to_csv() {
    let wrk = Workdir::new("flowers_nested_json_to_csv");
    let flowers_nested_json_file = wrk.load_test_file("flowers_nested.json");

    let temp_csv = wrk.path("temp.csv").to_string_lossy().to_string();
    let mut cmd = wrk.command("json");
    cmd.args(vec![
        flowers_nested_json_file.as_str(),
        "--jaq",
        ".roses",
        "--output",
        &temp_csv,
    ]);

    wrk.assert_success(&mut cmd);

    let mut table_cmd = wrk.command("table");
    table_cmd.arg(&temp_csv);
    let got: String = wrk.stdout(&mut table_cmd);

    let expected = r#"color  quantity
red    4
white  1
pink   1"#;
    similar_asserts::assert_eq!(got, expected);
}

// Task 3
#[test]
#[cfg(not(feature = "datapusher_plus"))]
fn buses_csv_to_json() {
    let wrk = Workdir::new("buses_csv_to_json");
    let buses_csv_file = wrk.load_test_file("buses.csv");
    let mut cmd = wrk.command("slice");
    cmd.arg(buses_csv_file.as_str()).arg("--json");

    let got: String = wrk.stdout(&mut cmd);
    let expected = r#"[{"id":"001","primary_color":"black","secondary_color":"blue","length":"full","air_conditioner":"true","amenities":"wheelchair ramp, tissue boxes, cup holders, USB ports"},{"id":"002","primary_color":"black","secondary_color":"red","length":"full","air_conditioner":"true","amenities":"wheelchair ramp, tissue boxes, USB ports"},{"id":"003","primary_color":"white","secondary_color":"blue","length":"half","air_conditioner":"true","amenities":"wheelchair ramp, tissue boxes"},{"id":"004","primary_color":"orange","secondary_color":"blue","length":"full","air_conditioner":"false","amenities":"wheelchair ramp, tissue boxes, USB ports"},{"id":"005","primary_color":"black","secondary_color":"blue","length":"full","air_conditioner":"true","amenities":"wheelchair ramp, tissue boxes, cup holders, USB ports"}]"#;
    similar_asserts::assert_eq!(got, expected);
}
