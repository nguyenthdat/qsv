use std::fs::File;

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use csv::Reader;

fn original_trim_bs_whitespace(bytes: &[u8]) -> &[u8] {
    let mut start = 0;
    let mut end = bytes.len();

    // Find start by scanning forward
    while start < end {
        let b = unsafe { *bytes.get_unchecked(start) };
        if !b.is_ascii_whitespace() {
            break;
        }
        start += 1;
    }

    // Find end by scanning backward
    while end > start {
        let b = unsafe { *bytes.get_unchecked(end - 1) };
        if !b.is_ascii_whitespace() {
            break;
        }
        end -= 1;
    }

    unsafe { bytes.get_unchecked(start..end) }
}

fn optimized_trim_bs_whitespace(bytes: &[u8]) -> &[u8] {
    let mut start = 0;
    let mut end = bytes.len();

    // Lookup table for ASCII whitespace characters
    const WHITESPACE: [bool; 256] = {
        let mut table = [false; 256];
        table[b' ' as usize] = true;
        table[b'\t' as usize] = true;
        table[b'\n' as usize] = true;
        table[b'\r' as usize] = true;
        table[b'\x0C' as usize] = true; // form feed
        table
    };

    // Find start by scanning forward
    while start < end {
        if !WHITESPACE[unsafe { *bytes.get_unchecked(start) } as usize] {
            break;
        }
        start += 1;
    }

    // Find end by scanning backward
    while end > start {
        if !WHITESPACE[unsafe { *bytes.get_unchecked(end - 1) } as usize] {
            break;
        }
        end -= 1;
    }

    unsafe { bytes.get_unchecked(start..end) }
}

fn trim_ascii(bytes: &[u8]) -> &[u8] {
    let start = bytes
        .iter()
        .position(|&b| !b.is_ascii_whitespace())
        .unwrap_or(bytes.len());
    let end = bytes
        .iter()
        .rposition(|&b| !b.is_ascii_whitespace())
        .map_or(0, |i| i + 1);
    &bytes[start..end]
}

fn trim_spaces_only(bytes: &[u8]) -> &[u8] {
    let start = bytes.iter().position(|&b| b != b' ').unwrap_or(bytes.len());
    let end = bytes.iter().rposition(|&b| b != b' ').map_or(0, |i| i + 1);
    &bytes[start..end]
}

fn bench_trim(c: &mut Criterion) {
    // Read the CSV file
    let file = File::open("/tmp/NYC_311_SR_2010-2020-sample-1M.csv").unwrap();
    let mut rdr = Reader::from_reader(file);
    let records: Vec<csv::ByteRecord> = rdr.byte_records().map(|r| r.unwrap()).collect();

    let mut group = c.benchmark_group("trim_bs_whitespace");

    group.bench_function("original", |b| {
        b.iter(|| {
            for record in &records {
                for field in record.iter() {
                    black_box(original_trim_bs_whitespace(field));
                }
            }
        })
    });

    group.bench_function("optimized", |b| {
        b.iter(|| {
            for record in &records {
                for field in record.iter() {
                    black_box(optimized_trim_bs_whitespace(field));
                }
            }
        })
    });

    group.bench_function("trim_ascii", |b| {
        b.iter(|| {
            for record in &records {
                for field in record.iter() {
                    black_box(trim_ascii(field));
                }
            }
        })
    });

    group.bench_function("trim_spaces_only", |b| {
        b.iter(|| {
            for record in &records {
                for field in record.iter() {
                    black_box(trim_spaces_only(field));
                }
            }
        })
    });

    group.finish();
}

criterion_group!(benches, bench_trim);
criterion_main!(benches);
