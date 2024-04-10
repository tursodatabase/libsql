use std::{env::args, collections::HashMap, path::Path, io::{BufWriter, BufReader, BufRead, Write}};

use regex::Regex;
use walkdir::WalkDir;

fn main() {
    let re = Regex::new(r#"([a-zA-Z0-9]*)\.test_(\d+)\.sql"#).unwrap();
    let dir = args().nth(1).unwrap();
    let mut tests = HashMap::new();
    let walk = WalkDir::new(dir);
    for entry in walk {
        let entry = entry.unwrap();
        let name = entry.file_name().to_str().unwrap();
        let Some(capts) = re.captures(name) else { continue };
        let test_name = &capts[1];
        let num = capts[2].parse::<usize>().unwrap();

        tests.entry(test_name.to_string()).or_insert(Vec::new()).push((num, entry.path().to_owned()));
    }

    let out_dir = args().nth(2).unwrap();
    let out_dir: &Path = out_dir.as_ref();
    std::fs::create_dir_all(&out_dir).unwrap();
    for (test_name, mut tests) in tests {
        let file = std::fs::File::create(out_dir.join(test_name)).unwrap();
        let mut writer = BufWriter::new(file);
        tests.sort_by_key(|x| x.0);
        for test in tests {
            let mut reader = BufReader::new(std::fs::File::open(test.1).unwrap());
            let mut line = String::new();
            while reader.read_line(&mut line).unwrap() != 0 {
                if !line.starts_with("--") {
                    writer.write_all(&line.as_bytes()).unwrap();
                }

                line.clear();
            }
            writer.flush().unwrap();
        }
    }
}
