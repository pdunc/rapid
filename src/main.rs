use clap::Parser;
use rust_stdf::{stdf_file::*, stdf_record_type::*, StdfRecord};
use std::collections::HashMap;
use std::sync::mpsc;
use std::thread::{self, JoinHandle};

/// Simple program to greet a person
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Include pass/fail column for each test in parametric report
    #[arg(short = 'p', long)]
    is_pass_fail_column_in_parametric: bool,

    /// Include functional tests in parametric report
    #[arg(short = 'f', long)]
    is_functional_in_parametric: bool,

    /// Files to process
    files: Vec<String>,
}

fn main() {
    let args = Args::parse();

    println!("{:?}", args);

    let (tx, rx) = mpsc::channel();

    let mut handles: Vec<JoinHandle<()>> = vec![];

    struct Msg {
        sender: String,
        rec: StdfRecord,
    }

    for stdf_path in args.files {
        let txToClosure = tx.clone();
        let handle = thread::spawn(move || {
            println!("starting to process {}", stdf_path);
            let mut reader = match StdfReader::new(&stdf_path) {
                Ok(r) => r,
                Err(e) => {
                    println!("{}", e);
                    return;
                }
            };

            // use type filter to work on certain types,
            // use `|` to combine multiple typs
            let rec_types = REC_PIR | REC_PRR | REC_PTR;
            // iterator starts from current file position,
            // if file hits EOF, it will NOT redirect to 0.
            for rec in reader
                .get_record_iter()
                .map(|x| x.unwrap())
                .filter(|x| x.is_type(rec_types))
            {
                txToClosure
                    .send(Msg {
                        sender: stdf_path.clone(),
                        rec,
                    })
                    .unwrap();
            }

            println!("finished processing {}", stdf_path);
        });

        println!("pushing the handle");
        handles.push(handle);
    }

    drop(tx);

    let mut test_results = HashMap::new();
    // we will count total DUT# in the file
    // and put test result of PTR named
    // "continuity test" in a vector.
    let mut dut_count: u64 = 0;
    let mut continuity_rlt = vec![];

    for msg in rx {
        if msg.sender == "something" {
            println!("something");
        }

        match msg.rec {
            StdfRecord::PIR(ref pir_rec) => {
                dut_count += 1;
                // println!("PIR_HEAD{}_SITE{}", pir_rec.head_num, pir_rec.site_num);
            }
            StdfRecord::PTR(ref ptr_rec) => {
                let test_txt = ptr_rec.test_txt.to_string();

                if ptr_rec.test_txt == "continuity test" {
                    continuity_rlt.push(ptr_rec.result);
                }
                if !test_results.contains_key(&test_txt) {
                    test_results.insert(test_txt, dut_count);
                } else {
                    test_results[&test_txt];
                }
            }
            StdfRecord::PRR(ref prr_rec) => {
                // println!("PRR_ID{}_TXT{}", prr_rec.part_id, prr_rec.part_txt);
            }
            _ => {}
        }
    }
    println!(
        "Total duts {} \n continuity result {:?}",
        dut_count, continuity_rlt
    );

    for handle in handles {
        handle.join().unwrap();
    }
}
