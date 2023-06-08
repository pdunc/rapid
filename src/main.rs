use clap::Parser;
use rust_stdf::{stdf_file::*, stdf_record_type::*, StdfRecord};
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

    let mut handles: Vec<JoinHandle<()>> = vec![];

    for stdf_path in args.files {
        let handle = thread::spawn(move || {
            println!("starting to process {}", stdf_path);
            let mut reader = match StdfReader::new(&stdf_path) {
                Ok(r) => r,
                Err(e) => {
                    println!("{}", e);
                    return;
                }
            };

            // we will count total DUT# in the file
            // and put test result of PTR named
            // "continuity test" in a vector.
            let mut dut_count: u64 = 0;
            let mut continuity_rlt = vec![];

            // use type filter to work on certain types,
            // use `|` to combine multiple typs
            let rec_types = REC_PIR | REC_PTR;
            // iterator starts from current file position,
            // if file hits EOF, it will NOT redirect to 0.
            for rec in reader
                .get_record_iter()
                .map(|x| x.unwrap())
                .filter(|x| x.is_type(rec_types))
            {
                match rec {
                    StdfRecord::PIR(_) => {
                        dut_count += 1;
                    }
                    StdfRecord::PTR(ref ptr_rec) => {
                        if ptr_rec.test_txt == "continuity test" {
                            continuity_rlt.push(ptr_rec.result);
                        }
                    }
                    _ => {}
                }
            }
            println!(
                "Total duts {} \n continuity result {:?}",
                dut_count, continuity_rlt
            );
            println!("finished processing {}", stdf_path);
        });

        println!("pushing the handle");
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }
}
