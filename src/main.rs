use chrono::{TimeZone, Utc};
use clap::Parser;
use rust_stdf::{stdf_file::*, stdf_record_type::*, StdfRecord};
use std::collections::HashMap;
use std::io::{self, BufWriter};
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

    /// Split output into one per input file
    #[arg(short = 'm', long)]
    multiple_output_files: bool,

    /// Files to process
    files: Vec<String>,
}

struct Msg {
    sender: String,
    rec: StdfRecord,
}

// trait Report {
//     fn new(&self, per_file_report: bool) -> ParametricReport;
//     fn processMessage(&self, msg: &Msg) -> Result<(), io::Error>;
// }

// struct ParametricReport {
//     per_file_report: bool,
//     output_files: Vec<BufWriter<File>>,
//     master_info: HashMap<String, String>
// }

// impl ParametricReport {}

// impl Report for ParametricReport {
//     fn new(&self, per_file_report: bool) -> ParametricReport {
//         ParametricReport {
//             per_file_report,
//             output_files: vec![],
//             master_info: HashMap::new()
//         }
//     }

//     fn processMessage(&self, msg: Msg) -> Result<(), io::Error> {

//         let (sender, rec) = msg;

//         match rec {
//             StdfRecord::MIR(ref mir) => {
//                 mir.
//             }
//         }
//         Ok(())
//     }
// }

// struct MirInfo {
//     valid: bool,
// }

fn main() {
    let args = Args::parse();

    println!("{:?}", args);

    let (tx, rx) = mpsc::channel();

    let mut handles: Vec<JoinHandle<()>> = vec![];

    for stdf_path in args.files {
        let tx_to_closure = tx.clone();
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
            // let rec_types = REC_PIR | REC_PRR | REC_PTR;
            // iterator starts from current file position,
            // if file hits EOF, it will NOT redirect to 0.
            for rec in reader.get_record_iter().map(|x| x.unwrap())
            // .filter(|x| x.is_type(rec_types))
            {
                tx_to_closure
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

    let mut mir_cols: HashMap<String, String> = HashMap::new();
    let mut sdr_cols: HashMap<String, String> = HashMap::new();
    let mut pir_cols: HashMap<String, rust_stdf::PIR> = HashMap::new();
    let mut hbr_cols: HashMap<String, HashMap<u16, String>> = HashMap::new();
    let mut sbr_cols: HashMap<String, HashMap<u16, String>> = HashMap::new();
    let mut ptr_cols: HashMap<String, HashMap<(u8, u8), Vec<rust_stdf::PTR>>> = HashMap::new();
    let mut test_defs: HashMap<String, f32> = HashMap::new();

    for msg in rx {
        match msg.rec {
            StdfRecord::MIR(ref mir) => {
                let setup_t = Utc.timestamp_opt(mir.setup_t.into(), 0).unwrap();

                let mir_string = format!(
                    "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},",
                    msg.sender,
                    mir.lot_id,
                    mir.serl_num,
                    setup_t.to_rfc3339(),
                    mir.part_typ,
                    mir.dsgn_rev,
                    mir.pkg_typ,
                    mir.facil_id,
                    mir.proc_id,
                    mir.flow_id,
                    mir.job_nam,
                    mir.job_rev,
                    mir.oper_nam,
                    mir.tstr_typ,
                    mir.stat_num,
                    mir.exec_ver,
                    mir.test_cod,
                    mir.mode_cod,
                    mir.tst_temp,
                    mir.spec_nam,
                    mir.spec_ver,
                );
                println!("MIR {}", mir_string);
                mir_cols.insert(msg.sender, mir_string);
            }
            StdfRecord::SDR(ref sdr) => {
                let sdr_string = format!(
                    "{},{},{},{},{},{},{},",
                    sdr.hand_id,
                    sdr.hand_typ,
                    sdr.load_id,
                    sdr.cont_id,
                    sdr.card_typ,
                    sdr.dib_typ,
                    sdr.dib_id
                );
                println!("SDR {}", sdr_string);
                sdr_cols.insert(msg.sender, sdr_string);
            }
            StdfRecord::HBR(ref hbr) => {
                if !hbr_cols.contains_key(&msg.sender) {
                    let bin_lookup = HashMap::new();
                    hbr_cols.insert(msg.sender.clone(), bin_lookup);
                }
                hbr_cols
                    .get_mut(&msg.sender)
                    .unwrap()
                    .insert(hbr.hbin_num, hbr.hbin_nam.to_string());

                println!("HBR {:?}", hbr_cols);
            }
            StdfRecord::SBR(ref sbr) => {
                if !sbr_cols.contains_key(&msg.sender) {
                    let bin_lookup = HashMap::new();
                    sbr_cols.insert(msg.sender.clone(), bin_lookup);
                }
                sbr_cols
                    .get_mut(&msg.sender)
                    .unwrap()
                    .insert(sbr.sbin_num, sbr.sbin_nam.to_string());

                println!("SBR {:?}", sbr_cols);
            }
            StdfRecord::PIR(pir) => {
                let pir_string = format!("{},", pir.site_num);
                println!("PIR {}", pir_string);
                pir_cols.insert(msg.sender, pir);
            }
            StdfRecord::PTR(ptr) => {
                if !ptr_cols.contains_key(&msg.sender) {
                    let bin_lookup = HashMap::new();
                    ptr_cols.insert(msg.sender.clone(), bin_lookup);
                }
                let ptrs = ptr_cols.get_mut(&msg.sender).unwrap();

                if !ptrs.contains_key(&(ptr.head_num, ptr.site_num)) {
                    ptrs.insert((ptr.head_num, ptr.site_num), Vec::new());
                }
                ptrs.get_mut(&(ptr.head_num, ptr.site_num))
                    .unwrap()
                    .push(ptr);
            }
            StdfRecord::PRR(prr) => {
                let test_results = ptr_cols
                    .get_mut(&msg.sender)
                    .unwrap()
                    .get_mut(&(prr.head_num, prr.site_num))
                    .unwrap();

                let test_string: String = test_results
                    .into_iter()
                    .map(|x| x.result.to_string())
                    .collect::<Vec<String>>()
                    .join(",");

                println!("TEST RESULTS LENGTH {}", test_results.len());

                test_results.clear();
            }
            _ => {}
        }
    }

    for handle in handles {
        handle.join().unwrap();
    }
}
