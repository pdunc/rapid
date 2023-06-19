use chrono::{TimeZone, Utc};
use clap::Parser;
use polars::prelude::*;
use rust_stdf::{stdf_file::*, StdfRecord};
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

    type HeadNum = u8;
    type SiteNum = u8;
    type BinNum = u16;
    type BinDescription = String;
    type FileName = String;
    type PartId = usize;
    type ColumnName = String;
    type TestResult = Option<f32>;

    let mut mir_cols: HashMap<FileName, rust_stdf::MIR> = HashMap::new();
    let mut sdr_cols: HashMap<FileName, rust_stdf::SDR> = HashMap::new();
    let mut pir_cols: HashMap<FileName, rust_stdf::PIR> = HashMap::new();
    let mut hbr_cols: HashMap<FileName, HashMap<BinNum, BinDescription>> = HashMap::new();
    let mut sbr_cols: HashMap<FileName, HashMap<BinNum, BinDescription>> = HashMap::new();
    let mut ptr_cols: HashMap<FileName, HashMap<(HeadNum, SiteNum), Vec<rust_stdf::PTR>>> =
        HashMap::new();
    let mut ptr_data: HashMap<FileName, HashMap<ColumnName, Vec<TestResult>>> = HashMap::new();
    let mut n_parts_observered: HashMap<FileName, PartId> = HashMap::new();
    let mut prrs: HashMap<FileName, Vec<rust_stdf::PRR>> = HashMap::new();

    for msg in rx {
        match msg.rec {
            StdfRecord::MIR(mir) => {
                // let setup_t = Utc.timestamp_opt(mir.setup_t.into(), 0).unwrap();

                // let mir_string = format!(
                //     "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},",
                //     msg.sender,
                //     mir.lot_id,
                //     mir.serl_num,
                //     setup_t.to_rfc3339(),
                //     mir.part_typ,
                //     mir.dsgn_rev,
                //     mir.pkg_typ,
                //     mir.facil_id,
                //     mir.proc_id,
                //     mir.flow_id,
                //     mir.job_nam,
                //     mir.job_rev,
                //     mir.oper_nam,
                //     mir.tstr_typ,
                //     mir.stat_num,
                //     mir.exec_ver,
                //     mir.test_cod,
                //     mir.mode_cod,
                //     mir.tst_temp,
                //     mir.spec_nam,
                //     mir.spec_ver,
                // );
                // println!("MIR {}", mir_string);
                mir_cols.insert(msg.sender, mir);
            }
            StdfRecord::SDR(sdr) => {
                // let sdr_string = format!(
                //     "{},{},{},{},{},{},{},",
                //     sdr.hand_id,
                //     sdr.hand_typ,
                //     sdr.load_id,
                //     sdr.cont_id,
                //     sdr.card_typ,
                //     sdr.dib_typ,
                //     sdr.dib_id
                // );
                // println!("SDR {}", sdr_string);
                sdr_cols.insert(msg.sender, sdr);
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

                // println!("HBR {:?}", hbr_cols);
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

                // println!("SBR {:?}", sbr_cols);
            }
            StdfRecord::PIR(pir) => {
                let pir_string = format!("{},", pir.site_num);
                // println!("PIR {}", pir_string);
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
                // When we hit a PRR, we want to collate all the PTRs/FTRs
                // which have occurred for this device.
                // We can't create the full string because the bin definitions
                // don't appear until the end of the file

                // hashmap of files --> hashmap of columns with a 'vec' of test results

                let test_results = ptr_cols
                    .get_mut(&msg.sender)
                    .unwrap()
                    .get_mut(&(prr.head_num, prr.site_num))
                    .unwrap();

                // add a hashmap for the ptr data for this file
                if !ptr_data.contains_key(&msg.sender) {
                    ptr_data.insert(msg.sender.clone(), HashMap::new());
                }

                let results = ptr_data.get_mut(&msg.sender).unwrap();

                if !n_parts_observered.contains_key(&msg.sender) {
                    n_parts_observered.insert(msg.sender.clone(), 0);
                }

                let parts_observed_in_file = n_parts_observered.get_mut(&msg.sender).unwrap();

                test_results.into_iter().for_each(|x| {
                    let ptr_results = results
                        .entry([x.test_num.to_string(), x.test_txt.clone()].join("___"))
                        .or_insert_with(|| vec![]);

                    let num_observations = ptr_results.len();

                    if num_observations < *parts_observed_in_file {
                        let elements_to_add = *parts_observed_in_file - num_observations;
                        let padding: Vec<Option<f32>> = vec![None; elements_to_add];
                        ptr_results.extend(padding);
                    }

                    ptr_results.push(Some(x.result));
                });

                // println!("TEST RESULTS LENGTH {}", test_results.len());

                test_results.clear();

                prrs.entry(msg.sender).or_insert_with(|| vec![]).push(prr);
                *parts_observed_in_file += 1;
            }
            _ => {}
        }
    }

    // println!("RESULTS {:?}", ptr_data);

    for (k, mir) in mir_cols {
        let sdr = sdr_cols.get(&k).unwrap();
        let prrs = prrs.get(&k).unwrap();
        let total_parts = n_parts_observered.get(&k).unwrap();
        let mut ptrs: Vec<Series> = ptr_data
            .get(&k)
            .unwrap()
            .iter()
            .map(|(tname, data)| Series::new(tname, data))
            .collect();

        let part_id_values: Vec<String> = prrs.iter().map(|prr| prr.part_id.clone()).collect();
        let part_txt_values: Vec<String> = prrs.iter().map(|prr| prr.part_txt.clone()).collect();
        let hbin_values: Vec<u32> = prrs.iter().map(|prr| prr.hard_bin as u32).collect();
        let sbin_values: Vec<u32> = prrs.iter().map(|prr| prr.soft_bin as u32).collect();

        let hbin_desc_values: Vec<BinDescription> = hbin_values
            .iter()
            .map(|hbin| {
                hbr_cols
                    .get(&k)
                    .unwrap()
                    .get(&(*hbin as BinNum))
                    .unwrap()
                    .clone()
            })
            .collect();

        let sbin_desc_values: Vec<BinDescription> = sbin_values
            .iter()
            .map(|sbin| {
                sbr_cols
                    .get(&k)
                    .unwrap()
                    .get(&(*sbin as BinNum))
                    .unwrap()
                    .clone()
            })
            .collect();

        let lot_ids = Series::new("Lot ID", vec![mir.lot_id; *total_parts]);
        let hand_typs = Series::new("Handler Type", vec![sdr.hand_typ.clone(); *total_parts]);

        let part_ids = Series::new("Part ID", part_id_values);
        let part_txt = Series::new("Part TXT", part_txt_values);

        let hbins = Series::new("HBIN", hbin_values);
        let sbins = Series::new("SBIN", sbin_values);

        let hbin_desc = Series::new("HBIN Description", hbin_desc_values);
        let sbin_desc = Series::new("SBIN Description", sbin_desc_values);

        let file_names = Series::new("File Name", vec![k.clone(); *total_parts]);

        let mut fields = vec![
            file_names, lot_ids, hand_typs, part_ids, part_txt, hbins, hbin_desc, sbins, sbin_desc,
        ];

        fields.append(&mut ptrs);

        let mut df = DataFrame::new(fields).unwrap();

        // let mut df = df!(
        //     "File Name" => &file_names,
        //     "Lot ID" => &lot_ids,
        //     "Handler Type" => &hand_typs,
        //     "Part ID" => &part_ids,
        //     "Part TXT" => &part_txts,
        //     "HBIN" => &hbins,
        //     "HBIN Description" => &hbin_desc,
        //     "SBIN" => &sbins,
        //     "SBIN Description" => &sbin_desc,
        // )
        // .unwrap();

        // println!("{:?}", df);
        let mut file = std::fs::File::create([k, ".para.csv".to_string()].join("")).unwrap();
        CsvWriter::new(&mut file).finish(&mut df).unwrap();
    }

    for handle in handles {
        handle.join().unwrap();
    }
}
