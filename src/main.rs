use chrono::{TimeZone, Utc};
use clap::Parser;
use polars::functions::diag_concat_df;
use polars::prelude::*;
use rust_stdf::{stdf_file::*, StdfRecord};
use std::collections::HashMap;
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
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

    /// Name separator for test names and numbers
    #[arg(short, long, default_value_t=("::").to_string())]
    separator: String,

    /// Output directory
    #[arg(short, long)]
    output_dir: Option<String>,

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

    let output_dir = args.output_dir.map(|x| PathBuf::from(x));

    let mut dfs: Vec<DataFrame> = vec![];

    let (tx, rx) = mpsc::channel();

    let mut handles: Vec<JoinHandle<()>> = vec![];

    for stdf_path in args.files {
        let tx_to_closure = tx.clone();
        let handle = thread::spawn(move || {
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
            for rec_result in reader.get_record_iter()
            // .filter(|x| x.is_type(rec_types))
            {
                let send_message_result = rec_result
                    .map(|rec| {
                        tx_to_closure.send(Msg {
                            sender: stdf_path.clone(),
                            rec,
                        })
                    })
                    .map_err(|err| println!("Problem reading STDF :: {}", err));

                if send_message_result.is_err() {
                    println!("Error sending message to processor, aborting");
                    break;
                }

                let internal_send_message_result = send_message_result.unwrap();

                if internal_send_message_result.is_err() {
                    println!(
                        "Error sending message :: {:?}",
                        internal_send_message_result.err()
                    );
                    break;
                }
            }
        });

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
    type FunctionalResult = Option<u32>;

    #[derive(Debug)]
    struct PtrOptionalData {
        opt_flag: Option<[u8; 1]>, // Optional data flag
        _res_scal: Option<i8>,     // Test results scaling exponent
        _llm_scal: Option<i8>,     // Low limit scaling exponent
        _hlm_scal: Option<i8>,     // High limit scaling exponent
        lo_limit: Option<f32>,     // Low test limit value
        hi_limit: Option<f32>,     // High test limit value
        _units: Option<String>,    // Test units
        _c_resfmt: Option<String>, // ANSI C result format string
        _c_llmfmt: Option<String>, // ANSI C low limit format string
        _c_hlmfmt: Option<String>, // ANSI C high limit format string
        _lo_spec: Option<f32>,     // Low specification limit value
        _hi_spec: Option<f32>,     // High specification limit value
    }

    let mut mir_cols: HashMap<FileName, rust_stdf::MIR> = HashMap::new();
    let mut sdr_cols: HashMap<FileName, rust_stdf::SDR> = HashMap::new();
    // let mut pir_cols: HashMap<FileName, rust_stdf::PIR> = HashMap::new();
    let mut hbr_cols: HashMap<FileName, HashMap<BinNum, BinDescription>> = HashMap::new();
    let mut sbr_cols: HashMap<FileName, HashMap<BinNum, BinDescription>> = HashMap::new();
    let mut limit_cols: HashMap<
        FileName,
        HashMap<(HeadNum, SiteNum), HashMap<ColumnName, PtrOptionalData>>,
    > = HashMap::new();
    let mut ptr_cols: HashMap<FileName, HashMap<(HeadNum, SiteNum), Vec<rust_stdf::PTR>>> =
        HashMap::new();
    let mut ftr_cols: HashMap<FileName, HashMap<(HeadNum, SiteNum), Vec<rust_stdf::FTR>>> =
        HashMap::new();
    let mut ptr_data: HashMap<FileName, HashMap<ColumnName, Vec<TestResult>>> = HashMap::new();
    let mut ftr_data: HashMap<FileName, HashMap<ColumnName, Vec<FunctionalResult>>> =
        HashMap::new();
    let mut pf_data: HashMap<FileName, HashMap<ColumnName, Vec<FunctionalResult>>> = HashMap::new();
    let mut n_parts_observered: HashMap<FileName, PartId> = HashMap::new();
    let mut prrs: HashMap<FileName, Vec<rust_stdf::PRR>> = HashMap::new();

    for msg in rx {
        match msg.rec {
            StdfRecord::MIR(mir) => {
                mir_cols
                    .entry(msg.sender)
                    .and_modify(|_| println!("Multiple MIR in file, not supported"))
                    .or_insert(mir);
            }
            StdfRecord::SDR(sdr) => {
                // TODO :: handle multiple SDRs, this is valid in STDF
                sdr_cols
                    .entry(msg.sender)
                    .and_modify(|_| println!("Multiple SDR in file, not supported"))
                    .or_insert(sdr);
            }
            StdfRecord::HBR(ref hbr) => {
                hbr_cols
                    .entry(msg.sender)
                    .or_insert(HashMap::new())
                    .entry(hbr.hbin_num)
                    .and_modify(|x| {
                        if *x != hbr.hbin_nam {
                            println!(
                                "Multiple definitions for HBIN {}, using {}",
                                hbr.hbin_num, x
                            )
                        }
                    })
                    .or_insert(hbr.hbin_nam.to_string());
            }
            StdfRecord::SBR(ref sbr) => {
                sbr_cols
                    .entry(msg.sender)
                    .or_insert(HashMap::new())
                    .entry(sbr.sbin_num)
                    .and_modify(|x| {
                        if *x != sbr.sbin_nam {
                            println!(
                                "Multiple definitions for HBIN {}, using {}",
                                sbr.sbin_num, x
                            )
                        }
                    })
                    .or_insert(sbr.sbin_nam.to_string());
            }
            StdfRecord::PIR(_) => {}
            StdfRecord::PTR(ptr) => {
                ptr_cols
                    .entry(msg.sender.clone())
                    .or_insert(HashMap::new())
                    .entry((ptr.head_num, ptr.site_num))
                    .or_insert(Vec::new())
                    .push(ptr.clone());

                let test_key =
                    [ptr.test_num.to_string(), ptr.test_txt.clone()].join(&args.separator);

                //bit 0 set = RES_SCAL value is invalid. The default set by the first PTR with this test
                // number will be used.
                // bit 1 reserved for future used and must be 1.
                // bit 2 set = No low specification limit.
                // bit 3 set = No high specification limit.
                // bit 4 set = LO_LIMIT and LLM_SCAL are invalid. The default values set for these fields
                // in the first PTR with this test number will be used.
                // bit 5 set = HI_LIMIT and HLM_SCAL are invalid. The default values set for these fields
                // in the first PTR with this test number will be used.
                // bit 6 set = No Low Limit for this test (LO_LIMIT and LLM_SCAL are invalid).
                // bit 7 set = NoHigh Limit for this test (HI_LIMIT and HLM_SCAL are invalid).

                limit_cols
                    .entry(msg.sender) // File name
                    .or_insert(HashMap::new())
                    .entry((ptr.head_num, ptr.site_num)) // head_num, site_num
                    .or_insert(HashMap::new())
                    .entry(test_key.clone()) // test_key, optionalData
                    .and_modify(|optional_data| {
                        let hi_lim_changed =
                            ptr.hi_limit.is_some() && (ptr.hi_limit != optional_data.hi_limit);
                        let lo_lim_changed =
                            ptr.lo_limit.is_some() && (ptr.lo_limit != optional_data.lo_limit);

                        if hi_lim_changed || lo_lim_changed {
                            println!("attempt to update existing limits, using initial limit :: {} :: ({:?},{:?}) -> ({:?},{:?})",
                            test_key,
                            optional_data.lo_limit,
                            optional_data.hi_limit,
                            ptr.lo_limit,
                            ptr.hi_limit,
                        );
                        }
                    })
                    .or_insert(PtrOptionalData {
                        opt_flag: ptr.opt_flag,
                        _res_scal: ptr.res_scal,
                        _llm_scal: ptr.llm_scal,
                        _hlm_scal: ptr.hlm_scal,
                        lo_limit: ptr.lo_limit,
                        hi_limit: ptr.hi_limit,
                        _units: ptr.units,
                        _c_resfmt: ptr.c_resfmt,
                        _c_llmfmt: ptr.c_llmfmt,
                        _c_hlmfmt: ptr.c_hlmfmt,
                        _lo_spec: ptr.lo_spec,
                        _hi_spec: ptr.hi_spec,
                    });
            }
            StdfRecord::FTR(ftr) => {
                ftr_cols
                    .entry(msg.sender)
                    .or_insert(HashMap::new())
                    .entry((ftr.head_num, ftr.site_num))
                    .or_insert(Vec::new())
                    .push(ftr);
            }
            StdfRecord::PRR(prr) => {
                // When we hit a PRR, we want to collate all the PTRs/FTRs
                // which have occurred for this device.

                // hashmap of files --> hashmap of columns with a 'vec' of test results

                let parts_observed_in_file =
                    n_parts_observered.entry(msg.sender.clone()).or_insert(0);

                let device_ptrs = ptr_cols
                    .entry(msg.sender.clone())
                    .or_insert(HashMap::new())
                    .entry((prr.head_num, prr.site_num))
                    .or_insert(Vec::new());

                let device_ftrs = ftr_cols
                    .entry(msg.sender.clone())
                    .or_insert(HashMap::new())
                    .entry((prr.head_num, prr.site_num))
                    .or_insert(Vec::new());

                let all_ptr_results = ptr_data.entry(msg.sender.clone()).or_insert(HashMap::new());
                let all_ftr_results = ftr_data.entry(msg.sender.clone()).or_insert(HashMap::new());
                let all_pf_results = pf_data.entry(msg.sender.clone()).or_insert(HashMap::new());

                let limits = limit_cols
                    .entry(msg.sender.clone())
                    .or_insert(HashMap::new())
                    .entry((prr.head_num, prr.site_num))
                    .or_insert(HashMap::new());

                device_ptrs.into_iter().for_each(|x| {
                    let test_key =
                        [x.test_num.to_string(), x.test_txt.clone()].join(&args.separator);

                    // get all of the current PTR results in a Vec
                    let ptr_results = all_ptr_results
                        .entry(test_key.clone())
                        .or_insert_with(|| vec![]);

                    let pf_results = all_pf_results
                        .entry([("PF").to_string(), test_key.clone()].join(&args.separator))
                        .or_insert_with(|| vec![]);

                    // extend the vec with `None` values if required
                    let current_num_ptr_observations = ptr_results.len();

                    if current_num_ptr_observations < *parts_observed_in_file {
                        let elements_to_add =
                            *parts_observed_in_file - current_num_ptr_observations;
                        let padding: Vec<Option<f32>> = vec![None; elements_to_add];
                        ptr_results.extend(padding);

                        let padding_pf: Vec<Option<u32>> = vec![None; elements_to_add];
                        pf_results.extend(padding_pf);
                    }

                    // Add the result for this PTR
                    ptr_results.push(Some(x.result));

                    let ptr_optional_data = limits.entry(test_key).or_insert(PtrOptionalData {
                        opt_flag: Some([0b1111_1111]), // all invalid
                        _res_scal: None,
                        _llm_scal: None,
                        _hlm_scal: None,
                        lo_limit: None,
                        hi_limit: None,
                        _units: None,
                        _c_resfmt: None,
                        _c_llmfmt: None,
                        _c_hlmfmt: None,
                        _lo_spec: None,
                        _hi_spec: None,
                    });

                    let lo_limit = if ptr_optional_data.opt_flag.is_some()
                        && ((ptr_optional_data.opt_flag.unwrap()[0] & 0b0101_0000) == 0)
                    {
                        ptr_optional_data.lo_limit
                    } else {
                        None
                    };
                    let hi_limit = if ptr_optional_data.opt_flag.is_some()
                        && ((ptr_optional_data.opt_flag.unwrap()[0] & 0b1010_0000) == 0)
                    {
                        ptr_optional_data.hi_limit
                    } else {
                        None
                    };

                    let pass_lo_limit = lo_limit.is_none() || x.result >= lo_limit.unwrap();
                    let pass_hi_limit = hi_limit.is_none() || x.result <= hi_limit.unwrap();

                    pf_results.push(Some((pass_lo_limit && pass_hi_limit) as u32));
                });

                device_ptrs.clear();

                // FTR implementation
                device_ftrs.into_iter().for_each(|x| {
                    let ftr_results = all_ftr_results
                        .entry([x.test_num.to_string(), x.test_txt.clone()].join(&args.separator))
                        .or_insert_with(|| vec![]);

                    let pf_results = all_pf_results
                        .entry(
                            [
                                ("PF").to_string(),
                                x.test_num.to_string(),
                                x.test_txt.clone(),
                            ]
                            .join(&args.separator),
                        )
                        .or_insert_with(|| vec![]);

                    let num_observations = ftr_results.len();

                    if num_observations < *parts_observed_in_file {
                        let elements_to_add = *parts_observed_in_file - num_observations;
                        let padding: Vec<Option<u32>> = vec![None; elements_to_add];
                        let padding_pf: Vec<Option<u32>> = vec![None; elements_to_add];
                        ftr_results.extend(padding);
                        pf_results.extend(padding_pf);
                    }

                    ftr_results.push(Some(x.test_flg[0] as u32));
                    pf_results.push(Some((x.test_flg[0] == 0) as u32));
                });

                device_ftrs.clear();

                prrs.entry(msg.sender).or_insert_with(|| vec![]).push(prr);
                *parts_observed_in_file += 1;
            }
            _ => {}
        }
    }

    // use MIR (one per device) as the means of building
    // the DataFrames
    for (k, mir) in mir_cols {
        let sdr = sdr_cols.get(&k).unwrap();
        let prrs = prrs.get(&k).unwrap();
        let total_parts = n_parts_observered.get(&k).unwrap();
        let mut ptrs: Vec<Series> = ptr_data
            .entry(k.clone())
            .or_insert(HashMap::new())
            .iter()
            .map(|(tname, data)| Series::new(tname, data))
            .collect();
        let mut ftrs: Vec<Series> = ftr_data
            .entry(k.clone())
            .or_insert(HashMap::new())
            .iter()
            .map(|(tname, data)| Series::new(tname, data))
            .collect();
        let mut pf: Vec<Series> = pf_data
            .entry(k.clone())
            .or_insert(HashMap::new())
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
                    .entry(k.clone())
                    .or_insert(HashMap::new())
                    .entry(*hbin as BinNum)
                    .or_insert(("").to_string())
                    .clone()
            })
            .collect();

        let sbin_desc_values: Vec<BinDescription> = sbin_values
            .iter()
            .map(|sbin| {
                sbr_cols
                    .entry(k.clone())
                    .or_insert(HashMap::new())
                    .entry(*sbin as BinNum)
                    .or_insert(("").to_string())
                    .clone()
            })
            .collect();

        let lot_ids = Series::new("Lot ID", vec![mir.lot_id; *total_parts]);
        let hand_id = Series::new("Handler ID", vec![sdr.hand_id.clone(); *total_parts]);
        let hand_typ = Series::new("Handler Type", vec![sdr.hand_typ.clone(); *total_parts]);
        let load_id = Series::new("Loadboard ID", vec![sdr.load_id.clone(); *total_parts]);
        let cont_id = Series::new("Cont ID", vec![sdr.cont_id.clone(); *total_parts]);
        let dib_typ = Series::new("DIB Type", vec![sdr.dib_typ.clone(); *total_parts]);
        let dib_id = Series::new("DIB ID", vec![sdr.dib_id.clone(); *total_parts]);

        let serl_num = Series::new("Serial Num", vec![mir.serl_num; *total_parts]);
        let setup_t = Series::new(
            "Setup Time",
            vec![
                Utc.timestamp_opt(mir.setup_t.into(), 0)
                    .unwrap()
                    .to_rfc3339();
                *total_parts
            ],
        );
        let part_typ = Series::new("Part Type", vec![mir.part_typ; *total_parts]);
        let dsgn_rev = Series::new("Design Rev", vec![mir.dsgn_rev; *total_parts]);
        let pkg_typ = Series::new("Package Type", vec![mir.pkg_typ; *total_parts]);
        let facil_id = Series::new("Facility ID", vec![mir.facil_id; *total_parts]);
        let proc_id = Series::new("Process ID", vec![mir.proc_id; *total_parts]);
        let flow_id = Series::new("Flow ID", vec![mir.flow_id; *total_parts]);
        let job_nam = Series::new("Job Name", vec![mir.job_nam; *total_parts]);
        let job_rev = Series::new("Job Rev", vec![mir.job_rev; *total_parts]);
        let oper_nam = Series::new("Operator Name", vec![mir.oper_nam; *total_parts]);
        let tstr_typ = Series::new("Tester Type", vec![mir.tstr_typ; *total_parts]);
        let stat_num = Series::new("Station Num", vec![mir.stat_num as u32; *total_parts]);
        let exec_ver = Series::new("Exec Version", vec![mir.exec_ver; *total_parts]);
        let test_cod = Series::new("Test Code", vec![mir.test_cod; *total_parts]);
        let mode_cod = Series::new("Mode Code", vec![mir.mode_cod.to_string(); *total_parts]);
        let tst_temp = Series::new("Test Temperature", vec![mir.tst_temp; *total_parts]);
        let spec_nam = Series::new("Spec Name", vec![mir.spec_nam; *total_parts]);
        let spec_ver = Series::new("Spec Version", vec![mir.spec_ver; *total_parts]);

        let part_ids = Series::new("Part ID", part_id_values);
        let part_txt = Series::new("Part TXT", part_txt_values);

        let hbins = Series::new("HBIN", hbin_values);
        let sbins = Series::new("SBIN", sbin_values);

        let hbin_desc = Series::new("HBIN Description", hbin_desc_values);
        let sbin_desc = Series::new("SBIN Description", sbin_desc_values);

        let file_names = Series::new("File Name", vec![k.clone(); *total_parts]);

        let mut fields = vec![
            file_names, lot_ids, serl_num, setup_t, part_typ, dsgn_rev, pkg_typ, facil_id, proc_id,
            flow_id, job_nam, job_rev, oper_nam, tstr_typ, stat_num, exec_ver, test_cod, mode_cod,
            tst_temp, spec_nam, spec_ver, hand_id, hand_typ, load_id, cont_id, dib_typ, dib_id,
            part_ids, part_txt, hbins, hbin_desc, sbins, sbin_desc,
        ];

        fields.append(&mut ptrs);

        if args.is_functional_in_parametric {
            fields.append(&mut ftrs);
        }

        if args.is_pass_fail_column_in_parametric {
            fields.append(&mut pf)
        }

        let mut df = DataFrame::new(fields).unwrap();

        // if individual output files are required, do it here
        if args.multiple_output_files {
            let path = Path::new(&k);

            let dir = if output_dir.is_some() {
                output_dir.clone().unwrap()
            } else {
                path.parent().unwrap().to_path_buf()
            };

            let file_name =
                [path.file_name().unwrap(), OsStr::new(".para.csv")].join(OsStr::new(""));

            let mut file = std::fs::File::create(dir.join(file_name)).unwrap();
            CsvWriter::new(&mut file).finish(&mut df).unwrap();
        } else {
            // append dfs to df vec
            dfs.push(df);
        }
    }

    if !args.multiple_output_files {
        println!("Combining data into single report");

        let mut df = diag_concat_df(&dfs).unwrap();

        let dir = if output_dir.is_some() {
            output_dir.unwrap().clone()
        } else {
            Path::new(".").to_path_buf()
        };

        let file_name = OsStr::new("rapid_parametric.csv");

        let mut file = std::fs::File::create(dir.join(file_name)).unwrap();
        CsvWriter::new(&mut file).finish(&mut df).unwrap();
    }

    for handle in handles {
        handle.join().unwrap();
    }
}
