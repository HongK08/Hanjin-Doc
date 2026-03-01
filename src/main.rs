use anyhow::{bail, Context, Result};
use calamine::{open_workbook_auto, Data, Reader};
use chrono::{Duration as ChronoDuration, Local, NaiveDate};
use rayon::prelude::*;
use serde::Serialize;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::env;
use std::fs::{self, File};
use std::io::{BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime};
use zip::write::SimpleFileOptions;
use zip::{CompressionMethod, ZipArchive, ZipWriter};

#[derive(Debug)]
struct AppConfig {
    workdir: PathBuf,
    once: bool,
    smoke: bool,
    legacy: Option<LegacyArgs>,
}

#[derive(Debug, Clone)]
struct LegacyArgs {
    inbound: PathBuf,
    stock: PathBuf,
    outbound: PathBuf,
    out_json: PathBuf,
}

#[derive(Debug, Clone)]
struct Workspace {
    root: PathBuf,
    input_dir: PathBuf,
    output: PathBuf,
    logs: PathBuf,
    reports: PathBuf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum DocKind {
    Inbound,
    Stock,
    Outbound,
}

#[derive(Debug, Clone)]
struct CandidateFile {
    kind: DocKind,
    path: PathBuf,
    modified: SystemTime,
}

#[derive(Debug, Clone)]
struct BatchSelection {
    inbound: PathBuf,
    stock: PathBuf,
    outbound: PathBuf,
}

#[derive(Debug, Clone)]
struct BatchArtifacts {
    out_json: PathBuf,
    snapshot_date: String,
    converted_parts: usize,
    smoke_txt: Option<PathBuf>,
    docx_outdir: Option<PathBuf>,
    docx_generated: usize,
}

#[derive(Debug, Serialize)]
struct SnapshotMetaCounts {
    parts: usize,
    inbound_rows: usize,
    stock_rows: usize,
    outbound_rows: usize,
}

#[derive(Debug, Serialize)]
struct SnapshotStorage {
    raw_rows: String,
    part_rows: String,
}

#[derive(Debug, Serialize)]
struct SnapshotMeta {
    generated_at: String,
    snapshot_date: String,
    counts: SnapshotMetaCounts,
    storage: SnapshotStorage,
    calc_order: Vec<String>,
}

#[derive(Debug, Serialize)]
struct SnapshotRaw {
    inbound_rows: Vec<RowRecord>,
    stock_rows: Vec<StockRecord>,
    outbound_rows: Vec<RowRecord>,
}

#[derive(Debug, Serialize)]
struct Snapshot {
    meta: SnapshotMeta,
    parts: BTreeMap<String, PartBlock>,
    raw: SnapshotRaw,
}

#[derive(Debug)]
enum ProcessOutcome {
    Processed,
    Idle { reason: String },
}

#[derive(Debug)]
enum DetectResult {
    Ready(BatchSelection),
    MissingKinds(Vec<DocKind>),
    UnstableFiles(Vec<PathBuf>),
}

#[derive(Debug, Serialize)]
struct BatchReport {
    processed_at: String,
    status: String,
    snapshot_date: Option<String>,
    inbound_file: String,
    stock_file: String,
    outbound_file: String,
    outputs: Option<BatchReportOutputs>,
    converted_parts: Option<usize>,
    error: Option<String>,
}

#[derive(Debug, Serialize)]
struct BatchReportOutputs {
    out_json: String,
    docx_smoke_test: Option<String>,
    docx_output_dir: Option<String>,
    docx_generated: Option<usize>,
}

#[derive(Debug, Clone)]
struct DocumentRow {
    part_key: String,
    part_no: String,
    part_name: String,
    received_date: String,
    used_date_last: String,
    used_where: String,
    usage_reason: String,
    replacement_reason: String,
    current_stock_before: f64,
    outbound_qty_sum: f64,
    current_stock_updated: f64,
    purchase_order_note: String,
    equipment_no: String,
    model_name: String,
    issued_date: String,
    issued_qty: String,
    vendor_name: String,
    unit: String,
    unit_price: String,
}

#[derive(Clone)]
struct TemplateEntry {
    name: String,
    is_dir: bool,
    compression: CompressionMethod,
    data: Vec<u8>,
}

#[derive(Clone)]
struct TemplatePackage {
    entries: Vec<TemplateEntry>,
}

#[derive(Debug, Clone, Serialize)]
struct RowRecord {
    columns: HashMap<String, String>,
    part_no: Option<String>,
    part_name: Option<String>,
    qty: Option<f64>,
    date: Option<String>,
    date_iso: Option<String>,
    source_file: String,
}

#[derive(Debug, Clone, Serialize)]
struct StockRecord {
    columns: HashMap<String, String>,
    part_no: Option<String>,
    part_name: Option<String>,
    stock_qty: Option<f64>,
    source_file: String,
}

#[derive(Debug, Clone, Serialize)]
struct PartBlock {
    part_no: Option<String>,
    part_name: String,
    current_stock_before: Option<f64>,
    inbound_plus_stock: Option<f64>,
    current_stock_updated: Option<f64>,
    inbound_count: usize,
    outbound_count: usize,
    inbound_dates_raw: Vec<String>,
    outbound_dates_raw: Vec<String>,
    inbound_dates: Vec<String>,
    outbound_dates: Vec<String>,
    inbound_qty_sum: f64,
    outbound_qty_sum: f64,
    stock_row_idx: Vec<usize>,
    inbound_row_idx: Vec<usize>,
    outbound_row_idx: Vec<usize>,
}

fn main() -> Result<()> {
    init_rayon_pool();
    let config = AppConfig::from_args()?;
    if let Some(legacy) = &config.legacy {
        run_legacy_mode(legacy)?;
        return Ok(());
    }

    let workspace = setup_workspace(&config.workdir)?;

    println!("[translator_for_docu_v2] workdir: {}", workspace.root.display());
    println!("watch input: {}", workspace.input_dir.display());
    println!("output dir: {}", workspace.output.display());
    println!("logs dir: {}", workspace.logs.display());

    write_log(
        &workspace,
        &format!(
            "startup: input='{}', output='{}', logs='{}', smoke={}",
            workspace.input_dir.display(),
            workspace.output.display(),
            workspace.logs.display(),
            config.smoke
        ),
    );

    match process_available_batch(&workspace, config.smoke)? {
        ProcessOutcome::Processed => {}
        ProcessOutcome::Idle { reason } => write_log(&workspace, &reason),
    }

    if config.once {
        println!("once mode done");
        return Ok(());
    }

    // -----------------------------------------------------------------
    // Daily scheduler skeleton (disabled by comment on purpose)
    //
    // If you want to run this pipeline once every day at 05:00 local time,
    // replace `watch_loop(&workspace)` below with:
    //
    //   // schedule_daily_5am_loop(&workspace)?;
    //
    // and enable the function implementation in the commented block near
    // `watch_loop`.
    // -----------------------------------------------------------------
    watch_loop(&workspace, config.smoke)
}

impl AppConfig {
    fn from_args() -> Result<Self> {
        let args: Vec<String> = env::args().collect();
        let mut workdir = PathBuf::from("./DB");
        let mut once = false;
        let mut smoke = false;
        let mut workdir_explicit = false;
        let mut positional = Vec::new();

        let mut i = 1;
        while i < args.len() {
            match args[i].as_str() {
                "--workdir" => {
                    let next = args
                        .get(i + 1)
                        .context("--workdir requires a path")?;
                    workdir = PathBuf::from(next);
                    workdir_explicit = true;
                    i += 2;
                }
                "--once" => {
                    once = true;
                    i += 1;
                }
                "--smoke" => {
                    smoke = true;
                    i += 1;
                }
                _ => {
                    positional.push(args[i].clone());
                    i += 1;
                }
            }
        }

        let legacy = if positional.len() >= 3 {
            if workdir_explicit {
                bail!("legacy file args and --workdir cannot be used together");
            }
            let inbound = PathBuf::from(&positional[0]);
            let stock = PathBuf::from(&positional[1]);
            let outbound = PathBuf::from(&positional[2]);
            let out_json = positional
                .get(3)
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("stock_in_out_monthly.json"));
            Some(LegacyArgs {
                inbound,
                stock,
                outbound,
                out_json,
            })
        } else {
            if positional.len() == 1 {
                workdir = PathBuf::from(&positional[0]);
            } else if positional.len() > 1 {
                bail!("invalid args: use workdir(1 arg) or legacy files(3+ args)");
            }
            None
        };

        Ok(Self {
            workdir,
            once,
            smoke,
            legacy,
        })
    }
}

fn setup_workspace(root: &Path) -> Result<Workspace> {
    let root = root
        .canonicalize()
        .or_else(|_| {
            fs::create_dir_all(root)?;
            root.canonicalize()
        })
        .with_context(|| format!("workdir init failed: {}", root.display()))?;

    let input_dir = root.join("input");
    let output = root.join("output");
    let logs = root.join("logs");

    let ws = Workspace {
        root: root.clone(),
        input_dir,
        output,
        reports: logs.clone(),
        logs,
    };

    for dir in [&ws.input_dir, &ws.output, &ws.logs, &ws.reports] {
        fs::create_dir_all(dir).with_context(|| format!("create dir failed: {}", dir.display()))?;
    }

    Ok(ws)
}

fn watch_loop(workspace: &Workspace, smoke_enabled: bool) -> Result<()> {
    let mut last_idle_reason: Option<String> = None;
    loop {
        match process_available_batch(workspace, smoke_enabled) {
            Ok(ProcessOutcome::Processed) => {
                last_idle_reason = None;
            }
            Ok(ProcessOutcome::Idle { reason }) => {
                if last_idle_reason.as_deref() != Some(reason.as_str()) {
                    write_log(workspace, &reason);
                    last_idle_reason = Some(reason);
                }
            }
            Err(err) => {
                write_log(workspace, &format!("poll processing error: {err:#}"));
            }
        }
        std::thread::sleep(Duration::from_secs(2));
    }
}

fn process_available_batch(workspace: &Workspace, smoke_enabled: bool) -> Result<ProcessOutcome> {
    let batch = match detect_ready_batch(&workspace.input_dir)? {
        DetectResult::Ready(b) => b,
        DetectResult::MissingKinds(kinds) => {
            return Ok(ProcessOutcome::Idle {
                reason: format!("waiting: missing files [{}]", join_doc_kinds(&kinds)),
            });
        }
        DetectResult::UnstableFiles(paths) => {
            return Ok(ProcessOutcome::Idle {
                reason: format!("waiting: files still copying [{}]", join_paths(&paths)),
            });
        }
    };

    let fingerprint = compute_batch_fingerprint(&batch.inbound, &batch.stock, &batch.outbound)?;
    if is_same_as_last_processed(workspace, &fingerprint)? {
        return Ok(ProcessOutcome::Idle {
            reason: "waiting: no new batch changes".to_string(),
        });
    }

    println!(
        "batch detected: {}, {}, {}",
        batch.inbound.display(),
        batch.stock.display(),
        batch.outbound.display()
    );

    let stamp = Local::now().format("%Y%m%d_%H%M%S").to_string();

    write_log(
        workspace,
        &format!(
            "batch start: inbound='{}', stock='{}', outbound='{}'",
            batch.inbound.display(),
            batch.stock.display(),
            batch.outbound.display()
        ),
    );

    let mut report = BatchReport {
        processed_at: Local::now().format("%Y-%m-%d %H:%M:%S").to_string(),
        status: "failed".to_string(),
        snapshot_date: None,
        inbound_file: path_relative_to(&workspace.root, &batch.inbound),
        stock_file: path_relative_to(&workspace.root, &batch.stock),
        outbound_file: path_relative_to(&workspace.root, &batch.outbound),
        outputs: None,
        converted_parts: None,
        error: None,
    };

    match run_batch_pipeline(
        workspace,
        &batch.inbound,
        &batch.stock,
        &batch.outbound,
        smoke_enabled,
    ) {
        Ok(artifacts) => {
            report.snapshot_date = Some(artifacts.snapshot_date.clone());
            report.converted_parts = Some(artifacts.converted_parts);
            report.outputs = Some(BatchReportOutputs {
                out_json: path_relative_to(&workspace.root, &artifacts.out_json),
                docx_smoke_test: artifacts
                    .smoke_txt
                    .as_ref()
                    .map(|p| path_relative_to(&workspace.root, p)),
                docx_output_dir: artifacts
                    .docx_outdir
                    .as_ref()
                    .map(|p| path_relative_to(&workspace.root, p)),
                docx_generated: Some(artifacts.docx_generated),
            });

            report.status = "success".to_string();
            report.error = None;
            write_last_processed_fingerprint(workspace, &fingerprint)?;

            write_log(
                workspace,
                &format!(
                    "batch success: processed files [{}]",
                    join_paths(&[batch.inbound.clone(), batch.stock.clone(), batch.outbound.clone()])
                ),
            );
        }
        Err(err) => {
            report.error = Some(format!("{err:#}"));
            write_log(workspace, &format!("batch failed: {err:#}"));
        }
    }

    match write_batch_report(workspace, &stamp, &report) {
        Ok(path) => write_log(
            workspace,
            &format!("batch report saved: {}", path_relative_to(&workspace.root, &path)),
        ),
        Err(err) => write_log(workspace, &format!("batch report write failed: {err:#}")),
    }

    Ok(ProcessOutcome::Processed)
}

fn detect_ready_batch(source_dir: &Path) -> Result<DetectResult> {
    let files = collect_excel_candidates(source_dir)?;

    let mut latest: HashMap<DocKind, CandidateFile> = HashMap::new();
    for item in files {
        latest
            .entry(item.kind)
            .and_modify(|prev| {
                if item.modified > prev.modified {
                    *prev = item.clone();
                }
            })
            .or_insert(item);
    }

    let mut missing = Vec::new();
    if !latest.contains_key(&DocKind::Inbound) {
        missing.push(DocKind::Inbound);
    }
    if !latest.contains_key(&DocKind::Stock) {
        missing.push(DocKind::Stock);
    }
    if !latest.contains_key(&DocKind::Outbound) {
        missing.push(DocKind::Outbound);
    }
    if !missing.is_empty() {
        return Ok(DetectResult::MissingKinds(missing));
    }

    let inbound = latest
        .get(&DocKind::Inbound)
        .map(|v| v.path.clone())
        .context("inbound missing unexpectedly")?;
    let stock = latest
        .get(&DocKind::Stock)
        .map(|v| v.path.clone())
        .context("stock missing unexpectedly")?;
    let outbound = latest
        .get(&DocKind::Outbound)
        .map(|v| v.path.clone())
        .context("outbound missing unexpectedly")?;

    let unstable = detect_unstable_files(&[&inbound, &stock, &outbound], Duration::from_secs(1))?;
    if !unstable.is_empty() {
        return Ok(DetectResult::UnstableFiles(unstable));
    }

    Ok(DetectResult::Ready(BatchSelection {
        inbound,
        stock,
        outbound,
    }))
}

fn collect_excel_candidates(source_dir: &Path) -> Result<Vec<CandidateFile>> {
    let mut out = Vec::new();
    for entry in fs::read_dir(source_dir)
        .with_context(|| format!("read_dir failed: {}", source_dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() || !is_excel_file(&path) {
            continue;
        }

        let kind = match detect_kind_from_filename(&path) {
            Some(k) => k,
            None => continue,
        };
        let modified = fs::metadata(&path)?.modified().unwrap_or(SystemTime::UNIX_EPOCH);
        out.push(CandidateFile {
            kind,
            path,
            modified,
        });
    }
    Ok(out)
}

fn is_excel_file(path: &Path) -> bool {
    path.extension()
        .and_then(|v| v.to_str())
        .map(|ext| matches!(ext.to_ascii_lowercase().as_str(), "xlsx" | "xlsm" | "xls"))
        .unwrap_or(false)
}

fn detect_kind_from_filename(path: &Path) -> Option<DocKind> {
    let raw = path.file_name()?.to_string_lossy().to_string();
    let name = raw.to_ascii_lowercase();

    // Prefer explicit filename markers/keywords before loose number matching.
    if raw.contains("첨부파일4") || raw.contains("출고") || name.contains("outbound") {
        return Some(DocKind::Outbound);
    }
    if raw.contains("첨부파일3") || raw.contains("재고") || name.contains("stock") {
        return Some(DocKind::Stock);
    }
    if raw.contains("첨부파일2") || raw.contains("입고") || name.contains("inbound") {
        return Some(DocKind::Inbound);
    }

    // Fallback for numeric-only naming.
    if name.contains("4") {
        return Some(DocKind::Outbound);
    }
    if name.contains("3") {
        return Some(DocKind::Stock);
    }
    if name.contains("2") {
        return Some(DocKind::Inbound);
    }
    None
}

#[derive(Debug, Clone, Copy)]
struct FileState {
    size: u64,
    modified: SystemTime,
}

fn file_state(path: &Path) -> Result<FileState> {
    let meta = fs::metadata(path)?;
    Ok(FileState {
        size: meta.len(),
        modified: meta.modified().unwrap_or(SystemTime::UNIX_EPOCH),
    })
}

fn detect_unstable_files(paths: &[&Path], wait: Duration) -> Result<Vec<PathBuf>> {
    let before = paths
        .iter()
        .map(|p| file_state(p).map(|s| (p.to_path_buf(), s)))
        .collect::<Result<Vec<_>>>()?;

    std::thread::sleep(wait);

    let after = paths
        .iter()
        .map(|p| file_state(p).map(|s| (p.to_path_buf(), s)))
        .collect::<Result<Vec<_>>>()?;

    let mut unstable = Vec::new();
    for ((p1, b), (_, a)) in before.iter().zip(after.iter()) {
        if b.size != a.size || b.modified != a.modified {
            unstable.push(p1.clone());
        }
    }
    Ok(unstable)
}

fn run_batch_pipeline(
    workspace: &Workspace,
    inbound_path: &Path,
    stock_path: &Path,
    outbound_path: &Path,
    smoke_enabled: bool,
) -> Result<BatchArtifacts> {
    let snapshot = build_snapshot(inbound_path, stock_path, outbound_path)?;
    let out_json = workspace.output.join("stock_in_out_monthly.json");
    write_snapshot_json(&out_json, &snapshot)?;
    let smoke_txt = if smoke_enabled {
        Some(write_docx_smoke_test(workspace, &snapshot)?)
    } else {
        None
    };
    let (docx_outdir, docx_generated) = generate_docx_from_snapshot(workspace, &snapshot)?;

    Ok(BatchArtifacts {
        out_json,
        snapshot_date: snapshot.meta.snapshot_date,
        converted_parts: snapshot.parts.len(),
        smoke_txt,
        docx_outdir,
        docx_generated,
    })
}

/*
fn schedule_daily_5am_loop(workspace: &Workspace) -> Result<()> {
    // Runs one batch at 05:00 local time every day.
    // It checks every 30 seconds and prevents duplicate execution per date.
    let mut last_run_date: Option<String> = None;

    loop {
        let now = Local::now();
        let today = now.format("%Y-%m-%d").to_string();
        let hour = now.hour();
        let minute = now.minute();

        if hour == 5 && minute == 0 && last_run_date.as_deref() != Some(today.as_str()) {
            match process_available_batch(workspace) {
                Ok(ProcessOutcome::Processed) => {
                    write_log(workspace, "scheduled run(05:00): processed");
                }
                Ok(ProcessOutcome::Idle { reason }) => {
                    write_log(workspace, &format!("scheduled run(05:00): idle ({reason})"));
                }
                Err(err) => {
                    write_log(workspace, &format!("scheduled run(05:00) failed: {err:#}"));
                }
            }
            last_run_date = Some(today);
        }

        std::thread::sleep(Duration::from_secs(30));
    }
}
*/

fn init_rayon_pool() {
    let threads = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    let _ = rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
        .build_global();
}

fn build_snapshot(inbound_path: &Path, stock_path: &Path, outbound_path: &Path) -> Result<Snapshot> {
    let inbound_rows = read_inbound(inbound_path)?;
    let snapshot_date = extract_ymd_from_filename(stock_path)
        .unwrap_or_else(|| Local::now().format("%Y-%m-%d").to_string());
    let stock_rows = read_stock(stock_path)?;
    let outbound_rows = read_outbound(outbound_path)?;

    let mut by_key: BTreeMap<String, PartBlock> = BTreeMap::new();

    for (idx, s) in stock_rows.iter().enumerate() {
        let key = part_key(s.part_no.as_deref(), s.part_name.as_deref());
        let part_name = resolve_part_name(s.part_name.as_deref(), s.part_no.as_deref());

        by_key
            .entry(key.clone())
            .or_insert_with(|| init_part_block(s.part_no.clone(), part_name, s.stock_qty));
        if let Some(entry) = by_key.get_mut(&key) {
            entry.stock_row_idx.push(idx);
            if entry.current_stock_before.is_none() {
                entry.current_stock_before = s.stock_qty;
            }
        }
    }

    for (idx, r) in inbound_rows.iter().enumerate() {
        let key = part_key(r.part_no.as_deref(), r.part_name.as_deref());
        let part_name = resolve_part_name(r.part_name.as_deref(), r.part_no.as_deref());
        let entry = by_key
            .entry(key.clone())
            .or_insert_with(|| init_part_block(r.part_no.clone(), part_name, None));

        entry.inbound_count += 1;
        entry.inbound_qty_sum += r.qty.unwrap_or(0.0);
        if let Some(d_raw) = r.date.clone() {
            entry.inbound_dates_raw.push(d_raw.clone());
            entry
                .inbound_dates
                .push(r.date_iso.clone().unwrap_or_else(|| d_raw.clone()));
        }
        if entry.part_no.is_none() {
            entry.part_no = r.part_no.clone();
        }
        entry.inbound_row_idx.push(idx);
    }

    for (idx, r) in outbound_rows.iter().enumerate() {
        let key = part_key(r.part_no.as_deref(), r.part_name.as_deref());
        let part_name = resolve_part_name(r.part_name.as_deref(), r.part_no.as_deref());
        let entry = by_key
            .entry(key.clone())
            .or_insert_with(|| init_part_block(r.part_no.clone(), part_name, None));

        entry.outbound_count += 1;
        entry.outbound_qty_sum += r.qty.unwrap_or(0.0);
        if let Some(d_raw) = r.date.clone() {
            entry.outbound_dates_raw.push(d_raw.clone());
            entry
                .outbound_dates
                .push(r.date_iso.clone().unwrap_or_else(|| d_raw.clone()));
        }
        if entry.part_no.is_none() {
            entry.part_no = r.part_no.clone();
        }
        entry.outbound_row_idx.push(idx);
    }

    for block in by_key.values_mut() {
        block.inbound_dates_raw.sort();
        block.inbound_dates_raw.dedup();
        block.outbound_dates_raw.sort();
        block.outbound_dates_raw.dedup();
        block.inbound_dates.sort();
        block.inbound_dates.dedup();
        block.outbound_dates.sort();
        block.outbound_dates.dedup();

        // 1) current stock column raise
        let current_before = block.current_stock_before.unwrap_or(0.0);
        // 2) add inbound + stock
        let inbound_plus_stock = current_before + block.inbound_qty_sum;
        // 3) update current stock
        let updated = inbound_plus_stock - block.outbound_qty_sum;

        block.inbound_plus_stock = Some(inbound_plus_stock);
        block.current_stock_updated = Some(updated);
    }

    Ok(Snapshot {
        meta: SnapshotMeta {
            generated_at: Local::now().to_rfc3339(),
            snapshot_date,
            counts: SnapshotMetaCounts {
                parts: by_key.len(),
                inbound_rows: inbound_rows.len(),
                stock_rows: stock_rows.len(),
                outbound_rows: outbound_rows.len(),
            },
            storage: SnapshotStorage {
                raw_rows: "kept_once".to_string(),
                part_rows: "index_reference_only".to_string(),
            },
            calc_order: vec![
                "1.current_stock_before".to_string(),
                "2.inbound_plus_stock = current_stock_before + inbound_qty_sum".to_string(),
                "3.current_stock_updated = inbound_plus_stock - outbound_qty_sum".to_string(),
            ],
        },
        parts: by_key,
        raw: SnapshotRaw {
            inbound_rows,
            stock_rows,
            outbound_rows,
        },
    })
}

fn init_part_block(part_no: Option<String>, part_name: String, current_stock_before: Option<f64>) -> PartBlock {
    PartBlock {
        part_no,
        part_name,
        current_stock_before,
        inbound_plus_stock: None,
        current_stock_updated: None,
        inbound_count: 0,
        outbound_count: 0,
        inbound_dates_raw: Vec::new(),
        outbound_dates_raw: Vec::new(),
        inbound_dates: Vec::new(),
        outbound_dates: Vec::new(),
        inbound_qty_sum: 0.0,
        outbound_qty_sum: 0.0,
        stock_row_idx: Vec::new(),
        inbound_row_idx: Vec::new(),
        outbound_row_idx: Vec::new(),
    }
}

fn write_snapshot_json(out_json: &Path, snapshot: &Snapshot) -> Result<()> {
    let f = BufWriter::new(File::create(out_json).context("create out_json failed")?);
    serde_json::to_writer_pretty(f, snapshot).context("write json failed")
}

fn write_docx_smoke_test(workspace: &Workspace, snapshot: &Snapshot) -> Result<PathBuf> {
    let data = &snapshot.parts;

    let test_path = workspace.output.join(format!(
        "docx_smoke_test_{}.txt",
        Local::now().format("%Y%m%d_%H%M%S")
    ));
    let mut w = BufWriter::new(
        File::create(&test_path)
            .with_context(|| format!("create smoke test file failed: {}", test_path.display()))?,
    );

    writeln!(w, "DOCX Smoke Test (Text Prototype)")?;
    writeln!(w, "parts_count: {}", data.len())?;
    writeln!(w)?;

    for (name, block) in data {
        let part_no = block
            .part_no
            .clone()
            .unwrap_or_else(|| "(manual input)".to_string());
        let stock_qty = block
            .current_stock_before
            .map(|n| n.to_string())
            .unwrap_or_else(|| "(manual input)".to_string());
        let updated_qty = block
            .current_stock_updated
            .map(|n| n.to_string())
            .unwrap_or_else(|| "(manual input)".to_string());

        writeln!(w, "## {}", name)?;
        writeln!(w, "- part_no: {}", part_no)?;
        writeln!(w, "- current_stock_before: {}", stock_qty)?;
        writeln!(w, "- current_stock_updated: {}", updated_qty)?;
        writeln!(w)?;
    }

    w.flush()?;
    Ok(test_path)
}

fn generate_docx_from_snapshot(
    workspace: &Workspace,
    snapshot: &Snapshot,
) -> Result<(Option<PathBuf>, usize)> {
    let Some(template) = resolve_docx_template(workspace)? else {
        write_log(workspace, "docx template not found; skipping docx generation");
        return Ok((None, 0));
    };
    let template_pkg = Arc::new(load_template_package(&template)?);

    let rows = build_document_rows(snapshot, true, None);
    let outdir = workspace
        .output
        .join(Local::now().format("%Y-%m-%d").to_string());
    fs::create_dir_all(&outdir)
        .with_context(|| format!("create docx outdir failed: {}", outdir.display()))?;

    let targets = build_unique_docx_targets(&outdir, &rows);
    let generated = AtomicUsize::new(0);

    rows.par_iter()
        .zip(targets.par_iter())
        .enumerate()
        .try_for_each(|(idx, (row, output))| -> Result<()> {
            render_docx_from_package(&template_pkg, output, row, idx + 1)?;
            generated.fetch_add(1, Ordering::Relaxed);
            Ok(())
        })?;

    write_log(
        workspace,
        &format!(
            "docx generated: count={}, outdir='{}', template='{}'",
            generated.load(Ordering::Relaxed),
            outdir.display(),
            template.display()
        ),
    );

    Ok((Some(outdir), generated.load(Ordering::Relaxed)))
}

fn resolve_docx_template(workspace: &Workspace) -> Result<Option<PathBuf>> {
    let mut candidates: Vec<PathBuf> = fs::read_dir(&workspace.input_dir)?
        .filter_map(|e| e.ok().map(|v| v.path()))
        .filter(|p| {
            p.extension()
                .and_then(|e| e.to_str())
                .map(|e| e.eq_ignore_ascii_case("docx"))
                .unwrap_or(false)
        })
        .collect();

    candidates.sort_by(|a, b| {
        let ma = fs::metadata(a)
            .and_then(|m| m.modified())
            .unwrap_or(SystemTime::UNIX_EPOCH);
        let mb = fs::metadata(b)
            .and_then(|m| m.modified())
            .unwrap_or(SystemTime::UNIX_EPOCH);
        mb.cmp(&ma)
    });

    if let Some(p) = candidates.first() {
        return Ok(Some(p.clone()));
    }

    Ok(None)
}

fn build_document_rows(snapshot: &Snapshot, include_no_outbound: bool, limit: Option<usize>) -> Vec<DocumentRow> {
    let mut rows = Vec::new();

    for (part_key, part) in &snapshot.parts {
        if !include_no_outbound && part.outbound_count == 0 {
            continue;
        }

        let part_no = fallback_missing_doc(part.part_no.clone());
        let part_name = fallback_missing_doc(Some(part.part_name.clone()));
        let received_date = part
            .inbound_dates
            .first()
            .cloned()
            .unwrap_or_else(|| "입고기록없음".to_string());

        let mut used_date_last = part
            .outbound_dates
            .last()
            .cloned()
            .unwrap_or_else(|| "출고기록없음".to_string());
        let mut used_where = "출고기록없음".to_string();
        let mut usage_reason = "출고기록없음".to_string();
        let mut replacement_reason = "출고기록없음".to_string();
        let mut equipment_no = "기록없음".to_string();
        let mut model_name = "기록없음".to_string();
        let mut issued_date = used_date_last.clone();
        let mut issued_qty = format!("{:.0}", part.outbound_qty_sum);
        let mut unit = "기록없음".to_string();
        let mut vendor_name = "기록없음".to_string();
        let mut unit_price = "기록없음".to_string();

        if let Some(out_idx) = part.outbound_row_idx.last() {
            if let Some(row) = snapshot.raw.outbound_rows.get(*out_idx) {
                used_where = pick_first_col(
                    &row.columns,
                    &["장비명", "장비번호", "주요장비명", "주요Model명"],
                );
                usage_reason = pick_first_col(&row.columns, &["운영구분", "지급구분", "요청번호"]);
                replacement_reason = pick_first_col(&row.columns, &["지급구분", "운영구분", "요청번호"]);
                equipment_no = pick_first_col(&row.columns, &["장비번호"]);
                model_name = pick_first_col(&row.columns, &["Model명"]);
                issued_qty = pick_first_col(&row.columns, &["지급량"]);
                issued_date = row
                    .date_iso
                    .clone()
                    .unwrap_or_else(|| pick_first_col(&row.columns, &["지급일자"]));
                unit = pick_first_col(&row.columns, &["단위"]);
                unit_price = pick_first_col(&row.columns, &["단가", "구단가"]);
                if let Some(d) = row.date_iso.clone() {
                    used_date_last = d;
                }
            }
        }

        if let Some(in_idx) = part.inbound_row_idx.first() {
            if let Some(in_row) = snapshot.raw.inbound_rows.get(*in_idx) {
                let v = pick_first_col(&in_row.columns, &["납품업체"]);
                if v != "기록없음" {
                    vendor_name = v;
                }
                if unit == "기록없음" {
                    unit = pick_first_col(&in_row.columns, &["단위"]);
                }
                if unit_price == "기록없음" {
                    unit_price = pick_first_col(&in_row.columns, &["단가", "구단가"]);
                }
            }
        }

        let current_stock_before = part.current_stock_before.unwrap_or(0.0);
        let current_stock_updated = part.current_stock_updated.unwrap_or(0.0);
        let purchase_order_needed = current_stock_updated <= 0.0;
        let purchase_order_note = if purchase_order_needed {
            "재고 부족 가능성 확인 후 구매발주 검토".to_string()
        } else {
            "현재 재고 유지".to_string()
        };

        rows.push(DocumentRow {
            part_key: part_key.clone(),
            part_no,
            part_name,
            received_date,
            used_date_last,
            used_where,
            usage_reason,
            replacement_reason,
            current_stock_before,
            outbound_qty_sum: part.outbound_qty_sum,
            current_stock_updated,
            purchase_order_note,
            equipment_no,
            model_name,
            issued_date,
            issued_qty,
            vendor_name,
            unit,
            unit_price,
        });
    }

    rows.sort_by(|a, b| a.part_key.cmp(&b.part_key));
    if let Some(n) = limit {
        rows.truncate(n);
    }
    rows
}

fn pick_first_col(columns: &HashMap<String, String>, keys: &[&str]) -> String {
    for key in keys {
        if let Some(v) = columns.get(*key) {
            let t = v.trim();
            if !t.is_empty() {
                return t.to_string();
            }
        }
    }
    "기록없음".to_string()
}

fn fallback_missing_doc(v: Option<String>) -> String {
    v.map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "기록없음".to_string())
}

fn load_template_package(template: &Path) -> Result<TemplatePackage> {
    let tf = File::open(template).with_context(|| format!("open template failed: {}", template.display()))?;
    let mut zin = ZipArchive::new(tf).context("open template zip failed")?;
    let mut entries = Vec::with_capacity(zin.len());

    for i in 0..zin.len() {
        let mut entry = zin.by_index(i)?;
        let name = entry.name().to_string();
        let is_dir = entry.is_dir();
        let compression = entry.compression();
        let mut data = Vec::new();
        if !is_dir {
            entry.read_to_end(&mut data)?;
        }
        entries.push(TemplateEntry {
            name,
            is_dir,
            compression,
            data,
        });
    }

    Ok(TemplatePackage { entries })
}

fn render_docx_from_package(
    template_pkg: &TemplatePackage,
    output: &Path,
    row: &DocumentRow,
    serial: usize,
) -> Result<()> {
    let of = File::create(output).with_context(|| format!("create output failed: {}", output.display()))?;
    let mut zout = ZipWriter::new(of);

    for entry in &template_pkg.entries {
        let options = SimpleFileOptions::default()
            .compression_method(entry.compression)
            .unix_permissions(0o644);

        if entry.is_dir {
            zout.add_directory(entry.name.clone(), options)?;
            continue;
        }

        if entry.name == "word/document.xml" {
            let xml = String::from_utf8(entry.data.clone()).context("document.xml is not utf8")?;
            let patched = patch_document_xml_docx(&xml, row, serial);
            zout.start_file(
                entry.name.clone(),
                options.compression_method(CompressionMethod::Deflated),
            )?;
            zout.write_all(patched.as_bytes())?;
        } else {
            zout.start_file(entry.name.clone(), options)?;
            zout.write_all(&entry.data)?;
        }
    }

    zout.finish()?;
    Ok(())
}

fn patch_document_xml_docx(xml: &str, row: &DocumentRow, serial: usize) -> String {
    let values = build_docx_values(row, serial);
    patch_paragraph_text_runs_docx(xml, &values)
}

fn patch_paragraph_text_runs_docx(xml: &str, values: &BTreeMap<&'static str, String>) -> String {
    let mut out = String::with_capacity(xml.len() + 1024);
    let mut cursor = 0usize;

    while let Some(p_start_rel) = xml[cursor..].find("<w:p") {
        let p_start = cursor + p_start_rel;
        out.push_str(&xml[cursor..p_start]);

        let Some(p_end_rel) = xml[p_start..].find("</w:p>") else {
            out.push_str(&xml[p_start..]);
            return out;
        };
        let p_end = p_start + p_end_rel + "</w:p>".len();
        let paragraph = &xml[p_start..p_end];
        out.push_str(&patch_one_paragraph_docx(paragraph, values));
        cursor = p_end;
    }

    out.push_str(&xml[cursor..]);
    out
}

fn patch_one_paragraph_docx(p_xml: &str, values: &BTreeMap<&'static str, String>) -> String {
    let slots = find_text_slots_docx(p_xml);
    if slots.is_empty() {
        return p_xml.to_string();
    }

    let mut plain = String::new();
    for (s, e) in &slots {
        plain.push_str(&xml_unescape_docx(&p_xml[*s..*e]));
    }

    let replaced = replace_tokens_in_text_docx(&plain, values);
    if replaced == plain {
        return p_xml.to_string();
    }

    let mut out = String::with_capacity(p_xml.len() + 64);
    let mut last = 0usize;
    for (idx, (s, e)) in slots.iter().enumerate() {
        out.push_str(&p_xml[last..*s]);
        if idx == 0 {
            out.push_str(&xml_escape_docx(&replaced));
        }
        last = *e;
    }
    out.push_str(&p_xml[last..]);
    reduce_font_size_tags_docx(&out)
}

fn find_text_slots_docx(xml: &str) -> Vec<(usize, usize)> {
    let mut slots = Vec::new();
    let mut cursor = 0usize;

    while let Some(t_start_rel) = xml[cursor..].find("<w:t") {
        let t_start = cursor + t_start_rel;
        let Some(gt_rel) = xml[t_start..].find('>') else {
            break;
        };
        let content_start = t_start + gt_rel + 1;
        let Some(end_rel) = xml[content_start..].find("</w:t>") else {
            break;
        };
        let content_end = content_start + end_rel;
        slots.push((content_start, content_end));
        cursor = content_end + "</w:t>".len();
    }
    slots
}

fn replace_tokens_in_text_docx(text: &str, values: &BTreeMap<&'static str, String>) -> String {
    let mut out = String::with_capacity(text.len() + 64);
    let mut cursor = 0usize;
    while let Some(start_rel) = text[cursor..].find("{{") {
        let start = cursor + start_rel;
        out.push_str(&text[cursor..start]);
        let body_start = start + 2;
        if let Some(end_rel) = text[body_start..].find("}}") {
            let body_end = body_start + end_rel;
            let key = text[body_start..body_end].trim();
            let value = values
                .get(key)
                .cloned()
                .unwrap_or_else(|| "(직접입력)".to_string());
            out.push_str(&value);
            cursor = body_end + 2;
        } else {
            out.push_str(&text[start..]);
            return out;
        }
    }
    out.push_str(&text[cursor..]);
    out
}

fn build_docx_values(row: &DocumentRow, serial: usize) -> BTreeMap<&'static str, String> {
    let today = Local::now().format("%Y-%m-%d").to_string();
    let issued_date = if row.issued_date.trim().is_empty() {
        row.used_date_last.clone()
    } else {
        row.issued_date.clone()
    };
    let model = if row.model_name.trim().is_empty() {
        "(직접입력)".to_string()
    } else {
        row.model_name.clone()
    };
    let vendor = if row.vendor_name.trim().is_empty() {
        "(직접입력)".to_string()
    } else {
        row.vendor_name.clone()
    };
    let unit = if row.unit.trim().is_empty() {
        "(직접입력)".to_string()
    } else {
        row.unit.clone()
    };
    let purchase_qty = if row.current_stock_updated <= 0.0 {
        format!("{:.0}", row.outbound_qty_sum.max(1.0))
    } else {
        "0".to_string()
    };
    let replacement_qty = format!("{:.0}", row.outbound_qty_sum);

    let mut m = BTreeMap::new();
    m.insert("번호", row.part_no.clone());
    m.insert("문서번호", format!("DOC-{}-{:04}", Local::now().format("%Y%m%d"), serial));
    m.insert("작성일자", today.clone());
    m.insert("제목", format!("부품 구매 요청 - {} ({})", row.part_name, row.part_no));
    m.insert("품목", row.part_name.clone());
    m.insert("현재고", format!("{:.0}", row.current_stock_before));
    m.insert("구매량", purchase_qty);
    m.insert("교체수량1", replacement_qty.clone());
    m.insert("교체수량2", replacement_qty);
    m.insert("날짜1", issued_date.clone());
    m.insert("날짜2", issued_date);
    m.insert("대상장비", row.used_where.clone());
    m.insert("호기1", row.equipment_no.clone());
    m.insert("호기2", model);
    m.insert("부품-장착-수량", row.issued_qty.clone());
    m.insert("단위", unit);
    m.insert("사유", row.replacement_reason.clone());
    m.insert("비고", row.purchase_order_note.clone());
    m.insert("구-거래처", vendor);

    m.insert("관련사진1", "(직접기입)".to_string());
    m.insert("관련사진2", "(직접기입)".to_string());
    m.insert("1번설명", "(직접입력)".to_string());
    m.insert("2번설명", "(직접입력)".to_string());
    m.insert("부품-원리-및-역할", "(직접입력)".to_string());
    m.insert("ctc-승인시간", "(직접입력)".to_string());
    m.insert("ctc-승인여부", "(직접입력)".to_string());
    m.insert("moz-승인시간", "(직접입력)".to_string());
    m.insert("moz-승인여부", "(직접입력)".to_string());
    m.insert("합의여부", "(직접입력)".to_string());
    m.insert("성함1", "(직접입력)".to_string());
    m.insert("성함2", "(직접입력)".to_string());
    m.insert("직책1", "(직접입력)".to_string());
    m.insert("직책2", "(직접입력)".to_string());
    m.insert("발신부서", "(직접입력)".to_string());
    m.insert("현황-및-문제점-1)", "(직접입력)".to_string());
    m.insert("현황-및-문제점-2", "(직접입력)".to_string());
    m.insert("이후-진행사항", "(직접입력)".to_string());
    m.insert("납기기간", "(직접입력)".to_string());
    m.insert("비밀여부", "(직접입력)".to_string());
    m.insert("첨부파일", "(직접입력)".to_string());
    m.insert("새-거래처", "(직접입력)".to_string());
    m.insert("신단가", "(직접입력)".to_string());
    m.insert("공급액", "(직접입력)".to_string());
    m.insert("합계", "(직접입력)".to_string());
    m.insert("구단가", format_price_docx(&row.unit_price));
    m.insert("지급조건", "(직접입력)".to_string());
    m.insert("사용일", row.used_date_last.clone());
    m.insert("입고일", row.received_date.clone());
    m.insert("사용처", row.used_where.clone());
    m.insert("문제점", row.usage_reason.clone());
    m.insert("파트키", row.part_key.clone());
    m
}

fn xml_escape_docx(v: &str) -> String {
    v.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

fn xml_unescape_docx(v: &str) -> String {
    v.replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&amp;", "&")
}

fn reduce_font_size_tags_docx(xml: &str) -> String {
    let mut out = xml.to_string();
    out = reduce_one_size_tag_docx(&out, "<w:sz w:val=\"");
    out = reduce_one_size_tag_docx(&out, "<w:szCs w:val=\"");
    out
}

fn reduce_one_size_tag_docx(xml: &str, marker: &str) -> String {
    let mut out = String::with_capacity(xml.len());
    let mut cursor = 0usize;
    while let Some(rel) = xml[cursor..].find(marker) {
        let start = cursor + rel;
        out.push_str(&xml[cursor..start]);
        let num_start = start + marker.len();
        let Some(end_rel) = xml[num_start..].find('"') else {
            out.push_str(&xml[start..]);
            return out;
        };
        let num_end = num_start + end_rel;
        let old_num = &xml[num_start..num_end];
        if let Ok(v) = old_num.parse::<u16>() {
            let new_v = v.saturating_sub(2).max(16);
            out.push_str(marker);
            out.push_str(&new_v.to_string());
            out.push('"');
        } else {
            out.push_str(&xml[start..=num_end]);
        }
        cursor = num_end + 1;
    }
    out.push_str(&xml[cursor..]);
    out
}

fn sanitize_docx_filename(s: &str) -> String {
    let invalid: [char; 9] = ['<', '>', ':', '"', '/', '\\', '|', '?', '*'];
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        if ch.is_control() || invalid.contains(&ch) {
            out.push('_');
        } else {
            out.push(ch);
        }
    }
    let out = out.trim().trim_end_matches('.').to_string();
    if out.is_empty() {
        "item".to_string()
    } else {
        out
    }
}

fn build_unique_docx_targets(outdir: &Path, rows: &[DocumentRow]) -> Vec<PathBuf> {
    let mut reserved: HashSet<String> = HashSet::new();
    if let Ok(rd) = fs::read_dir(outdir) {
        for ent in rd.flatten() {
            if let Some(name) = ent.file_name().to_str() {
                reserved.insert(canonical_filename_key(name));
            }
        }
    }

    let mut out = Vec::with_capacity(rows.len());

    for (idx, row) in rows.iter().enumerate() {
        let base = sanitize_docx_filename(&row.part_name);
        let part_no = sanitize_docx_filename(&row.part_no);
        let key = format!("기안문_({})", base);

        let mut cand = format!("{key}.docx");
        if reserved.contains(&canonical_filename_key(&cand)) {
            cand = format!("{key}_{}.docx", part_no);
        }
        if reserved.contains(&canonical_filename_key(&cand)) {
            cand = format!("{key}_{:04}.docx", idx + 1);
        }

        let mut seq = idx + 1;
        while reserved.contains(&canonical_filename_key(&cand)) {
            seq += 1;
            cand = format!("{key}_{:04}.docx", seq);
        }

        reserved.insert(canonical_filename_key(&cand));
        let fname = cand;
        out.push(outdir.join(fname));
    }

    out
}

fn canonical_filename_key(name: &str) -> String {
    name.trim().to_lowercase()
}

fn format_price_docx(input: &str) -> String {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return "(직접입력)".to_string();
    }
    let negative = trimmed.starts_with('-');
    let unsigned = if negative { &trimmed[1..] } else { trimmed };
    let mut parts = unsigned.splitn(2, '.');
    let int_part = parts.next().unwrap_or_default();
    let frac_part = parts.next();
    if !int_part.chars().all(|c| c.is_ascii_digit()) {
        return input.to_string();
    }
    let mut grouped_rev = String::with_capacity(int_part.len() + (int_part.len() / 3));
    for (i, ch) in int_part.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            grouped_rev.push(',');
        }
        grouped_rev.push(ch);
    }
    let mut grouped: String = grouped_rev.chars().rev().collect();
    if let Some(frac) = frac_part {
        if !frac.is_empty() {
            grouped.push('.');
            grouped.push_str(frac);
        }
    }
    if negative {
        format!("-{}", grouped)
    } else {
        grouped
    }
}

fn run_legacy_mode(args: &LegacyArgs) -> Result<()> {
    let stamp = Local::now().format("%Y%m%d_%H%M%S").to_string();
    let temp_root = PathBuf::from(format!("./DB/legacy_tmp_{stamp}"));
    let workspace = setup_workspace(&temp_root)?;

    let artifacts = run_batch_pipeline(&workspace, &args.inbound, &args.stock, &args.outbound, false)?;

    if let Some(parent) = args.out_json.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)
                .with_context(|| format!("create dir failed: {}", parent.display()))?;
        }
    }

    fs::copy(&artifacts.out_json, &args.out_json).with_context(|| {
        format!(
            "copy failed: {} -> {}",
            artifacts.out_json.display(),
            args.out_json.display()
        )
    })?;

    println!("legacy mode done");
    println!("json: {}", args.out_json.display());
    Ok(())
}

fn write_batch_report(workspace: &Workspace, stamp: &str, report: &BatchReport) -> Result<PathBuf> {
    fs::create_dir_all(&workspace.reports)
        .with_context(|| format!("create report dir failed: {}", workspace.reports.display()))?;
    let report_path = workspace.reports.join(format!("batch_report_{stamp}.json"));
    let mut f = File::create(&report_path)
        .with_context(|| format!("report file create failed: {}", report_path.display()))?;
    serde_json::to_writer_pretty(&mut f, report).context("write batch report failed")?;
    writeln!(&mut f).ok();
    Ok(report_path)
}

fn path_relative_to(base: &Path, path: &Path) -> String {
    path.strip_prefix(base)
        .map(|p| p.display().to_string())
        .unwrap_or_else(|_| path.display().to_string())
}

fn last_batch_marker_path(workspace: &Workspace) -> PathBuf {
    workspace.logs.join("last_batch_fingerprint.txt")
}

fn compute_batch_fingerprint(inbound: &Path, stock: &Path, outbound: &Path) -> Result<String> {
    let mut rows = Vec::new();
    for p in [inbound, stock, outbound] {
        let meta = fs::metadata(p)?;
        let modified = meta
            .modified()
            .unwrap_or(SystemTime::UNIX_EPOCH)
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        rows.push(format!("{}|{}|{}", p.display(), meta.len(), modified));
    }
    rows.sort();
    Ok(rows.join("||"))
}

fn is_same_as_last_processed(workspace: &Workspace, current: &str) -> Result<bool> {
    let marker = last_batch_marker_path(workspace);
    if !marker.exists() {
        return Ok(false);
    }
    let last = fs::read_to_string(marker)?.trim().to_string();
    Ok(last == current)
}

fn write_last_processed_fingerprint(workspace: &Workspace, current: &str) -> Result<()> {
    fs::write(last_batch_marker_path(workspace), current)
        .context("write last batch marker failed")
}

fn write_log(workspace: &Workspace, msg: &str) {
    let now = Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
    let line = format!("[{now}] {msg}\n");
    let path = workspace.logs.join("workflow.log");

    let mut file = match fs::OpenOptions::new().create(true).append(true).open(&path) {
        Ok(v) => v,
        Err(_) => return,
    };
    let _ = file.write_all(line.as_bytes());
}

fn doc_kind_name(kind: DocKind) -> &'static str {
    match kind {
        DocKind::Inbound => "inbound",
        DocKind::Stock => "stock",
        DocKind::Outbound => "outbound",
    }
}

fn join_doc_kinds(kinds: &[DocKind]) -> String {
    kinds
        .iter()
        .map(|k| doc_kind_name(*k))
        .collect::<Vec<_>>()
        .join(", ")
}

fn join_paths(paths: &[PathBuf]) -> String {
    paths
        .iter()
        .map(|p| p.display().to_string())
        .collect::<Vec<_>>()
        .join(", ")
}

fn extract_ymd_from_filename(path: &Path) -> Option<String> {
    let name = path.file_name()?.to_str()?;
    let digits = name.chars().filter(|c| c.is_ascii_digit()).collect::<String>();
    for i in 0..digits.len().saturating_sub(7) {
        let chunk = &digits[i..i + 8];
        if chunk.starts_with("20") {
            return Some(format!("{}-{}-{}", &chunk[0..4], &chunk[4..6], &chunk[6..8]));
        }
    }
    None
}

fn resolve_part_name(part_name: Option<&str>, part_no: Option<&str>) -> String {
    let n = part_name.unwrap_or("").trim();
    if !n.is_empty() {
        return n.to_string();
    }
    let p = part_no.unwrap_or("").trim();
    if !p.is_empty() {
        return format!("PART_{p}");
    }
    "UNKNOWN_PART".to_string()
}

fn part_key(part_no: Option<&str>, part_name: Option<&str>) -> String {
    let name = resolve_part_name(part_name, part_no);
    let no = part_no.unwrap_or("").trim();
    if no.is_empty() {
        format!("{name}||NO_PART_NO")
    } else {
        format!("{name}||{no}")
    }
}

fn read_inbound(path: &Path) -> Result<Vec<RowRecord>> {
    let range = read_first_sheet(path)?;
    let rows = range.rows().collect::<Vec<_>>();
    if rows.is_empty() {
        return Ok(Vec::new());
    }
    let header = rows[0].iter().map(cell_to_string_raw).collect::<Vec<_>>();
    let part_no_idx = find_col_index(&header, &["부품번호", "part_no", "partno"]).unwrap_or(9);
    let part_name_idx = find_col_index(&header, &["품명", "part_name", "name"]).unwrap_or(10);
    let qty_idx = find_col_index(&header, &["검수량", "qty", "quantity"]).unwrap_or(12);
    let date_idx = find_col_index(&header, &["입고일자", "date"]).unwrap_or(1);
    let source_file = path
        .file_name()
        .and_then(|v| v.to_str())
        .unwrap_or("")
        .to_string();

    let mut out = Vec::new();
    for row in rows.into_iter().skip(1) {
        if row.iter().all(|c| matches!(c, Data::Empty)) {
            continue;
        }
        let date_raw = get_str_at(row, date_idx);
        let date_iso = date_raw
            .as_deref()
            .and_then(normalize_date_to_iso);
        out.push(RowRecord {
            columns: extract_row_columns(row, &header),
            part_no: get_str_at(row, part_no_idx),
            part_name: get_str_at(row, part_name_idx),
            qty: get_f64_at(row, qty_idx),
            date: date_raw,
            date_iso,
            source_file: source_file.clone(),
        });
    }
    Ok(out)
}

fn read_outbound(path: &Path) -> Result<Vec<RowRecord>> {
    let range = read_first_sheet(path)?;
    let rows = range.rows().collect::<Vec<_>>();
    if rows.is_empty() {
        return Ok(Vec::new());
    }
    let header = rows[0].iter().map(cell_to_string_raw).collect::<Vec<_>>();
    let part_no_idx = find_col_index(&header, &["부품번호", "part_no", "partno"]).unwrap_or(9);
    let part_name_idx = find_col_index(&header, &["품명", "part_name", "name"]).unwrap_or(10);
    let qty_idx = find_col_index(&header, &["지급량", "qty", "quantity"]).unwrap_or(12);
    let date_idx = find_col_index(&header, &["지급일자", "date"]).unwrap_or(1);
    let source_file = path
        .file_name()
        .and_then(|v| v.to_str())
        .unwrap_or("")
        .to_string();

    let mut out = Vec::new();
    for row in rows.into_iter().skip(1) {
        if row.iter().all(|c| matches!(c, Data::Empty)) {
            continue;
        }
        let date_raw = get_str_at(row, date_idx);
        let date_iso = date_raw
            .as_deref()
            .and_then(normalize_date_to_iso);
        out.push(RowRecord {
            columns: extract_row_columns(row, &header),
            part_no: get_str_at(row, part_no_idx),
            part_name: get_str_at(row, part_name_idx),
            qty: get_f64_at(row, qty_idx),
            date: date_raw,
            date_iso,
            source_file: source_file.clone(),
        });
    }
    Ok(out)
}

fn read_stock(path: &Path) -> Result<Vec<StockRecord>> {
    let range = read_first_sheet(path)?;
    let rows = range.rows().collect::<Vec<_>>();
    if rows.is_empty() {
        return Ok(Vec::new());
    }
    let header = rows[0].iter().map(cell_to_string_raw).collect::<Vec<_>>();
    let part_no_idx = find_col_index(&header, &["부품번호", "part_no", "partno"]).unwrap_or(2);
    let part_name_idx = find_col_index(&header, &["품명", "part_name", "name"]).unwrap_or(3);
    let qty_idx = find_col_index(&header, &["재고량", "stock_qty", "qty"]).unwrap_or(7);
    let source_file = path
        .file_name()
        .and_then(|v| v.to_str())
        .unwrap_or("")
        .to_string();

    let mut out = Vec::new();
    for row in rows.into_iter().skip(1) {
        if row.iter().all(|c| matches!(c, Data::Empty)) {
            continue;
        }
        out.push(StockRecord {
            columns: extract_row_columns(row, &header),
            part_no: get_str_at(row, part_no_idx),
            part_name: get_str_at(row, part_name_idx),
            stock_qty: get_f64_at(row, qty_idx),
            source_file: source_file.clone(),
        });
    }
    Ok(out)
}

fn read_first_sheet(path: &Path) -> Result<calamine::Range<Data>> {
    let mut wb = open_workbook_auto(path)
        .with_context(|| format!("open workbook failed: {}", path.display()))?;
    let sheet_name = wb
        .sheet_names()
        .first()
        .cloned()
        .context("no worksheet found")?;
    wb.worksheet_range(&sheet_name)
        .with_context(|| format!("worksheet read failed: {}", sheet_name))
}

fn find_col_index(header: &[String], aliases: &[&str]) -> Option<usize> {
    let alias_norm = aliases
        .iter()
        .map(|a| normalize_header_key(a))
        .collect::<Vec<_>>();
    for (i, h) in header.iter().enumerate() {
        let n = normalize_header_key(h);
        if alias_norm.iter().any(|a| a == &n) {
            return Some(i);
        }
    }
    None
}

fn normalize_header_key(s: &str) -> String {
    s.replace('\u{a0}', " ")
        .replace('\r', " ")
        .replace('\n', " ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join("")
        .to_lowercase()
}

fn extract_row_columns(row: &[Data], header: &[String]) -> HashMap<String, String> {
    let mut columns = HashMap::new();
    let mut duplicate_count: HashMap<String, usize> = HashMap::new();
    for (i, cell) in row.iter().enumerate() {
        let base = header
            .get(i)
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| format!("col_{i}"));
        let count = duplicate_count.entry(base.clone()).or_insert(0);
        *count += 1;
        let key = if *count > 1 {
            format!("{}__{}", base, count)
        } else {
            base
        };
        let value = cell_to_string_raw(cell).trim().to_string();
        columns.insert(key, value);
    }
    columns
}

fn get_str_at(row: &[Data], idx: usize) -> Option<String> {
    row.get(idx).and_then(cell_to_string)
}

fn get_f64_at(row: &[Data], idx: usize) -> Option<f64> {
    let c = row.get(idx)?;
    match c {
        Data::Float(f) => Some(*f),
        Data::Int(i) => Some(*i as f64),
        Data::String(s) => s.replace(',', "").trim().parse::<f64>().ok(),
        _ => cell_to_string(c)?.replace(',', "").trim().parse::<f64>().ok(),
    }
}

fn cell_to_string(c: &Data) -> Option<String> {
    let s = cell_to_string_raw(c);
    let t = s.trim().to_string();
    if t.is_empty() {
        None
    } else {
        Some(t)
    }
}

fn cell_to_string_raw(c: &Data) -> String {
    match c {
        Data::String(s) => s.clone(),
        Data::Float(f) => format!("{f}"),
        Data::Int(i) => format!("{i}"),
        Data::Bool(b) => format!("{b}"),
        Data::DateTime(dt) => dt.to_string(),
        Data::DateTimeIso(s) => s.clone(),
        Data::DurationIso(s) => s.clone(),
        Data::Error(e) => format!("{e:?}"),
        Data::Empty => String::new(),
    }
}

fn normalize_date_to_iso(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }

    if let Ok(serial) = trimmed.parse::<f64>() {
        return excel_serial_to_iso(serial);
    }

    for fmt in ["%Y-%m-%d", "%Y/%m/%d", "%m/%d/%Y", "%-m/%-d/%Y", "%Y.%m.%d"] {
        if let Ok(d) = NaiveDate::parse_from_str(trimmed, fmt) {
            return Some(d.format("%Y-%m-%d").to_string());
        }
    }

    None
}

fn excel_serial_to_iso(serial: f64) -> Option<String> {
    if !serial.is_finite() || serial < 1.0 {
        return None;
    }
    let base = NaiveDate::from_ymd_opt(1899, 12, 30)?;
    let day = serial.trunc() as i64;
    let date = base.checked_add_signed(ChronoDuration::days(day))?;
    Some(date.format("%Y-%m-%d").to_string())
}
