use anyhow::{bail, Context, Result};
use calamine::{open_workbook_auto, Data, Reader};
use chrono::{Duration as ChronoDuration, Local, NaiveDate};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
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

// 구매결정 V2 규칙(필수재고 30% + 단가 50만원 기준)
// 현재는 기존 로직 유지 요청으로 비활성화 상태.
const ENABLE_PURCHASE_DECISION_V2: bool = true;
const DATE_PARSE_FORMATS: [&str; 5] = ["%Y-%m-%d", "%Y/%m/%d", "%m/%d/%Y", "%-m/%-d/%Y", "%Y.%m.%d"];
const TEMPLATE_DIR_NAME: &str = "문서제작양식";
// 50만원 이상(>=) 템플릿(레거시): 부품 구매 요청 품의
const TEMPLATE_OVER_500K_DOCX: &str = "한진_부품구매_요청_양식.docx";
// 50만원 이상(>=) + 교체이력 있음
const TEMPLATE_OVER_500K_WITH_HISTORY_DOCX: &str = "부품구매요청_교체이럭_유.docx";
// 50만원 이상(>=) + 교체이력 없음
const TEMPLATE_OVER_500K_WITHOUT_HISTORY_DOCX: &str = "부품구매요청_교체이력_무.docx";
// 50만원 이하(<) 템플릿(레거시): 부품 구매 품의
const TEMPLATE_UNDER_EQ_500K_DOCX: &str = "한진_부품구매_양식.docx";
// 50만원 이하(<) + 교체이력 있음
const TEMPLATE_UNDER_EQ_500K_WITH_HISTORY_DOCX: &str = "부품구매_교체이력_유.docx";
// 50만원 이하(<) + 교체이력 없음
const TEMPLATE_UNDER_EQ_500K_WITHOUT_HISTORY_DOCX: &str = "부품구매_교체이력_무.docx";

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
    required_stock: Option<f64>,
    purchase_qty: f64,
    purchase_order_note: String,
    issued_qty: String,
    replacement_dates: [String; 6],
    replacement_qtys: [String; 6],
    replacement_hosts: [String; 6],
    vendor_name: String,
    manufacturer_name: String,
    unit: String,
    unit_price: String,
    part_role: String,
    template_kind: PurchaseTemplateKind,
    has_replacement_history: bool,
}

#[derive(Debug, Clone)]
struct ReplacementEvent {
    date_iso: String,
    qty: String,
    host: String,
    row_idx: usize,
}

#[derive(Debug, Clone, Deserialize)]
struct PartFunctionRecord {
    item_name: String,
    description: String,
}

#[derive(Debug, Clone, Default)]
struct PartRoleIndex {
    exact: HashMap<String, String>,
    items: Vec<PartRoleItem>,
    first_char_index: HashMap<char, Vec<usize>>,
}

#[derive(Debug, Clone)]
struct PartRoleItem {
    key: String,
    role: String,
    key_bigrams: HashSet<String>,
}

#[derive(Debug, Clone)]
struct PurchaseDecision {
    should_purchase: bool,
    note: String,
    template_kind: PurchaseTemplateKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PurchaseTemplateKind {
    Over500k,
    UnderEq500k,
}

impl PartRoleIndex {
    fn insert(&mut self, part_name: &str, role: &str) {
        let key = normalize_match_key(part_name);
        if key.is_empty() {
            return;
        }
        self.exact.insert(key.clone(), role.to_string());
        let idx = self.items.len();
        if let Some(ch) = key.chars().next() {
            self.first_char_index.entry(ch).or_default().push(idx);
        }
        self.items.push(PartRoleItem {
            key_bigrams: bigram_set(&key),
            key,
            role: role.to_string(),
        });
    }

    fn lookup(&self, part_name: &str) -> Option<String> {
        let mut query = String::with_capacity(part_name.len());
        normalize_match_key_into(part_name, &mut query);
        if query.is_empty() {
            return None;
        }
        if let Some(role) = self.exact.get(&query) {
            return Some(role.clone());
        }

        let query_bigrams = bigram_set(&query);
        let mut candidate_indices: Option<&[usize]> = None;
        if let Some(ch) = query.chars().next() {
            if let Some(indices) = self.first_char_index.get(&ch) {
                candidate_indices = Some(indices.as_slice());
            }
        }

        let mut best_score = 0.0f64;
        let mut best_role: Option<&str> = None;
        if let Some(indices) = candidate_indices {
            for idx in indices {
                let item = &self.items[*idx];
                let mut score = jaccard_bigram_score_sets(&query_bigrams, &item.key_bigrams);
                if item.key.starts_with(&query) || query.starts_with(&item.key) {
                    score += 0.15;
                }
                if item.key.contains(&query) || query.contains(&item.key) {
                    score += 0.10;
                }
                let prefix = common_prefix_len(&query, &item.key) as f64;
                let base = query.len().max(item.key.len()) as f64;
                if base > 0.0 {
                    score += (prefix / base) * 0.10;
                }
                if score > best_score {
                    best_score = score;
                    best_role = Some(item.role.as_str());
                }
            }
        } else {
            for item in &self.items {
                let mut score = jaccard_bigram_score_sets(&query_bigrams, &item.key_bigrams);
                if item.key.starts_with(&query) || query.starts_with(&item.key) {
                    score += 0.15;
                }
                if item.key.contains(&query) || query.contains(&item.key) {
                    score += 0.10;
                }
                let prefix = common_prefix_len(&query, &item.key) as f64;
                let base = query.len().max(item.key.len()) as f64;
                if base > 0.0 {
                    score += (prefix / base) * 0.10;
                }
                if score > best_score {
                    best_score = score;
                    best_role = Some(item.role.as_str());
                }
            }
        }
        // Temp_Setting
        if best_score >= 0.30 {
            return best_role.map(|v| v.to_string());
        }
        None
    }
}

fn is_ignored_match_char(c: char) -> bool {
    c.is_whitespace()
        || matches!(
            c,
            '-' | '_' | '(' | ')' | '[' | ']' | '{' | '}' | '.' | ',' | ':' | '/' | '\\' | '|'
                | '\'' | '"' | '`' | '~' | '!' | '@' | '#' | '$' | '%' | '^' | '&' | '*' | '+'
                | '=' | '?'
        )
}

fn normalize_match_key_into(s: &str, out: &mut String) {
    out.clear();
    out.reserve(s.len());
    for ch in s.chars() {
        for lower in ch.to_lowercase() {
            if !is_ignored_match_char(lower) {
                out.push(lower);
            }
        }
    }
}

fn normalize_match_key(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    normalize_match_key_into(s, &mut out);
    out
}

fn jaccard_bigram_score_sets(a: &HashSet<String>, b: &HashSet<String>) -> f64 {
    if a.is_empty() || b.is_empty() {
        return 0.0;
    }
    let inter = a.intersection(b).count() as f64;
    let union = a.union(b).count() as f64;
    if union == 0.0 {
        0.0
    } else {
        inter / union
    }
}

fn bigram_set(s: &str) -> HashSet<String> {
    let chars: Vec<char> = s.chars().collect();
    if chars.len() < 2 {
        return HashSet::new();
    }
    let mut out = HashSet::with_capacity(chars.len().saturating_sub(1));
    for i in 0..chars.len() - 1 {
        out.insert(format!("{}{}", chars[i], chars[i + 1]));
    }
    out
}

fn common_prefix_len(a: &str, b: &str) -> usize {
    a.chars().zip(b.chars()).take_while(|(x, y)| x == y).count()
}

fn is_effective_row(row: &[Data]) -> bool {
    row.iter().any(|c| !matches!(c, Data::Empty))
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

#[derive(Clone)]
struct NamedTemplatePackages {
    over_500k_with_history: Arc<TemplatePackage>,
    over_500k_without_history: Arc<TemplatePackage>,
    under_eq_500k_with_history: Arc<TemplatePackage>,
    under_eq_500k_without_history: Arc<TemplatePackage>,
    over_500k_with_history_name: String,
    over_500k_without_history_name: String,
    under_eq_500k_with_history_name: String,
    under_eq_500k_without_history_name: String,
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
    if let Some(name) = path.file_name().and_then(|v| v.to_str()) {
        if name.starts_with("~$") {
            return false;
        }
    }
    path.extension()
        .and_then(|v| v.to_str())
        .map(|ext| matches!(ext.to_ascii_lowercase().as_str(), "xlsx" | "xlsm" | "xls"))
        .unwrap_or(false)
}

fn detect_kind_from_filename(path: &Path) -> Option<DocKind> {
    let raw = path.file_name()?.to_string_lossy().to_string();
    let name = raw.to_ascii_lowercase();

    // Prefer explicit filename markers/keywords before loose number matching.
    if raw.contains("출고") || name.contains("outbound") {
        return Some(DocKind::Outbound);
    }
    if raw.contains("재고") || name.contains("stock") {
        return Some(DocKind::Stock);
    }
    if raw.contains("입고") || name.contains("inbound") {
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
    let Some(templates) = resolve_docx_templates_by_name(workspace)? else {
        write_log(workspace, "named docx templates not found; skipping docx generation");
        return Ok((None, 0));
    };

    let part_roles = load_part_role_index(&workspace.input_dir)?;
    let rows = build_document_rows(snapshot, true, None, part_roles.as_ref());
    let outdir = workspace
        .output
        .join(Local::now().format("%Y-%m-%d").to_string());
    fs::create_dir_all(&outdir)
        .with_context(|| format!("create docx outdir failed: {}", outdir.display()))?;
    for dir in [
        outdir.join("over_500k").join("history_yes"),
        outdir.join("over_500k").join("history_no"),
        outdir.join("under_eq_500k").join("history_yes"),
        outdir.join("under_eq_500k").join("history_no"),
    ] {
        fs::create_dir_all(&dir).with_context(|| format!("create dir failed: {}", dir.display()))?;
    }

    let targets = build_unique_docx_targets_by_group(&outdir, &rows);
    let generated = AtomicUsize::new(0);

    rows.par_iter()
        .zip(targets.par_iter())
        .enumerate()
        .try_for_each(|(idx, (row, output))| -> Result<()> {
            let selected = select_template_for_row(row, &templates);
            render_docx_from_package(selected, output, row, idx + 1)?;
            generated.fetch_add(1, Ordering::Relaxed);
            Ok(())
        })?;

    write_log(
        workspace,
        &format!(
            "docx generated: count={}, outdir='{}', split_dirs='over_500k/history_yes,over_500k/history_no,under_eq_500k/history_yes,under_eq_500k/history_no', templates='over_hist_yes:{}','over_hist_no:{}','under_hist_yes:{}','under_hist_no:{}'",
            generated.load(Ordering::Relaxed),
            outdir.display(),
            templates.over_500k_with_history_name,
            templates.over_500k_without_history_name,
            templates.under_eq_500k_with_history_name,
            templates.under_eq_500k_without_history_name
        ),
    );

    Ok((Some(outdir), generated.load(Ordering::Relaxed)))
}

fn resolve_docx_templates_by_name(workspace: &Workspace) -> Result<Option<NamedTemplatePackages>> {
    let template_dir = workspace.input_dir.join(TEMPLATE_DIR_NAME);

    let over_500k_path = workspace.input_dir.join(TEMPLATE_OVER_500K_DOCX);
    let over_500k_with_history_path = template_dir.join(TEMPLATE_OVER_500K_WITH_HISTORY_DOCX);
    let over_500k_without_history_path = template_dir.join(TEMPLATE_OVER_500K_WITHOUT_HISTORY_DOCX);

    let (over_500k_with_history_pkg, over_500k_with_history_name, over_500k_without_history_pkg, over_500k_without_history_name) =
        if over_500k_with_history_path.exists() && over_500k_without_history_path.exists() {
            (
                Arc::new(load_template_package(&over_500k_with_history_path)?),
                TEMPLATE_OVER_500K_WITH_HISTORY_DOCX.to_string(),
                Arc::new(load_template_package(&over_500k_without_history_path)?),
                TEMPLATE_OVER_500K_WITHOUT_HISTORY_DOCX.to_string(),
            )
        } else if over_500k_path.exists() {
            write_log(
                workspace,
                &format!(
                    "500k over history templates not fully found; fallback to legacy template '{}'",
                    TEMPLATE_OVER_500K_DOCX
                ),
            );
            let legacy = Arc::new(load_template_package(&over_500k_path)?);
            (
                legacy.clone(),
                TEMPLATE_OVER_500K_DOCX.to_string(),
                legacy,
                TEMPLATE_OVER_500K_DOCX.to_string(),
            )
        } else {
            return Ok(None);
        };

    let under_eq_500k_path = workspace.input_dir.join(TEMPLATE_UNDER_EQ_500K_DOCX);
    let under_eq_500k_with_history_path = template_dir.join(TEMPLATE_UNDER_EQ_500K_WITH_HISTORY_DOCX);
    let under_eq_500k_without_history_path = template_dir.join(TEMPLATE_UNDER_EQ_500K_WITHOUT_HISTORY_DOCX);

    let (under_eq_500k_with_history_pkg, under_eq_500k_with_history_name, under_eq_500k_without_history_pkg, under_eq_500k_without_history_name) =
        if under_eq_500k_with_history_path.exists() && under_eq_500k_without_history_path.exists() {
            (
                Arc::new(load_template_package(&under_eq_500k_with_history_path)?),
                TEMPLATE_UNDER_EQ_500K_WITH_HISTORY_DOCX.to_string(),
                Arc::new(load_template_package(&under_eq_500k_without_history_path)?),
                TEMPLATE_UNDER_EQ_500K_WITHOUT_HISTORY_DOCX.to_string(),
            )
        } else if under_eq_500k_path.exists() {
            write_log(
                workspace,
                &format!(
                    "500k under history templates not fully found; fallback to legacy template '{}'",
                    TEMPLATE_UNDER_EQ_500K_DOCX
                ),
            );
            let legacy = Arc::new(load_template_package(&under_eq_500k_path)?);
            (
                legacy.clone(),
                TEMPLATE_UNDER_EQ_500K_DOCX.to_string(),
                legacy,
                TEMPLATE_UNDER_EQ_500K_DOCX.to_string(),
            )
        } else {
            write_log(
                workspace,
                "500k under templates not found; fallback to 500k over(no-history) template",
            );
            (
                over_500k_without_history_pkg.clone(),
                over_500k_without_history_name.clone(),
                over_500k_without_history_pkg.clone(),
                over_500k_without_history_name.clone(),
            )
        };

    Ok(Some(NamedTemplatePackages {
        over_500k_with_history: over_500k_with_history_pkg,
        over_500k_without_history: over_500k_without_history_pkg,
        under_eq_500k_with_history: under_eq_500k_with_history_pkg,
        under_eq_500k_without_history: under_eq_500k_without_history_pkg,
        over_500k_with_history_name,
        over_500k_without_history_name,
        under_eq_500k_with_history_name,
        under_eq_500k_without_history_name,
    }))
}

fn select_template_for_row<'a>(row: &DocumentRow, templates: &'a NamedTemplatePackages) -> &'a TemplatePackage {
    match row.template_kind {
        PurchaseTemplateKind::Over500k => {
            if row.has_replacement_history {
                templates.over_500k_with_history.as_ref()
            } else {
                templates.over_500k_without_history.as_ref()
            }
        }
        PurchaseTemplateKind::UnderEq500k => {
            if row.has_replacement_history {
                templates.under_eq_500k_with_history.as_ref()
            } else {
                templates.under_eq_500k_without_history.as_ref()
            }
        }
    }
}

fn build_document_rows(
    snapshot: &Snapshot,
    include_no_outbound: bool,
    limit: Option<usize>,
    part_roles: Option<&PartRoleIndex>,
) -> Vec<DocumentRow> {
    let mut rows = Vec::with_capacity(snapshot.parts.len());

    for (part_key, part) in &snapshot.parts {
        if !include_no_outbound && part.outbound_count == 0 {
            continue;
        }

        let part_no = fallback_missing_doc(part.part_no.clone());
        let part_name = fallback_missing_doc(Some(part.part_name.clone()));
        let part_role = part_roles
            .and_then(|idx| idx.lookup(&part_name))
            .unwrap_or_else(|| "(직접입력)".to_string());
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
        let mut issued_qty = format!("{:.0}", part.outbound_qty_sum);
        let mut replacement_dates = [
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
        ];
        let mut replacement_qtys = [
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
        ];
        let mut replacement_hosts = [
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
        ];
        let mut unit = "기록없음".to_string();
        let mut vendor_name = "기록없음".to_string();
        let mut manufacturer_name = "기록없음".to_string();
        let mut unit_price = "기록없음".to_string();

        if let Some(out_idx) = part.outbound_row_idx.last() {
            if let Some(row) = snapshot.raw.outbound_rows.get(*out_idx) {
                let equip_name = pick_first_col(&row.columns, &["장비명", "주요장비명"]);
                let equip_no = pick_first_col(&row.columns, &["장비번호"]);
                let model_fallback = pick_first_col(&row.columns, &["주요Model명", "Model명"]);
                used_where = if equip_name != "기록없음" {
                    equip_name
                } else if equip_no != "기록없음" {
                    equip_no
                } else {
                    model_fallback
                };
                usage_reason = pick_first_col(&row.columns, &["운영구분", "지급구분", "요청번호"]);
                replacement_reason = pick_first_col(&row.columns, &["지급구분", "운영구분", "요청번호"]);
                issued_qty = pick_first_col(&row.columns, &["지급량"]);
                if let Some(d) = row.date_iso.clone() {
                    used_date_last = d;
                }
            }
        }

        let events = collect_replacement_events(snapshot, part);
        let has_replacement_history = !events.is_empty();
        if !events.is_empty() {
            let latest = events.last().expect("events is non-empty");
            used_date_last = latest.date_iso.clone();
            // Fill both left/right history columns top-down: 1,4,2,5,3,6
            let slot_order = [0usize, 3, 1, 4, 2, 5];
            for (i, ev) in events.iter().take(6).enumerate() {
                let slot = slot_order[i];
                replacement_dates[slot] = ev.date_iso.clone();
                replacement_qtys[slot] = ev.qty.clone();
                replacement_hosts[slot] = ev.host.clone();
            }
        }

        if let Some(in_idx) = part.inbound_row_idx.last() {
            if let Some(in_row) = snapshot.raw.inbound_rows.get(*in_idx) {
                let v = pick_first_col(
                    &in_row.columns,
                    &["납품업체", "납품업체명", "거래처", "업체", "공급업체", "구매업체"],
                );
                if v != "기록없음" {
                    vendor_name = v;
                }

                unit = pick_first_col(&in_row.columns, &["단위"]);
                unit_price = pick_first_col(
                    &in_row.columns,
                    &["단가", "구단가", "재고금액(원)", "재고 금액(원)", "재고금액", "금액"],
                );
            }
        }

        let current_stock_before = part.current_stock_before.unwrap_or(0.0);
        let current_stock_updated = part.current_stock_updated.unwrap_or(0.0);
        let stock_row = part
            .stock_row_idx
            .first()
            .and_then(|idx| snapshot.raw.stock_rows.get(*idx));
        if let Some(row) = stock_row {
            if is_missing_doc_value(&used_where) {
                used_where = pick_first_col(&row.columns, &["주요장비명", "장비명", "재고번호"]);
            }
            if is_missing_doc_value(&vendor_name) {
                vendor_name = pick_first_col(
                    &row.columns,
                    &["납품업체", "납품업체명", "거래처", "업체", "공급업체", "구매업체"],
                );
            }
            if is_missing_doc_value(&manufacturer_name) {
                let m = pick_first_col(
                    &row.columns,
                    &[
                        "주요Model명",
                        "Model명",
                        "부품제조사",
                        "부품 제조사",
                    ],
                );
                manufacturer_name = extract_manufacturer_name(&m);
            }
            if is_missing_doc_value(&unit) {
                unit = pick_first_col(&row.columns, &["단위"]);
            }
            if is_missing_doc_value(&unit_price) {
                unit_price = pick_first_col(
                    &row.columns,
                    &["재고금액(원)", "재고 금액(원)", "재고금액", "금액", "단가", "구단가"],
                );
            }
        }
        let required_stock = stock_row
            .and_then(|row| {
                let raw = pick_first_col(
                    &row.columns,
                    &["필수재고량", "필수 재고량", "최소재고", "min_stock", "safety_stock"],
                );
                parse_numeric_text(&raw)
            });
        let unit_price_value = parse_numeric_text(&unit_price);
        let decision = if ENABLE_PURCHASE_DECISION_V2 {
            decide_purchase_v2(required_stock, current_stock_before, unit_price_value)
        } else {
            decide_purchase_legacy(current_stock_updated)
        };
        if ENABLE_PURCHASE_DECISION_V2 && !decision.should_purchase {
            continue;
        }
        let purchase_order_note = decision.note;
        let purchase_qty = required_stock
            .map(|req| (req - current_stock_before).max(0.0))
            .filter(|v| *v > 0.0)
            .unwrap_or(1.0);

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
            required_stock,
            purchase_qty,
            purchase_order_note,
            issued_qty,
            replacement_dates,
            replacement_qtys,
            replacement_hosts,
            vendor_name,
            manufacturer_name,
            unit,
            unit_price,
            part_role,
            template_kind: decision.template_kind,
            has_replacement_history,
        });
    }

    rows.sort_by(|a, b| a.part_key.cmp(&b.part_key));
    if let Some(n) = limit {
        rows.truncate(n);
    }
    rows
}

fn collect_replacement_events(snapshot: &Snapshot, part: &PartBlock) -> Vec<ReplacementEvent> {
    let mut raw_events = Vec::new();
    for idx in &part.outbound_row_idx {
        if let Some(row) = snapshot.raw.outbound_rows.get(*idx) {
            let date_iso = row
                .date_iso
                .clone()
                .or_else(|| {
                    let raw = pick_first_col(&row.columns, &["지급일자", "출고일자", "date"]);
                    normalize_date_to_iso(&raw)
                })
                .unwrap_or_else(|| "0000-00-00".to_string());
            let qty = row
                .qty
                .map(|v| format!("{:.0}", v))
                .unwrap_or_else(|| {
                    let raw = pick_first_col(&row.columns, &["지급량", "검수량", "qty"]);
                    parse_numeric_text(&raw)
                        .map(|n| format!("{:.0}", n))
                        .unwrap_or_else(|| "0".to_string())
                });
            let host = pick_first_col(&row.columns, &["장비번호", "호기", "설비번호", "장비명", "주요장비명"]);
            raw_events.push(ReplacementEvent {
                date_iso,
                qty,
                host,
                row_idx: *idx,
            });
        }
    }

    raw_events.sort_by(|a, b| match a.date_iso.cmp(&b.date_iso) {
        std::cmp::Ordering::Equal => a.row_idx.cmp(&b.row_idx),
        ord => ord,
    });

    // Quantity-based plotting:
    // one outbound row with qty=N becomes N plotted points (qty=1 each).
    let mut latest_plots_rev = Vec::with_capacity(6);
    for ev in raw_events.iter().rev() {
        if latest_plots_rev.len() >= 6 {
            break;
        }
        let n = parse_numeric_text(&ev.qty)
            .map(|v| v.max(0.0).round() as usize)
            .unwrap_or(1);
        if n == 0 {
            continue;
        }
        let take = n.min(6 - latest_plots_rev.len());
        for _ in 0..take {
            latest_plots_rev.push(ReplacementEvent {
                date_iso: ev.date_iso.clone(),
                qty: "1".to_string(),
                host: ev.host.clone(),
                row_idx: ev.row_idx,
            });
        }
    }
    latest_plots_rev.reverse();
    latest_plots_rev
}

fn pick_first_col(columns: &HashMap<String, String>, keys: &[&str]) -> String {
    // 1) exact key match
    for key in keys {
        if let Some(v) = columns.get(*key) {
            let t = v.trim();
            if !t.is_empty() {
                return t.to_string();
            }
        }
    }

    // 2) normalized key match (handles line-breaks/spaces/nbsp in Excel headers)
    let norm_aliases = keys
        .iter()
        .map(|k| normalize_header_key(k))
        .collect::<Vec<_>>();
    for (k, v) in columns {
        let nk = normalize_header_key(k);
        if norm_aliases.iter().any(|a| a == &nk) {
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

fn parse_numeric_text(raw: &str) -> Option<f64> {
    let cleaned: String = raw
        .chars()
        .filter(|c| c.is_ascii_digit() || *c == '.' || *c == '-')
        .collect();
    if cleaned.is_empty() || cleaned == "-" || cleaned == "." {
        return None;
    }
    cleaned.parse::<f64>().ok()
}

fn decide_purchase_legacy(current_stock_updated: f64) -> PurchaseDecision {
    if current_stock_updated <= 0.0 {
        PurchaseDecision {
            should_purchase: true,
            note: "재고 부족 가능성 확인 후 구매발주 검토".to_string(),
            template_kind: PurchaseTemplateKind::Over500k,
        }
    } else {
        PurchaseDecision {
            should_purchase: true,
            note: "현재 재고 유지".to_string(),
            template_kind: PurchaseTemplateKind::Over500k,
        }
    }
}

fn is_missing_doc_value(v: &str) -> bool {
    let t = v.trim();
    t.is_empty() || matches!(t, "기록없음" | "출고기록없음" | "입고기록없음")
}

fn is_plausible_manufacturer_token(token: &str) -> bool {
    let t = token.trim();
    if t.is_empty() {
        return false;
    }

    let has_alpha = t.chars().any(|c| c.is_alphabetic());
    let has_digit = t.chars().any(|c| c.is_ascii_digit());

    has_alpha && !has_digit
}

fn extract_manufacturer_name(raw: &str) -> String {
    if is_missing_doc_value(raw) {
        return "기록없음".to_string();
    }

    let raw = raw.trim();
    if is_plausible_manufacturer_token(raw) {
        return raw.to_string();
    }

    for token in raw.split(|c: char| {
        c.is_whitespace() || matches!(c, '/' | '\\' | '|' | ',' | ';' | '(' | ')' | '[' | ']')
    }) {
        let token = token.trim();
        if is_plausible_manufacturer_token(token) {
            return token.to_string();
        }
    }

    "기록없음".to_string()
}

fn decide_purchase_v2(
    required_stock: Option<f64>,
    current_stock: f64,
    unit_price: Option<f64>,
) -> PurchaseDecision {
    let Some(req) = required_stock else {
        return PurchaseDecision {
            should_purchase: false,
            note: "구매 제외: 필수재고량 데이터 없음".to_string(),
            template_kind: PurchaseTemplateKind::UnderEq500k,
        };
    };

    if req <= 0.0 {
        return PurchaseDecision {
            should_purchase: false,
            note: "구매 제외: 필수재고량이 0 이하".to_string(),
            template_kind: PurchaseTemplateKind::UnderEq500k,
        };
    }

    if current_stock >= req {
        return PurchaseDecision {
            should_purchase: false,
            note: "구매 제외: 현재고가 필수재고량 이상".to_string(),
            template_kind: PurchaseTemplateKind::UnderEq500k,
        };
    }

    if current_stock > req * 0.3 {
        return PurchaseDecision {
            should_purchase: false,
            note: "구매 제외: 현재고가 필수재고량의 30% 초과".to_string(),
            template_kind: PurchaseTemplateKind::UnderEq500k,
        };
    }

    let price = unit_price.unwrap_or(0.0);
    if price >= 500_000.0 {
        PurchaseDecision {
            should_purchase: true,
            note: "구매 진행: 과거 단가 50만원 이상 -> 부품 구매 요청 품의".to_string(),
            template_kind: PurchaseTemplateKind::Over500k,
        }
    } else {
        PurchaseDecision {
            should_purchase: true,
            note: "구매 진행: 과거 단가 50만원 이하 -> 부품 구매 품의".to_string(),
            template_kind: PurchaseTemplateKind::UnderEq500k,
        }
    }
}

fn load_part_role_index(input_dir: &Path) -> Result<Option<PartRoleIndex>> {
    let candidates = [
        input_dir.join("Part_function.json"),
        input_dir.join("part_function.json"),
        input_dir.join("part-role.json"),
        input_dir.join("part_role.json"),
    ];

    let source = candidates.into_iter().find(|p| p.exists());
    let Some(path) = source else {
        return Ok(None);
    };

    let raw = fs::read_to_string(&path)
        .with_context(|| format!("read part role json failed: {}", path.display()))?;

    let records: Vec<PartFunctionRecord> = match serde_json::from_str(&raw) {
        Ok(v) => v,
        Err(_) => {
            let names: Vec<String> = regex::Regex::new(r#""item_name"\s*:\s*"([^"]*)""#)
                .ok()
                .map(|re| {
                    re.captures_iter(&raw)
                        .filter_map(|c| c.get(1).map(|m| m.as_str().to_string()))
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            let descs: Vec<String> = regex::Regex::new(r#""description"\s*:\s*"([^"]*)""#)
                .ok()
                .map(|re| {
                    re.captures_iter(&raw)
                        .filter_map(|c| c.get(1).map(|m| m.as_str().to_string()))
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();

            let size = names.len().min(descs.len());
            let mut out = Vec::with_capacity(size);
            for i in 0..size {
                out.push(PartFunctionRecord {
                    item_name: names[i].clone(),
                    description: descs[i].clone(),
                });
            }
            out
        }
    };

    if records.is_empty() {
        return Ok(None);
    }

    let mut index = PartRoleIndex::default();
    for r in records {
        let name = r.item_name.trim();
        let role = r.description.trim();
        if name.is_empty() || role.is_empty() {
            continue;
        }
        index.insert(name, role);
    }

    if index.items.is_empty() {
        return Ok(None);
    }

    Ok(Some(index))
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
    let compacted = prune_empty_replacement_rows_docx(xml, &values);
    patch_paragraph_text_runs_docx(&compacted, &values)
}

fn patch_paragraph_text_runs_docx(xml: &str, values: &BTreeMap<&'static str, String>) -> String {
    let ranges = find_tag_ranges_docx(xml, "w:p");
    if ranges.is_empty() {
        return xml.to_string();
    }

    let mut out = String::with_capacity(xml.len() + 1024);
    let mut cursor = 0usize;

    for (p_start, p_end) in ranges {
        out.push_str(&xml[cursor..p_start]);
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
        let mut t_start = cursor + t_start_rel;
        // Exact tag only: <w:t ...> or <w:t>, not <w:tc>, <w:tbl>, <w:tab>...
        loop {
            let next = xml[t_start + "<w:t".len()..].chars().next();
            let is_exact = matches!(next, Some('>') | Some(' ') | Some('\t') | Some('\r') | Some('\n'));
            if is_exact {
                break;
            }
            let retry_from = t_start + 1;
            let Some(next_rel) = xml[retry_from..].find("<w:t") else {
                return slots;
            };
            t_start = retry_from + next_rel;
        }

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

fn prune_empty_replacement_rows_docx(xml: &str, values: &BTreeMap<&'static str, String>) -> String {
    let row_ranges = find_tag_ranges_docx(xml, "w:tr");
    if row_ranges.is_empty() {
        return xml.to_string();
    }

    let token_re = match regex::Regex::new(r"\{\{\s*([^}]+?)\s*\}\}") {
        Ok(v) => v,
        Err(_) => return xml.to_string(),
    };

    let mut out = String::with_capacity(xml.len());
    let mut cursor = 0usize;
    for (start, end) in row_ranges {
        out.push_str(&xml[cursor..start]);
        let row_xml = &xml[start..end];
        let plain = extract_plain_text_docx(row_xml);
        let is_replacement_row =
            plain.contains("{{날짜") || plain.contains("{{호기") || plain.contains("{{교체수량");

        if !is_replacement_row {
            out.push_str(row_xml);
            cursor = end;
            continue;
        }

        let mut keep = false;
        for cap in token_re.captures_iter(&plain) {
            let Some(m) = cap.get(1) else {
                continue;
            };
            let key = m.as_str().trim();
            if let Some(v) = values.get(key) {
                if !v.trim().is_empty() {
                    keep = true;
                    break;
                }
            }
        }

        if keep {
            out.push_str(row_xml);
        }
        cursor = end;
    }

    out.push_str(&xml[cursor..]);
    out
}

fn extract_plain_text_docx(xml: &str) -> String {
    let slots = find_text_slots_docx(xml);
    if slots.is_empty() {
        return String::new();
    }
    let mut out = String::new();
    for (s, e) in slots {
        out.push_str(&xml_unescape_docx(&xml[s..e]));
    }
    out
}

fn find_tag_ranges_docx(xml: &str, tag: &str) -> Vec<(usize, usize)> {
    let mut out = Vec::new();
    let start_pat = format!("<{tag}");
    let end_pat = format!("</{tag}>");
    let mut cursor = 0usize;

    while let Some(start_rel) = xml[cursor..].find(&start_pat) {
        let mut start = cursor + start_rel;
        // Ensure exact tag match: <w:tr ...> or <w:tr>, but not <w:trPr>.
        loop {
            let next = xml[start + start_pat.len()..].chars().next();
            let is_exact = matches!(next, Some('>') | Some(' ') | Some('\t') | Some('\r') | Some('\n'));
            if is_exact {
                break;
            }
            let retry_from = start + 1;
            let Some(next_rel) = xml[retry_from..].find(&start_pat) else {
                return out;
            };
            start = retry_from + next_rel;
        }

        let Some(end_rel) = xml[start..].find(&end_pat) else {
            break;
        };
        let end = start + end_rel + end_pat.len();
        out.push((start, end));
        cursor = end;
    }
    out
}

fn build_docx_values(row: &DocumentRow, serial: usize) -> BTreeMap<&'static str, String> {
    let now = Local::now();
    let today = now.format("%Y-%m-%d").to_string();
    let doc_date = now.format("%Y%m%d").to_string();
    let vendor = if is_missing_doc_value(&row.vendor_name) {
        "(직접입력)".to_string()
    } else {
        row.vendor_name.clone()
    };
    let manufacturer = if is_missing_doc_value(&row.manufacturer_name) {
        "(직접입력)".to_string()
    } else {
        row.manufacturer_name.clone()
    };
    let unit = if is_missing_doc_value(&row.unit) {
        "(직접입력)".to_string()
    } else {
        row.unit.clone()
    };
    let purchase_reason = build_purchase_reason_text(row);
    let target_where = if is_missing_doc_value(&row.used_where) {
        "(직접입력)".to_string()
    } else {
        row.used_where.clone()
    };
    let purchase_qty = format!("{:.0}", row.purchase_qty.max(1.0));
    let purchase_qty_num = row.purchase_qty.max(1.0);
    let unit_price_num = parse_numeric_text(&row.unit_price);
    let unit_price_text = unit_price_num
        .map(|n| format_price_docx(&format!("{:.2}", n)))
        .unwrap_or_else(|| "(직접입력)".to_string());
    let supply_amount_text = unit_price_num
        .map(|p| format_price_docx(&format!("{:.2}", p * purchase_qty_num)))
        .unwrap_or_else(|| "(직접입력)".to_string());
    let mut m = BTreeMap::new();
    m.insert("번호", row.part_no.clone());
    m.insert("문서번호", format!("DOC-{}-{:04}", doc_date, serial));
    m.insert("작성일자", today.clone());
    m.insert("제목", format!("부품 구매 요청 - {} ({})", row.part_name, row.part_no));
    m.insert("품목", row.part_name.clone());
    m.insert("부품명", row.part_name.clone());
    m.insert("품번", row.part_no.clone());
    m.insert("파트넘버", row.part_no.clone());
    m.insert("장비범주", target_where.clone());
    m.insert("장비", target_where.clone());
    m.insert("장비명", target_where.clone());
    m.insert("현재고", format!("{:.0}", row.current_stock_before));
    m.insert("재고", format!("{:.0}", row.current_stock_before));
    m.insert("구매량", purchase_qty);
    m.insert("구매수량", format!("{:.0}", purchase_qty_num));
    m.insert("교체수량1", row.replacement_qtys[0].clone());
    m.insert("교체수량2", row.replacement_qtys[1].clone());
    m.insert("교체수량3", row.replacement_qtys[2].clone());
    m.insert("교체수량4", row.replacement_qtys[3].clone());
    m.insert("교체수량5", row.replacement_qtys[4].clone());
    m.insert("교체수량6", row.replacement_qtys[5].clone());
    m.insert("날짜1", row.replacement_dates[0].clone());
    m.insert("날짜2", row.replacement_dates[1].clone());
    m.insert("날짜3", row.replacement_dates[2].clone());
    m.insert("날짜4", row.replacement_dates[3].clone());
    m.insert("날짜5", row.replacement_dates[4].clone());
    m.insert("날짜6", row.replacement_dates[5].clone());
    m.insert("대상장비", target_where);
    m.insert("호기1", row.replacement_hosts[0].clone());
    m.insert("호기2", row.replacement_hosts[1].clone());
    m.insert("호기3", row.replacement_hosts[2].clone());
    m.insert("호기4", row.replacement_hosts[3].clone());
    m.insert("호기5", row.replacement_hosts[4].clone());
    m.insert("호기6", row.replacement_hosts[5].clone());
    m.insert("부품-장착-수량", row.issued_qty.clone());
    m.insert("단위", unit);
    m.insert("단가", unit_price_text.clone());
    m.insert("사유", "(직접입력)".to_string());
    m.insert("구매사유", purchase_reason.clone());
    m.insert("비고", purchase_reason.clone());
    m.insert("구-거래처", vendor.clone());
    m.insert("구거래처", vendor.clone());
    m.insert("부품제조사", manufacturer);
    let supplier = m
        .get("구-거래처")
        .cloned()
        .unwrap_or_else(|| "(직접입력)".to_string());
    m.insert("공급업체", supplier);

    m.insert("관련사진1", "(직접기입)".to_string());
    m.insert("관련사진2", "(직접기입)".to_string());
    m.insert("1번설명", "(직접입력)".to_string());
    m.insert("2번설명", "(직접입력)".to_string());
    m.insert("부품-원리-및-역할", row.part_role.clone());
    m.insert("부품역할", row.part_role.clone());
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
    m.insert("현황-및-문제점-1)", purchase_reason.clone());
    m.insert("현황-및-문제점-2", "(직접입력)".to_string());
    m.insert("이후-진행사항", "(직접입력)".to_string());
    m.insert("납기기간", "(직접입력)".to_string());
    m.insert("비밀여부", "(직접입력)".to_string());
    m.insert("첨부파일", "(직접입력)".to_string());
    m.insert("새-거래처", "(직접입력)".to_string());
    m.insert("신단가", "(직접입력)".to_string());
    m.insert("공급액", "(직접입력)".to_string());
    m.insert("공급가액", supply_amount_text.clone());
    m.insert("공급가액합계", supply_amount_text.clone());
    m.insert("합계", supply_amount_text);
    m.insert("구단가", unit_price_text);
    m.insert("지급조건", "(직접입력)".to_string());
    m.insert("교체일", row.replacement_dates[0].clone());
    m.insert("교체일2", row.replacement_dates[1].clone());
    m.insert("교체장비호기", row.replacement_hosts[0].clone());
    m.insert("교체장비호기2", row.replacement_hosts[1].clone());
    m.insert("총 교체수량", row.issued_qty.clone());
    m.insert("교체내역 유무", if row.has_replacement_history { "유".to_string() } else { "무".to_string() });
    m.insert("수리진행여부", "(직접입력)".to_string());
    m.insert("사용일", row.used_date_last.clone());
    m.insert("입고일", row.received_date.clone());
    m.insert("사용처", row.used_where.clone());
    m.insert("문제점", "(직접입력)".to_string());
    m.insert("파트키", row.part_key.clone());
    m
}

fn build_purchase_reason_text(row: &DocumentRow) -> String {
    let req = row.required_stock.unwrap_or(0.0).max(0.0);
    let cur = row.current_stock_before.max(0.0);
    if req > 0.0 {
        format!(
            "해당 부품은 {}부품으로서 필수재고 {:.0}개중, 현재고 {:.0}개로 재고확보를 위한 부품 구매 신청",
            row.part_name, req, cur
        )
    } else {
        format!(
            "해당 부품은 {} 부품으로서 현재고 {:.0}개로 재고확보를 위한 부품 구매 신청",
            row.part_name, cur
        )
    }
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

fn build_unique_docx_targets_by_group(outdir: &Path, rows: &[DocumentRow]) -> Vec<PathBuf> {
    let over_dir_yes = outdir.join("over_500k").join("history_yes");
    let over_dir_no = outdir.join("over_500k").join("history_no");
    let under_dir_yes = outdir.join("under_eq_500k").join("history_yes");
    let under_dir_no = outdir.join("under_eq_500k").join("history_no");

    let mut reserved_over_yes = load_reserved_filenames(&over_dir_yes);
    let mut reserved_over_no = load_reserved_filenames(&over_dir_no);
    let mut reserved_under_yes = load_reserved_filenames(&under_dir_yes);
    let mut reserved_under_no = load_reserved_filenames(&under_dir_no);
    let mut out = Vec::with_capacity(rows.len());

    for (idx, row) in rows.iter().enumerate() {
        let (target_dir, reserved) = match row.template_kind {
            PurchaseTemplateKind::Over500k => {
                if row.has_replacement_history {
                    (&over_dir_yes, &mut reserved_over_yes)
                } else {
                    (&over_dir_no, &mut reserved_over_no)
                }
            }
            PurchaseTemplateKind::UnderEq500k => {
                if row.has_replacement_history {
                    (&under_dir_yes, &mut reserved_under_yes)
                } else {
                    (&under_dir_no, &mut reserved_under_no)
                }
            }
        };

        let base = sanitize_docx_filename(&row.part_name);
        let part_no = sanitize_docx_filename(&row.part_no);
        // Include part_no by default to prevent wrong-doc selection when names duplicate.
        let key = format!("기안문_({})_{}", base, part_no);

        let mut cand = format!("{key}.docx");
        if reserved.contains(&canonical_filename_key(&cand)) {
            cand = format!("{key}_{:04}.docx", idx + 1);
        }

        let mut seq = idx + 1;
        while reserved.contains(&canonical_filename_key(&cand)) {
            seq += 1;
            cand = format!("{key}_{:04}.docx", seq);
        }

        reserved.insert(canonical_filename_key(&cand));
        out.push(target_dir.join(cand));
    }

    out
}

fn load_reserved_filenames(dir: &Path) -> HashSet<String> {
    let mut reserved = HashSet::new();
    if let Ok(rd) = fs::read_dir(dir) {
        for ent in rd.flatten() {
            if let Some(name) = ent.file_name().to_str() {
                reserved.insert(canonical_filename_key(name));
            }
        }
    }
    reserved
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
        let frac_trimmed = frac.trim_end_matches('0');
        if !frac_trimmed.is_empty() {
            grouped.push('.');
            grouped.push_str(frac_trimmed);
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
    let f = File::create(&report_path)
        .with_context(|| format!("report file create failed: {}", report_path.display()))?;
    let mut w = BufWriter::new(f);
    serde_json::to_writer_pretty(&mut w, report).context("write batch report failed")?;
    writeln!(&mut w).ok();
    w.flush().ok();
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

    let out = rows[1..]
        .par_iter()
        .filter_map(|row| {
            if !is_effective_row(row) {
                return None;
            }
            let date_raw = get_str_at(row, date_idx);
            let date_iso = date_raw.as_deref().and_then(normalize_date_to_iso);
            Some(RowRecord {
                columns: extract_row_columns(row, &header),
                part_no: get_str_at(row, part_no_idx),
                part_name: get_str_at(row, part_name_idx),
                qty: get_f64_at(row, qty_idx),
                date: date_raw,
                date_iso,
                source_file: source_file.clone(),
            })
        })
        .collect();
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

    let out = rows[1..]
        .par_iter()
        .filter_map(|row| {
            if !is_effective_row(row) {
                return None;
            }
            let date_raw = get_str_at(row, date_idx);
            let date_iso = date_raw.as_deref().and_then(normalize_date_to_iso);
            Some(RowRecord {
                columns: extract_row_columns(row, &header),
                part_no: get_str_at(row, part_no_idx),
                part_name: get_str_at(row, part_name_idx),
                qty: get_f64_at(row, qty_idx),
                date: date_raw,
                date_iso,
                source_file: source_file.clone(),
            })
        })
        .collect();
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

    let out = rows[1..]
        .par_iter()
        .filter_map(|row| {
            if !is_effective_row(row) {
                return None;
            }
            Some(StockRecord {
                columns: extract_row_columns(row, &header),
                part_no: get_str_at(row, part_no_idx),
                part_name: get_str_at(row, part_name_idx),
                stock_qty: get_f64_at(row, qty_idx),
                source_file: source_file.clone(),
            })
        })
        .collect();
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

    if let Some(d) = fast_parse_date(trimmed) {
        return Some(d.format("%Y-%m-%d").to_string());
    }

    if let Ok(serial) = trimmed.parse::<f64>() {
        return excel_serial_to_iso(serial);
    }

    for fmt in DATE_PARSE_FORMATS {
        if let Ok(d) = NaiveDate::parse_from_str(trimmed, fmt) {
            return Some(d.format("%Y-%m-%d").to_string());
        }
    }

    None
}

fn fast_parse_date(s: &str) -> Option<NaiveDate> {
    // Fast path for YYYY[-/.]MM[-/.]DD and M/D/YYYY
    if s.len() == 10 {
        let b = s.as_bytes();
        let sep = b[4] as char;
        if (sep == '-' || sep == '/' || sep == '.') && b[7] == b[4] {
            let y = parse_u32_ascii(&s[0..4])?;
            let m = parse_u32_ascii(&s[5..7])?;
            let d = parse_u32_ascii(&s[8..10])?;
            return NaiveDate::from_ymd_opt(y as i32, m, d);
        }
    }

    if s.contains('/') {
        let mut it = s.split('/');
        let m = it.next()?;
        let d = it.next()?;
        let y = it.next()?;
        if it.next().is_none()
            && (1..=2).contains(&m.len())
            && (1..=2).contains(&d.len())
            && y.len() == 4
        {
            let y = parse_u32_ascii(y)?;
            let m = parse_u32_ascii(m)?;
            let d = parse_u32_ascii(d)?;
            return NaiveDate::from_ymd_opt(y as i32, m, d);
        }
    }

    None
}

fn parse_u32_ascii(s: &str) -> Option<u32> {
    if s.is_empty() || !s.bytes().all(|c| c.is_ascii_digit()) {
        return None;
    }
    s.parse::<u32>().ok()
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











