use anyhow::{Context, Result};
use chrono::Local;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::env;
use std::fs::{self, File};
use std::io::BufWriter;
use std::path::{Path, PathBuf};

#[derive(Debug, Deserialize)]
struct Snapshot {
    meta: SnapshotMeta,
    parts: BTreeMap<String, PartBlock>,
    raw: SnapshotRaw,
}

#[derive(Debug, Deserialize)]
struct SnapshotMeta {
    snapshot_date: String,
}

#[derive(Debug, Deserialize)]
struct SnapshotRaw {
    inbound_rows: Vec<RowRecord>,
    outbound_rows: Vec<RowRecord>,
}

#[derive(Debug, Deserialize)]
struct PartBlock {
    part_no: Option<String>,
    part_name: String,
    current_stock_before: Option<f64>,
    current_stock_updated: Option<f64>,
    inbound_dates: Vec<String>,
    outbound_dates: Vec<String>,
    inbound_qty_sum: f64,
    outbound_qty_sum: f64,
    outbound_count: usize,
    inbound_row_idx: Vec<usize>,
    outbound_row_idx: Vec<usize>,
}

#[derive(Debug, Deserialize)]
struct RowRecord {
    columns: HashMap<String, String>,
    date_iso: Option<String>,
}

#[derive(Debug, Serialize)]
struct DocumentDraft {
    generated_at: String,
    snapshot_date: String,
    total_parts: usize,
    rows: Vec<DocumentRow>,
}

#[derive(Debug, Serialize, Deserialize)]
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
    inbound_qty_sum: f64,
    outbound_qty_sum: f64,
    current_stock_updated: f64,
    purchase_order_needed: bool,
    purchase_order_note: String,
    equipment_no: String,
    model_name: String,
    issued_date: String,
    issued_qty: String,
    vendor_name: String,
    unit: String,
    unit_price: String,
}

fn main() -> Result<()> {
    let cfg = Config::from_args();
    let snapshot = read_snapshot(&cfg.input_json)?;
    let draft = build_document_draft(&snapshot, cfg.limit, cfg.include_no_outbound);

    fs::create_dir_all(&cfg.output_dir)
        .with_context(|| format!("create output dir failed: {}", cfg.output_dir.display()))?;
    let out_name = format!("document_draft_{}.json", Local::now().format("%Y%m%d_%H%M%S"));
    let out_path = cfg.output_dir.join(out_name);

    let w = BufWriter::new(
        File::create(&out_path).with_context(|| format!("create failed: {}", out_path.display()))?,
    );
    serde_json::to_writer_pretty(w, &draft).context("write draft json failed")?;

    println!("doc_writer done");
    println!("input: {}", cfg.input_json.display());
    println!("output: {}", out_path.display());
    println!("rows: {}", draft.rows.len());
    Ok(())
}

struct Config {
    input_json: PathBuf,
    output_dir: PathBuf,
    limit: Option<usize>,
    include_no_outbound: bool,
}

impl Config {
    fn from_args() -> Self {
        let mut input_json = PathBuf::from("./DB/output/stock_in_out_monthly.json");
        let mut output_dir = PathBuf::from("./DB/output");
        let mut limit = None;
        let mut include_no_outbound = false;

        let args: Vec<String> = env::args().collect();
        let mut i = 1;
        while i < args.len() {
            match args[i].as_str() {
                "--input" => {
                    if let Some(v) = args.get(i + 1) {
                        input_json = PathBuf::from(v);
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--outdir" => {
                    if let Some(v) = args.get(i + 1) {
                        output_dir = PathBuf::from(v);
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--limit" => {
                    if let Some(v) = args.get(i + 1) {
                        limit = v.parse::<usize>().ok();
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--include-no-outbound" => {
                    include_no_outbound = true;
                    i += 1;
                }
                _ => i += 1,
            }
        }

        Self {
            input_json,
            output_dir,
            limit,
            include_no_outbound,
        }
    }
}

fn read_snapshot(path: &Path) -> Result<Snapshot> {
    let f = File::open(path).with_context(|| format!("open failed: {}", path.display()))?;
    serde_json::from_reader(f).with_context(|| format!("parse failed: {}", path.display()))
}

fn build_document_draft(
    snapshot: &Snapshot,
    limit: Option<usize>,
    include_no_outbound: bool,
) -> DocumentDraft {
    let mut rows = Vec::new();

    for (part_key, part) in &snapshot.parts {
        if !include_no_outbound && part.outbound_count == 0 {
            continue;
        }

        let part_no = fallback_missing(part.part_no.clone());
        let part_name = fallback_missing(Some(part.part_name.clone()));
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
                used_where = pick_first(&row.columns, &["장비명", "장비번호", "주요장비명", "주요Model명"]);
                usage_reason = pick_first(&row.columns, &["운영구분", "지급구분", "요청번호"]);
                replacement_reason = pick_first(&row.columns, &["지급구분", "운영구분", "요청번호"]);
                equipment_no = pick_first(&row.columns, &["장비번호"]);
                model_name = pick_first(&row.columns, &["Model명"]);
                issued_qty = pick_first(&row.columns, &["지급량"]);
                issued_date = row
                    .date_iso
                    .clone()
                    .unwrap_or_else(|| pick_first(&row.columns, &["지급일자"]));
                unit = pick_first(&row.columns, &["단위"]);
                unit_price = pick_first(&row.columns, &["단가", "구단가"]);
                if let Some(d) = row.date_iso.clone() {
                    used_date_last = d;
                }
            }
        }

        if let Some(in_idx) = part.inbound_row_idx.first() {
            if let Some(in_row) = snapshot.raw.inbound_rows.get(*in_idx) {
                let v = pick_first(&in_row.columns, &["납품업체"]);
                if v != "기록없음" {
                    vendor_name = v;
                }
                if unit == "기록없음" {
                    unit = pick_first(&in_row.columns, &["단위"]);
                }
                if unit_price == "기록없음" {
                    unit_price = pick_first(&in_row.columns, &["단가", "구단가"]);
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
            inbound_qty_sum: part.inbound_qty_sum,
            outbound_qty_sum: part.outbound_qty_sum,
            current_stock_updated,
            purchase_order_needed,
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

    DocumentDraft {
        generated_at: Local::now().to_rfc3339(),
        snapshot_date: snapshot.meta.snapshot_date.clone(),
        total_parts: rows.len(),
        rows,
    }
}

fn pick_first(columns: &HashMap<String, String>, keys: &[&str]) -> String {
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

fn fallback_missing(v: Option<String>) -> String {
    v.map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "기록없음".to_string())
}
