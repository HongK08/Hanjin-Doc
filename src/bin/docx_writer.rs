use anyhow::{Context, Result};
use chrono::Local;
use serde::Deserialize;
use std::collections::{BTreeMap, HashSet};
use std::env;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use zip::write::SimpleFileOptions;
use zip::{CompressionMethod, ZipArchive, ZipWriter};

#[derive(Debug, Deserialize)]
struct DocumentDraft {
    rows: Vec<DocumentRow>,
}

#[derive(Debug, Deserialize)]
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

struct Config {
    template: PathBuf,
    draft: PathBuf,
    outdir_base: PathBuf,
    limit: Option<usize>,
}

impl Config {
    fn from_args() -> Self {
        let mut template = PathBuf::from("./DB/input/한진_기안문_양식.docx");
        let mut draft = PathBuf::from("./DB/output/document_draft_20260301_213058.json");
        let mut outdir_base = PathBuf::from("./DB/output");
        let mut limit = None;

        let args: Vec<String> = env::args().collect();
        let mut i = 1;
        while i < args.len() {
            match args[i].as_str() {
                "--template" => {
                    if let Some(v) = args.get(i + 1) {
                        template = PathBuf::from(v);
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--draft" => {
                    if let Some(v) = args.get(i + 1) {
                        draft = PathBuf::from(v);
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--outdir" => {
                    if let Some(v) = args.get(i + 1) {
                        outdir_base = PathBuf::from(v);
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
                _ => i += 1,
            }
        }

        Self {
            template,
            draft,
            outdir_base,
            limit,
        }
    }
}

fn main() -> Result<()> {
    let cfg = Config::from_args();
    let date_dir = Local::now().format("%Y-%m-%d").to_string();
    let outdir = cfg.outdir_base.join(date_dir);
    fs::create_dir_all(&outdir)
        .with_context(|| format!("create outdir failed: {}", outdir.display()))?;

    let draft: DocumentDraft = serde_json::from_reader(
        File::open(&cfg.draft).with_context(|| format!("open draft failed: {}", cfg.draft.display()))?,
    )
    .with_context(|| format!("parse draft failed: {}", cfg.draft.display()))?;

    let rows = if let Some(n) = cfg.limit {
        draft.rows.into_iter().take(n).collect::<Vec<_>>()
    } else {
        draft.rows
    };

    let mut generated = 0usize;
    for (idx, row) in rows.iter().enumerate() {
        let output = unique_output_path(&outdir, row, idx + 1);
        render_docx(&cfg.template, &output, row, idx + 1)?;
        generated += 1;
    }

    println!("docx_writer done");
    println!("template: {}", cfg.template.display());
    println!("draft: {}", cfg.draft.display());
    println!("outdir: {}", outdir.display());
    println!("generated: {}", generated);
    Ok(())
}

fn render_docx(template: &Path, output: &Path, row: &DocumentRow, serial: usize) -> Result<()> {
    let tf = File::open(template).with_context(|| format!("open template failed: {}", template.display()))?;
    let mut zin = ZipArchive::new(tf).context("open template zip failed")?;

    let of = File::create(output).with_context(|| format!("create output failed: {}", output.display()))?;
    let mut zout = ZipWriter::new(of);

    for i in 0..zin.len() {
        let mut entry = zin.by_index(i)?;
        let name = entry.name().to_string();
        let is_dir = entry.is_dir();
        let options = SimpleFileOptions::default()
            .compression_method(entry.compression())
            .unix_permissions(0o644);

        if is_dir {
            zout.add_directory(name, options)?;
            continue;
        }

        let mut buf = Vec::new();
        entry.read_to_end(&mut buf)?;

        if name == "word/document.xml" {
            let xml = String::from_utf8(buf).context("document.xml is not utf8")?;
            let patched = patch_document_xml(&xml, row, serial);
            zout.start_file(name, options.compression_method(CompressionMethod::Deflated))?;
            zout.write_all(patched.as_bytes())?;
        } else {
            zout.start_file(name, options)?;
            zout.write_all(&buf)?;
        }
    }

    zout.finish()?;
    Ok(())
}

fn patch_document_xml(xml: &str, row: &DocumentRow, serial: usize) -> String {
    let values = build_values(row, serial);
    patch_paragraph_text_runs(xml, &values)
}

fn patch_paragraph_text_runs(xml: &str, values: &BTreeMap<&'static str, String>) -> String {
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
        out.push_str(&patch_one_paragraph(paragraph, values));
        cursor = p_end;
    }

    out.push_str(&xml[cursor..]);
    out
}

fn patch_one_paragraph(p_xml: &str, values: &BTreeMap<&'static str, String>) -> String {
    let slots = find_text_slots(p_xml);
    if slots.is_empty() {
        return p_xml.to_string();
    }

    let mut plain = String::new();
    for (s, e) in &slots {
        plain.push_str(&xml_unescape(&p_xml[*s..*e]));
    }

    let replaced = replace_tokens_in_text(&plain, values);
    if replaced == plain {
        return p_xml.to_string();
    }

    let mut out = String::with_capacity(p_xml.len() + 64);
    let mut last = 0usize;
    for (idx, (s, e)) in slots.iter().enumerate() {
        out.push_str(&p_xml[last..*s]);
        if idx == 0 {
            out.push_str(&xml_escape(&replaced));
        }
        last = *e;
    }
    out.push_str(&p_xml[last..]);
    reduce_font_size_tags(&out)
}

fn find_text_slots(xml: &str) -> Vec<(usize, usize)> {
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

fn replace_tokens_in_text(text: &str, values: &BTreeMap<&'static str, String>) -> String {
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

fn build_values(row: &DocumentRow, serial: usize) -> BTreeMap<&'static str, String> {
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
    m.insert("구단가", format_price(&row.unit_price));
    m.insert("지급조건", "(직접입력)".to_string());

    m.insert("사용일", row.used_date_last.clone());
    m.insert("입고일", row.received_date.clone());
    m.insert("사용처", row.used_where.clone());
    m.insert("문제점", row.usage_reason.clone());
    m.insert("파트키", row.part_key.clone());
    m
}

fn xml_escape(v: &str) -> String {
    v.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

fn xml_unescape(v: &str) -> String {
    v.replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&amp;", "&")
}

fn sanitize_filename(s: &str) -> String {
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

fn unique_output_path(outdir: &Path, row: &DocumentRow, serial: usize) -> PathBuf {
    let base = sanitize_filename(&row.part_name);
    let primary = outdir.join(format!("기안문_({}).docx", base));
    if !path_exists_case_insensitive(outdir, primary.file_name().and_then(|s| s.to_str()).unwrap_or_default()) {
        return primary;
    }

    let part_no = sanitize_filename(&row.part_no);
    let with_no = outdir.join(format!("기안문_({})_{}.docx", base, part_no));
    if !path_exists_case_insensitive(outdir, with_no.file_name().and_then(|s| s.to_str()).unwrap_or_default()) {
        return with_no;
    }

    outdir.join(format!("기안문_({})_{:04}.docx", base, serial))
}

fn path_exists_case_insensitive(dir: &Path, file_name: &str) -> bool {
    let target = canonical_filename_key(file_name);
    let mut set: HashSet<String> = HashSet::new();
    if let Ok(rd) = fs::read_dir(dir) {
        for ent in rd.flatten() {
            if let Some(name) = ent.file_name().to_str() {
                set.insert(canonical_filename_key(name));
            }
        }
    }
    set.contains(&target)
}

fn canonical_filename_key(name: &str) -> String {
    name.trim().to_lowercase()
}

fn format_price(input: &str) -> String {
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

fn reduce_font_size_tags(xml: &str) -> String {
    let mut out = xml.to_string();
    out = reduce_one_size_tag(&out, "<w:sz w:val=\"");
    out = reduce_one_size_tag(&out, "<w:szCs w:val=\"");
    out
}

fn reduce_one_size_tag(xml: &str, marker: &str) -> String {
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
