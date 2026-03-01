# Hanjin-Doc

Excel(입고/재고/출고) 데이터를 읽어 통합 JSON 스냅샷을 만들고, DOCX 기안문을 자동 생성하는 Rust 배치 파이프라인입니다.

## 1. 기술 스택
- Rust 2024
- `calamine` (Excel 파싱)
- `serde`, `serde_json` (데이터 직렬화)
- `rayon` (병렬 처리)
- `zip` (DOCX 템플릿 치환)
- `chrono` (날짜/시간)

## 2. 디렉토리 규칙 (상대경로)
작업 루트는 기본 `./DB` 입니다.

- `DB/input`: 입력 Excel + DOCX 템플릿
- `DB/output`: 통합 JSON, 생성 DOCX, 스모크 결과
- `DB/logs`: 배치 리포트/워크플로우 로그

예시:
```text
app/
  src/
  DB/
    input/
    output/
    logs/
```

## 3. 파이프라인
1. `input`에서 최신 Excel 3종 탐지 (입고/재고/출고)
2. 행 단위 raw 컬럼 보존 + 파트 키(`품명||부품번호`) 기준 집계
3. `stock_in_out_monthly.json` 생성
4. 문서 row 생성
5. DOCX 템플릿 placeholder 치환 후 파일 생성
6. 배치 로그/리포트 기록

## 4. 실행 방법
프로젝트 루트(`app`) 기준입니다.

### 4.1 1회 배치 실행 (권장)
```powershell
cargo run --release --bin fin_rust_app -- --workdir ./DB --once
```

### 4.2 감시 모드 실행 (지속 루프)
```powershell
cargo run --release --bin fin_rust_app -- --workdir ./DB
```

### 4.3 1회 배치 + 스모크 출력 ON (선택)
```powershell
cargo run --release --bin fin_rust_app -- --workdir ./DB --once --smoke
```

### 4.4 레거시 모드 (파일 직접 지정)
```powershell
cargo run --release --bin fin_rust_app -- <inbound.xlsx> <stock.xlsx> <outbound.xlsx> <out.json>
```

## 5. 입력 파일 규칙
- Excel 확장자: `.xlsx`, `.xlsm`, `.xls`
- 파일명에서 종류 판별:
  - `입고` 또는 `inbound` 포함 -> 입고
  - `재고` 또는 `stock` 포함 -> 재고
  - `출고` 또는 `outbound` 포함 -> 출고

운영 시에는 각 종류별 최신 파일 1개씩 사용합니다.

## 6. 주요 산출물
- `DB/output/stock_in_out_monthly.json`
- `DB/output/<YYYY-MM-DD>/기안문_(부품명...).docx`
- `DB/output/docx_smoke_test_*.txt` (`--smoke` 옵션 사용 시)
- `DB/logs/workflow.log`
- `DB/logs/batch_report_*.json`

## 7. 성능/운영 메모
- release 빌드 기준 사용 권장 (`--release`)
- 중복 배치 방지를 위해 마지막 처리 fingerprint 기록
- 파일 복사 중 상태 감지를 위해 안정성 체크 후 처리
- `main.rs`에 일일 05:00 스케줄러 스켈레톤이 주석으로 포함되어 있어, 필요 시 활성화 가능

## 8. Git 운영 정책
이 저장소는 코드 전용으로 운영합니다.
- 포함: `src`, `Cargo.toml`, `Cargo.lock`, `README.md`
- 제외: `DB/**` (입력/출력/로그 산출물)

---
문의/변경 요청은 이슈 또는 커밋 메시지 기준으로 반영합니다.
