# Doc Purchase Pipeline

Excel(입고/재고/출고) 데이터를 읽어 통합 스냅샷 JSON을 만들고, 구매 품의 DOCX를 생성하는 Rust 배치 앱입니다.

## 기술 스택
- Rust 2024
- `calamine` (Excel 파싱)
- `serde`, `serde_json` (직렬화)
- `rayon` (병렬 처리)
- `zip` (DOCX 패키지/치환)
- `chrono` (날짜/시간)

## 디렉토리
기본 작업 루트는 `./DB`입니다.

- `DB/input`: 입력 Excel + DOCX 템플릿
- `DB/output`: 결과 JSON + 생성 DOCX
- `DB/logs`: 워크플로 로그 + 배치 리포트

## 현재 동작 규칙
1. `input`에서 최신 Excel 3종(입고/재고/출고)을 자동 선택
2. `품명||부품번호` 키로 집계 후 `stock_in_out_monthly.json` 생성
3. 구매 조건(활성화됨):
 - `현재고 < 필수재고량`
 - `현재고 <= 필수재고량의 30%`
4. 단가 분기:
 - `>= 500,000`: 구매 요청 품의 템플릿
 - `< 500,000`: 구매 품의 템플릿
5. 500K 이상 문서는 교체이력 유/무에 따라 템플릿 분기
6. 교체이력은 수량 기반 플롯(출고 수량 `N` => 이력 `N`건)으로 반영
7. DOCX 출력 폴더:
 - `over_500k/history_yes`
 - `over_500k/history_no`
 - `under_eq_500k`

참고:
- 템플릿 파일명은 `src/main.rs` 상수(`TEMPLATE_*`)로 관리됩니다.
- README에는 특정 회사명/브랜드명을 노출하지 않습니다.

## 실행
프로젝트 루트(`app`) 기준:

```powershell
# 1회 실행
cargo run --bin fin_rust_app -- --once

# 감시 모드
cargo run --bin fin_rust_app

# 1회 + 스모크
cargo run --bin fin_rust_app -- --once --smoke

# 작업 루트 지정
cargo run --bin fin_rust_app -- --workdir ./DB --once
```

레거시(직접 파일 지정):
```powershell
cargo run --bin fin_rust_app -- <inbound.xlsx> <stock.xlsx> <outbound.xlsx> <out.json>
```

## 산출물
- `DB/output/stock_in_out_monthly.json`
- `DB/output/<YYYY-MM-DD>/...docx`
- `DB/logs/workflow.log`
- `DB/logs/batch_report_*.json`

## 운영 메모
- 중복 배치 방지용 fingerprint: `DB/logs/last_batch_fingerprint.txt`
- 수동 강제 재실행 시 해당 파일 삭제 후 `--once` 실행
- 코드 변경 후 빠른 검증:
```powershell
cargo check --bin fin_rust_app
```
