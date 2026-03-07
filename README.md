# Doc Automation Pipeline

Excel 3종(입고/재고/출고)을 자동 감지해 통합 스냅샷 JSON을 만들고, 구매 품의 DOCX를 생성하는 Rust 배치 애플리케이션입니다.

## 1. 현재 구현 요약
- 입력 폴더(`DB/input`)에서 최신 Excel 3종을 자동 선택
- 부품 키(`품명||부품번호`) 기준으로 입고/재고/출고 데이터를 통합
- 구매 진행 여부를 `필수재고량`, `현재고`, `단가` 기준으로 판단
- 50만원 기준 + 교체이력 유/무에 따라 템플릿 분기
- DOCX 토큰(`{{키}}`) 치환 및 교체이력 빈 행 동적 제거
- 실행 리포트/로그/중복방지 fingerprint 기록

## 2. 기술 스택
- Rust 2024
- calamine (Excel 읽기)
- serde / serde_json (JSON 직렬화)
- rayon (병렬 처리)
- zip (DOCX 패키지 읽기/쓰기)
- chrono (날짜 처리)
- regex (템플릿 토큰/보정 파싱)

## 3. 디렉토리 구조
기본 작업 루트는 `./DB` 입니다.

- `DB/input`
  - Excel 입력 파일 3종
  - `문서제작양식/*.docx` (템플릿)
  - `Part_function.json` (선택)
- `DB/output`
  - `stock_in_out_monthly.json`
  - `<YYYY-MM-DD>/over_500k/history_yes|no/*.docx`
  - `<YYYY-MM-DD>/under_eq_500k/history_yes|no/*.docx`
- `DB/logs`
  - `workflow.log`
  - `batch_report_*.json`
  - `last_batch_fingerprint.txt`

## 4. 입력 파일 감지 규칙
### 4.1 자동 감지
파일명 키워드로 종류를 판별합니다.

- 입고: `입고` 또는 `inbound`
- 재고: `재고` 또는 `stock`
- 출고: `출고` 또는 `outbound`

3종이 모두 있어야 배치가 실행됩니다.

### 4.2 안정성 체크
감지된 3개 파일에 대해 1초 간격으로 `파일 크기/수정시간`을 다시 확인합니다.
변동이 있으면 해당 루프에서는 실행하지 않고 대기합니다.

## 5. 데이터 통합/계산 규칙
### 5.1 부품 키
`part_key = 품명 || 부품번호`

- 품명이 비어 있으면 `PART_<part_no>` 사용
- 품번도 없으면 `UNKNOWN_PART||NO_PART_NO`

### 5.2 재고 계산 순서
1. `current_stock_before`
2. `inbound_plus_stock = current_stock_before + inbound_qty_sum`
3. `current_stock_updated = inbound_plus_stock - outbound_qty_sum`

스냅샷 JSON(`output/stock_in_out_monthly.json`)의 `meta.calc_order`에도 동일 순서로 기록됩니다.

## 6. 구매 판단 규칙(V2 활성)
`ENABLE_PURCHASE_DECISION_V2 = true` 기준입니다.

구매 진행 조건:
1. `필수재고량` 값이 존재하고 0보다 커야 함
2. `현재고 < 필수재고량`
3. `현재고 <= 필수재고량 * 0.3`

위 조건을 모두 통과한 경우만 구매 대상이 됩니다.

단가 분기:
- `단가 >= 500,000` -> `Over500k` (부품 구매 요청 품의)
- `단가 < 500,000` -> `UnderEq500k` (부품 구매 품의)

## 7. 템플릿 분기 규칙
템플릿 기본 위치: `DB/input/문서제작양식`

### 7.1 파일명 상수(현재 코드 기준)
- 50만원 이상 + 교체이력 유: `부품구매요청_교체이럭_유.docx`
- 50만원 이상 + 교체이력 무: `부품구매요청_교체이력_무.docx`
- 50만원 이하 + 교체이력 유: `부품구매_교체이력_유.docx`
- 50만원 이하 + 교체이력 무: `부품구매_교체이력_무.docx`

### 7.2 fallback
- 분기 템플릿이 없으면 legacy 템플릿으로 fallback
- 특정 그룹 템플릿이 없을 때는 다른 그룹 템플릿으로 최종 fallback

## 8. 교체이력 처리
### 8.1 수량 기반 플롯
출고 1행의 `지급량 = N`이면, 교체이력 포인트를 `N`건으로 확장(각 수량 1)합니다.
최신순 기준 최대 6건만 문서에 반영됩니다.

### 8.2 배열 배치 순서
템플릿 좌/우 2열 구조에 맞춰 인덱스 순서 `[1,4,2,5,3,6]` 형태로 채웁니다.

### 8.3 빈 행 제거
교체이력 토큰(`{{날짜*}}`, `{{호기*}}`, `{{교체수량*}}`) 값이 전부 비어 있는 행은 DOCX XML 레벨에서 제거합니다.

## 9. 컬럼 매핑 핵심
### 9.1 거래처(공급업체)
우선순위:
- `납품업체`, `납품업체명`, `거래처`, `업체`, `공급업체`, `구매업체`

### 9.2 제조사(`{{부품제조사}}`)
재고 데이터 기준 우선순위:
- `주요Model명`, `Model명`, `부품제조사`, `부품 제조사`

제조사 후보값은 필터링됩니다.
- 알파벳은 있고 숫자가 섞인 토큰은 제외
- 적합값이 없으면 `기록없음` -> 문서 치환 시 `(직접입력)`

### 9.3 구매사유 문구
현재 문구 형식:
- `해당 부품은 {부품명}부품으로서 필수재고 {필수재고}개중, 현재고 {현재고}개로 재고확보를 위한 부품 구매 신청`

`구매사유`, `비고`, `현황-및-문제점-1)`에 동일 문구가 반영됩니다.

## 10. 실행 방법
프로젝트 루트(`app`) 기준.

### 10.1 1회 실행
```powershell
cargo run --bin fin_rust_app -- --once
```

### 10.2 감시 모드(2초 polling)
```powershell
cargo run --bin fin_rust_app
```

### 10.3 스모크 텍스트 출력 포함
```powershell
cargo run --bin fin_rust_app -- --once --smoke
```

### 10.4 workdir 지정
```powershell
cargo run --bin fin_rust_app -- --workdir ./DB --once
```

### 10.5 fingerprint 무시하고 강제 재실행
```powershell
if (Test-Path 'DB\logs\last_batch_fingerprint.txt') { Remove-Item 'DB\logs\last_batch_fingerprint.txt' -Force }
cargo run --bin fin_rust_app -- --once
```

### 10.6 legacy 모드(파일 직접 지정)
```powershell
cargo run --bin fin_rust_app -- <inbound.xlsx> <stock.xlsx> <outbound.xlsx> <out.json>
```

## 11. 산출물
- `DB/output/stock_in_out_monthly.json`
- `DB/output/<YYYY-MM-DD>/**/*.docx`
- `DB/logs/workflow.log`
- `DB/logs/batch_report_*.json`

## 12. 트러블슈팅
### 12.1 DOCX가 생성되지 않음
- `DB/input/문서제작양식` 템플릿 파일명 확인
- 로그의 template fallback 메시지 확인

### 12.2 배치가 계속 대기 상태
- 파일명에 `입고/재고/출고` 키워드가 있는지 확인
- 업로드 중 파일로 판단되어 unstable 상태일 수 있으니 저장 완료 후 재시도

### 12.3 같은 배치가 재실행되지 않음
- `DB/logs/last_batch_fingerprint.txt` 삭제 후 `--once` 실행

### 12.4 금액 포맷
- `format_price_docx`에서 천단위 콤마 적용
- 소수점 `.00`은 제거

## 13. 개발 메모
빠른 검증:
```powershell
cargo check --bin fin_rust_app
```

핵심 구현 파일:
- `src/main.rs`
