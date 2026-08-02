# 기존 파이프라인 분석과 보존 경계

## 리팩터링 전 구조

기존 `daily_pipeline.py`는 현재·최근 10년 S&P 500 관련 종목과 검토된
ETF·ETP를 사용했습니다. 종목별로 Alpaca SIP 30분봉을 180일 단위로
가져와 XNYS 정규장 1h/4h/1d로 집계했습니다.

문제점:

- 공급자 30분봉을 공식 파일로 보존하지 않았습니다.
- 종목별 10년 파일을 매일 읽고 병합하여 전체 파일을 교체했습니다.
- 체크포인트가 저장 형식·종목·단계와 장기 파일 존재 여부에 묶였습니다.
- 파일 경로가 사실상의 inventory였습니다.
- 파생 파일과 정확한 원천 객체 계보가 없었습니다.
- 증분 파일 Compaction 수명주기가 없었습니다.

## 보존한 동작

- Alpaca 30분봉과 180일 요청 chunk
- RAW와 Adjustment.ALL
- 실패 재시도와 다른 종목 계속 처리
- 비활성 종목을 외부 데이터보다 길게 생성하지 않는 정책
- XNYS DST·휴장·조기 폐장
- 관측된 봉만 집계하는 `source_minutes`
- OHLCV, 중복, 누락과 보고 기능
- 현재·최근 편출입 종목과 ETF 정책
- 기존 파일 무삭제

## 공식 경로에서 변경한 동작

- 공급자 RAW/ADJUSTED 30m를 불변 Parquet으로 publish
- 티커 대신 `instrument_id`로 shard
- 종목별 10년 파일 대신 연도·기간·shard·part 객체
- DAY 증분과 WEEK/MONTH/YEAR Compaction
- Local/S3 ObjectStore와 Local/PostgreSQL Catalog 경계
- DBML 열 이름의 JSONL Catalog
- Manifest가 inventory를 결정하고 prefix listing을 정본으로 사용하지 않음
- 실패 Manifest는 QUARANTINED이며 AVAILABLE로 노출하지 않음

기존 `update_symbol_data`와 종목별 경로 함수는 자동 삭제하지 않고
`--legacy-long-term` 및 마이그레이션 호환을 위해 남겼습니다. 기본
`daily_pipeline.py` 실행은 새 DAY 파이프라인입니다.

## DP1에서 정리한 legacy (2026-08)

참조가 하나도 없던 다음 파일을 삭제했습니다.

- `data_collection/script.py` (기존 5분봉 수집기)
- `data_validation/check_data.py` — 읽을 수 없는 파일과 빈 파일 모두에 대해
  동일한 `{"rows": 0, "start": "N/A", "regular_market_pct": 0.0}` 를 돌려주어
  두 상황을 구분할 수 없었습니다.
- `data_validation/data_report.py` — "생성된" 리포트 안에 특정 종목(ABNB)을
  지목하는 하드코딩 한국어 산문이 포함돼 있었습니다.

계속 남는 legacy와 그 이유:

- `data_collection/collect_sip_1min.py` — `daily_pipeline.py`가 호출하고,
  `InactiveSymbolCache`·`should_skip_inactive_symbol`을
  `market_pipeline_lib/engine.py`가 import합니다.
- `data_collection/collect_sip_long_term.py`, `etf_universe.py`,
  `get_ticker.py` — `daily_pipeline.py` 경로에서 사용합니다.
- `data_validation/audit_regular_session.py` — 구간별 impact scope 산정 로직을
  이후 단계에서 `processing.quality_issues`로 이관할 예정입니다.
- `data_validation/quality_control.py`, `data_filtering/` — 보고서 경로에서 사용합니다.

## DP1에서 고친 조용한 실패

- `data_collection/get_ticker.py` — 네트워크·파싱 실패 시 하드코딩된 7개 티커
  목록을 돌려주었고, `collect_sip_1min.py`가 그 결과로 실제 티커 파일을
  덮어썼습니다. 이제 실패는 `SP500UniverseError`로 올라오고, 티커 파일 쓰기
  앞에 최소 개수 가드(`MIN_EXPECTED_TICKERS`)가 있습니다.
- `data_collection/collect_sip_1min.py` — 0행 결과를 성공으로 보고해
  `daily_pipeline.py`가 해당 종목을 완료로 체크포인트했습니다. 이제 0행은
  `CollectionResult(False, empty=True)`(`outcome == "EMPTY"`)이라는 별도의
  비성공 결과이며 완료로 기록되지 않습니다.
