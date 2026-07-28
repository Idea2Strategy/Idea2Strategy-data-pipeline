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
