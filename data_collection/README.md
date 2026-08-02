# 데이터 수집

## 통합 파이프라인 수집기

공식 수집 경로는 `market_pipeline.py backfill`과 `incremental`입니다.
`collect_sip_long_term.py`의 Alpaca 30분봉 adapter를 통해 다음 데이터를
불변 객체로 보존합니다.

- 공급자 RAW 30분봉
- 공급자 ADJUSTED 30분봉
- 각각을 원천으로 한 XNYS 정규장 1시간·4시간·일봉

최초 백필은 180일 chunk와 연도·shard YEAR 객체를 사용합니다. 이후에는
완료 거래 세션 DAY 객체를 추가하고 WEEK→MONTH→YEAR로 Compaction합니다.
30분봉을 버리지 않으며 종목별 10년 파일을 공식 출력으로 갱신하지 않습니다.

기존 종목별 장기 파일 함수는 마이그레이션 호환을 위해 남아 있으며
`daily_pipeline.py --legacy-long-term`에서만 명시적으로 사용합니다.

## 종목 유니버스

- `get_ticker.py`: Wikipedia의 현재 S&P 500과 최근 10년 편출 종목
- `etf_universe.py`: `ticker_info/etf_universe.csv`의 검토된 ETF·ETP

가격 데이터와 S&P 500 편출 종목 조회 범위를 모두 최근 10년으로 사용합니다.

## 기존 독립 수집기

- `collect_sip_1min.py`: 기존 최근 3년 SIP 1분봉 수집기

`daily_pipeline.py`가 계속 호출하므로 유지하지만 새 통합 파이프라인의 정본 경로는 아닙니다.
기존 5분봉 수집기 `script.py`는 참조하는 코드가 하나도 없어 DP1에서 삭제했습니다.
