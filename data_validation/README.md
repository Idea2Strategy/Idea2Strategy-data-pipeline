# 데이터 검사

통합 파이프라인은 Raw와 Adjusted의 정규장 1시간봉·4시간봉·일봉을 각각 검사합니다.

검사 항목:

- 파일 존재 여부와 읽기 가능 여부
- OHLCV 필수 컬럼과 가격 관계
- 중복 타임스탬프
- XNYS 세션에서 예상하지 않은 타임스탬프
- 최초·마지막 거래일
- 예상 봉 수, 누락 봉 수와 커버리지

기본 실행은 종목별 요약 CSV만 생성합니다. `daily_pipeline.py --deep-quality`를 사용하면 연속 누락 구간 상세 CSV도 생성합니다. 무거래 구간은 정상적으로 봉이 없을 수 있으므로 자동 보간하거나 합성하지 않습니다.

보고서는 `report/latest/{format}/`과 `report/history/{session}/{format}/`에 함께 저장됩니다.

`quality_control.py`와 `audit_regular_session.py`의 독립 CLI는 기존 데이터셋을 점검할 때 사용할 수 있도록 유지합니다.

DP1에서 삭제한 파일:

- `check_data.py` — 참조하는 코드가 없었고, 읽을 수 없는 파일과 빈 파일에 대해 동일한
  `{"rows": 0, "start": "N/A", "regular_market_pct": 0.0}` 를 돌려주어 두 상황을 구분할 수 없었습니다.
- `data_report.py` — 참조하는 코드가 없었고, "생성된" 리포트 안에 특정 종목(ABNB)을 지목하는
  하드코딩 한국어 산문 약 30줄이 들어 있었습니다.
