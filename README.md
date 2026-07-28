# Idea2Strategy DB 정합 시장 데이터 파이프라인

Alpaca SIP의 미국 주식·ETF 30분봉을 RAW와 ADJUSTED로 보존하고, XNYS
정규장 1시간·4시간·일봉을 불변 Parquet 객체와 DBML 형태의 Catalog로
관리합니다.

공식 진입점은 `market_pipeline.py`입니다. `daily_pipeline.py`도 기본적으로
동일한 DAY 증분 엔진을 사용합니다. 기존 종목별 10년 파일 갱신은
`--legacy-long-term`을 명시한 경우에만 실행됩니다.

## 데이터 계약

연도마다 가격 유형을 섞지 않는 다음 8개 논리 데이터셋을 사용합니다.

```text
ALPACA_SIP_RAW_30M
├─ RAW 30m
├─ DERIVED 1h
├─ DERIVED 4h
└─ DERIVED 1d

ALPACA_SIP_ADJUSTED_30M
├─ ADJUSTED 30m
├─ DERIVED 1h
├─ DERIVED 4h
└─ DERIVED 1d
```

공급자 30분봉은 정규장 필터 전에 보존합니다. DERIVED만 XNYS 정규장으로
필터링하며 `source_minutes`에 실제 관측된 30분봉 분량을 기록합니다.
누락·미상장·공급자 미제공 구간은 생성하거나 보간하지 않습니다.

공식 Parquet 열:

```text
instrument_id string UUID
provider_symbol string
bar_start_at timestamp[us, UTC]
session_date_et date32
open/high/low/close float64
volume int64
trade_count nullable int64
vwap nullable float64
source_minutes int16  # DERIVED에만 존재
```

## 설치

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

`.env.example`을 참고하여 Alpaca 키를 환경변수로 제공합니다. S3와
PostgreSQL 실행은 각각 선택 의존성 `boto3`, `psycopg[binary]`가 필요합니다.
인증정보는 객체, Catalog, 상태, 보고서에 저장하지 않습니다.

`instrument_map.csv`에는 최소 다음 열이 필요합니다.

```csv
provider_symbol,instrument_id
AAPL,검증된-market_data.instruments-UUID
```

임의 UUID는 생성하지 않습니다. 권장 추가 열은 `provider_reference`,
`asset_type`, `primary_exchange_mic`입니다.

## 읽기 전용 계획

```powershell
python market_pipeline.py plan `
  --local-root .\market_data_store `
  --staging-root .\pipeline_state\staging `
  --instrument-map .\instrument_map.csv `
  --start-year 2016 --end-year 2025 `
  --price-type all --resolution all --dry-run
```

`plan`은 API, S3, DB, 로컬 객체와 Catalog를 변경하지 않습니다.

## 과거 백필

```powershell
python market_pipeline.py backfill `
  --local-root .\market_data_store `
  --staging-root .\pipeline_state\staging `
  --instrument-map .\instrument_map.csv `
  --start-year 2016 --end-year 2025 `
  --price-type all --resolution all `
  --shard-count 16 --target-size-mib 256 --max-size-mib 512 `
  --resume
```

Alpaca 요청은 종목·가격 유형별 180일 chunk입니다. chunk 결과를 staging
Parquet으로 보존한 후 월 단위 RecordBatch 범위로 읽어 연도·shard YEAR
객체를 직접 만듭니다. 10년 전체를 하나의 DataFrame에 올리지 않습니다.

전체 실행 전 대표 샘플과 `--max-symbols`로 검증해야 합니다. 이 저장소의
구현 과정에서는 실제 10년 전체 수집을 실행하지 않았습니다.

## 일별 증분

```powershell
python market_pipeline.py incremental `
  --local-root .\market_data_store `
  --staging-root .\pipeline_state\staging `
  --instrument-map .\instrument_map.csv `
  --start 2026-07-28 --end 2026-07-29 `
  --price-type all --resume
```

완료된 XNYS 세션만 DAY 객체와 새 Manifest Revision으로 publish합니다.
종목마다 파일을 만들지 않고 연도·shard·layer·resolution 단위로 묶습니다.
기존 객체는 덮어쓰지 않습니다.

ADJUSTED 증분 전에는 최근 10개 완료 세션의 겹치는 공급자 값을 다시
비교합니다. 가격 Revision이 감지되면 Catalog에 보존 중인 Adjusted 연도를
새 Revision으로 자동 백필하고 그 원천을 사용하는 파생 데이터도 다시 만든
후 당일 DAY publish를 계속합니다.

현재 완료 거래일 한 건을 처리하는 운영용 래퍼:

```powershell
python daily_pipeline.py `
  --instrument-map .\instrument_map.csv `
  --local-root .\market_data_store `
  --staging-root .\pipeline_state\staging `
  --price-type all --resume
```

## 파생봉과 Compaction

공급자 30분봉에서 파생 데이터를 다시 만들기:

```powershell
python market_pipeline.py derive `
  --local-root .\market_data_store `
  --staging-root .\pipeline_state\staging `
  --instrument-map .\instrument_map.csv `
  --start-year 2024 --end-year 2024 `
  --price-type adjusted --resolution all
```

Compaction은 재실행 가능한 별도 명령입니다.

```powershell
python market_pipeline.py compact ... `
  --price-type adjusted --layer DERIVED --resolution 1h `
  --granularity WEEK --period 2026-07-20

python market_pipeline.py compact ... `
  --price-type adjusted --layer DERIVED --resolution 1h `
  --granularity MONTH --period 2026-07-01

python market_pipeline.py compact ... `
  --price-type adjusted --layer DERIVED --resolution 1h `
  --granularity YEAR --period 2025-01-01
```

수명주기는 `DAY → WEEK → MONTH → YEAR`입니다. 월 경계를 넘는 주는 DAY로
유지하여 MONTH 경계를 보존합니다. MONTH는 완성된 WEEK와 경계의 DAY를,
YEAR는 MONTH와 남은 WEEK·DAY를 사용합니다. 모든 입력 객체는
`dataset_object_lineage`의 `COMPACTED_FROM`으로 기록됩니다.

새 AVAILABLE Manifest에서 같은 shard·시간을 나타내는 서로 다른
granularity는 함께 활성화되지 않습니다. 이전 Manifest와 객체는
`SUPERSEDED` 상태로 남아 고정된 과거 백테스트를 재현할 수 있습니다.

## 기존 파일 마이그레이션

```powershell
python market_pipeline.py migrate `
  --input-root . `
  --local-root .\market_data_store `
  --staging-root .\pipeline_state\staging `
  --instrument-map .\instrument_map.csv `
  --start-year 2016 --end-year 2025 `
  --price-type all --resolution all --resume
```

기존 종목별 파일은 staging 입력으로만 읽으며 삭제하거나 덮어쓰지 않습니다.
`market_data_backfill.py`는 이전 구현과 출력 검토를 위한 legacy 도구이고,
새 작업에는 `market_pipeline.py migrate`를 사용합니다.

## 객체 키

로컬과 S3는 같은 상대 키를 사용합니다.

```text
market-data/
  provider=ALPACA/
  feed=ALPACA_SIP_ADJUSTED_30M/
  dataset={stable-logical-dataset-id}/
  revision=1/
  layer=ADJUSTED/
  resolution=30m/
  granularity=YEAR/
  partition_start=2024-01-01/
  partition_end=2025-01-01/
  shard=s03-of-16/
  part-00001.parquet
```

shard는 다음 고정식으로 계산합니다.

```text
first_unsigned_64_bits(SHA256(canonical instrument UUID UTF-8)) % shard_count
```

객체는 임시 파일 완성·Footer 검사·SHA-256 검사 후 원자적으로 publish됩니다.
같은 키에 다른 바이트가 있으면 덮어쓰지 않고 실패합니다.

## 검증과 benchmark

```powershell
python market_pipeline.py validate --local-root .\market_data_store
python market_pipeline.py benchmark --local-root .\market_data_store
```

검증 범위:

- 고정 스키마와 UTC/ET 날짜
- 중복 키, OHLC 관계, 유한·양수 가격
- 음수 volume/trade_count
- DERIVED XNYS 범위와 조기 폐장 `source_minutes`
- 파티션 경계와 활성 객체 시간 비중첩
- Parquet Footer, 행 수, 바이트 SHA-256
- Manifest 객체 합계와 canonical dataset hash

benchmark는 파일 크기, part/granularity 수, 전체 Manifest 조회 시간,
종목·월 조회 시간과 Python peak memory를 측정합니다. 실제 대표 데이터
측정 전에는 production-ready로 표현하지 않습니다.

## Catalog, S3, PostgreSQL

로컬 Catalog는 `market_data_store/catalog-export` 아래 DBML 열 이름의
JSONL을 저장합니다.

```text
providers.jsonl
feeds.jsonl
pipeline-runs.jsonl
dataset-manifests.jsonl
storage-objects.jsonl
dataset-objects.jsonl
dataset-lineage.jsonl
dataset-object-lineage.jsonl
quality-incidents.jsonl
pipeline-run-outputs.local.jsonl  # DBML 보완 전 로컬 provenance
summary.json
```

DBML 계약 검증 및 별도 export:

```powershell
python market_pipeline.py export-db-plan `
  --local-root .\market_data_store `
  --output-root .\db-plan `
  --dbml C:\path\to\schema.draft.dbml
```

S3는 기본 dry-run이고 `--execute`가 있어야 업로드합니다.

```powershell
python market_pipeline.py upload `
  --local-root .\market_data_store `
  --output-catalog-root .\s3-catalog-export `
  --bucket example-bucket --prefix historical
```

PostgreSQL도 기본 dry-run입니다.

```powershell
python market_pipeline.py apply-db `
  --local-root .\market_data_store `
  --dbml C:\path\to\schema.draft.dbml
```

`--execute`를 사용해도 provider의 `rights_version=UNVERIFIED` 또는
`status=REVIEW_REQUIRED`이면 차단됩니다. 실제 저장·장기 보존·백테스트·
재배포 권리 근거를 검토하여 승인된 버전을 반영해야 합니다.

S3와 PostgreSQL은 하나의 분산 트랜잭션이 아닙니다. S3 검증 후 DB를
반영하며 DB 실패 시 업로드한 불변 객체를 즉시 삭제하지 않고 재시도 가능한
orphan으로 보존합니다.

## 재개와 staging 정리

`--resume`은 완료된 staging fragment와 검증된 불변 객체를 재사용합니다.
정리는 기본 실행에 포함되지 않으며 성공한 pipeline run만 대상으로 합니다.

```powershell
python market_pipeline.py cleanup-staging `
  --local-root .\market_data_store `
  --staging-root .\pipeline_state\staging `
  --run-id {pipeline-run-uuid}

python market_pipeline.py cleanup-staging ... --execute
```

## 종목 범위와 법적 주의

현재 및 최근 편출입 S&P 500 티커의 합집합은 point-in-time 지수 구성 종목이
아닙니다. 개별 종목 데이터 수집과 시점별 지수 Universe 재현을 구분합니다.
상장 전 또는 Alpaca 제공 시작 전 데이터를 만들지 않습니다.

## DBML 공백

현재 DBML은 최초 수집 실행과 그 실행이 생성한 source
`dataset_objects`를 직접 연결하는 출력 테이블이 부족합니다. DBML을
임의 수정하지 않았으며 다음 보완 후보를 문서화합니다.

```text
pipeline_run_outputs
├─ pipeline_run_id
├─ dataset_manifest_id
└─ dataset_object_id
```

또한 `dataset_manifests.dataset_hash`의 전역 UNIQUE는 서로 다른 논리
데이터셋이 동일 바이트 집합을 가질 때 충돌할 수 있습니다. DB dry-run은
중복을 표시하고 실행을 차단합니다.

상세 설계와 현재 구조 분석은
[`docs/db-aligned-market-pipeline.md`](docs/db-aligned-market-pipeline.md)와
[`docs/current-pipeline-analysis.md`](docs/current-pipeline-analysis.md)를
참조합니다.

## 테스트

```powershell
python -m unittest discover -s tests -v
```
