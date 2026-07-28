# DB 정합 불변 시장 데이터 파이프라인 설계

## 구성 요소

```text
AlpacaBarSource
  ↓ 180일 chunk / 완료 XNYS DAY
staging Parquet
  ↓ 월 단위 scan, 정렬, 품질 검사
ObjectStore
  ├─ LocalObjectStore
  └─ S3ObjectStore
  ↓ receipt + SHA-256
MarketDataCatalog
  ├─ LocalCatalog(JSONL)
  └─ PostgresCatalog
```

수집·파생·Compaction은 같은 `ObjectStore`와 `MarketDataCatalog` 계약을
사용합니다. S3 SDK와 SQL은 경계 구현 밖의 처리 로직에 노출하지 않습니다.

## 불변성과 Revision

객체 키에는 논리 dataset ID, Revision, layer, resolution, granularity,
파티션, shard와 part가 포함됩니다. 기존 키에 다른 바이트를 쓰지 않습니다.

증분 또는 Compaction은:

1. 새 storage object를 만듭니다.
2. 새 Manifest Revision에 활성 객체 snapshot을 만듭니다.
3. 대체 기간과 겹치는 이전 객체 관계를 제외합니다.
4. 기존 Manifest를 SUPERSEDED로 전환합니다.
5. 기존 storage object는 유지합니다.

동일 idempotency key의 성공 실행은 재사용합니다. 실패 실행은 같은 staging
fragment와 검증 완료 객체를 이용해 재개합니다. Adjusted 정정은 명시적인
새 Revision으로 영향 연도와 파생 데이터셋을 다시 생성합니다. 일별 실행은
최근 10개 완료 세션의 겹치는 Adjusted 값을 재조회합니다. 변경이 감지되면
보존 중인 Adjusted 연도 전체를 잠재 영향 범위로 보고 자동 백필한 뒤 당일
증분을 계속합니다.

## 백필 메모리 경계

API 결과는 종목·180일 chunk·가격 유형·연도·shard staging 파일로 나눕니다.
YEAR publish는 staging을 월 단위 PyArrow Dataset batch로 읽습니다.
정렬과 파생 집계의 최대 작업 범위는 한 달·shard이며 전체 10년 또는 전체
종목을 하나의 DataFrame으로 만들지 않습니다.

같은 YEAR partition 안에 여러 part가 있을 수 있지만 실제 `period_start/end`는
겹치지 않습니다. 파일 내부는 `instrument_id, bar_start_at`으로 정렬합니다.

## Compaction 규칙

- WEEK: ET 월요일 포함, 다음 월요일 미포함
- MONTH: 매월 1일 포함, 다음 달 1일 미포함
- YEAR: 1월 1일 포함, 다음 해 1월 1일 미포함

월을 가로지르는 주는 WEEK로 만들지 않고 DAY tail로 남깁니다. 이에 따라
MONTH는 완전히 포함된 WEEK와 경계 DAY를 안전하게 결합합니다. YEAR도 동일한
원리로 MONTH와 남은 WEEK·DAY를 결합합니다.

대상 기간의 XNYS 거래 세션이 입력 partition으로 모두 덮이지 않으면
`COMPACTION_INPUT_INCOMPLETE`로 중단합니다. 입력 데이터 행이 없다는 이유로
봉을 만들지는 않습니다.

## 품질과 상태 전환

Manifest는 BUILDING으로 시작합니다. 객체 publish, Footer, 행 수, SHA-256,
스키마와 품질 검사가 모두 성공해야 AVAILABLE입니다. API chunk 또는 일부
종목이 실패하면 성공 객체는 보존하지만 Manifest는 QUARANTINED입니다.

검증 실패는 `quality-incidents.jsonl`에 구조화합니다. 지원 코드에는 중복,
가격/OHLC, 음수 activity, UTC/ET 날짜, 파티션, XNYS, source_minutes,
Footer, 해시, 행 수, 활성 객체 겹침과 불완전 Compaction이 포함됩니다.

## 해시

객체 해시는 완성된 Parquet 바이트 SHA-256입니다.

dataset hash는 다음 canonical field를 정렬한 JSON의 SHA-256입니다.

```text
content_hash
object_kind
partition_granularity
partition_start/end
period_start/end
shard_key
part_number
row_count
schema_version
```

로컬 절대 경로와 실행 UUID는 dataset hash와 object key에 포함하지 않습니다.

## 로컬과 원격

로컬 `bucket_name`은 null입니다. 로컬 디렉터리를 S3 bucket처럼 표현하지
않습니다. S3 업로드 receipt로 만든 별도 Catalog export만 실제 bucket,
provider version ID와 ETag를 반영합니다.

PostgreSQL 실행 전:

- DBML 열 계약
- dataset hash UNIQUE 충돌
- provider 권리 버전
- instrument ID 존재

를 확인합니다. 실제 실행은 하나의 DB transaction입니다. S3 업로드와 DB는
분산 transaction이 아니므로 DB 실패 시 검증된 S3 객체는 재시도를 위해
보존합니다.

## 아직 필요한 DBML 보완

최초 수집 실행의 output을 직접 표현하는 `pipeline_run_outputs`가 없습니다.
현재 source 객체의 실행 provenance는 Local Catalog 실행 요약과 결정적 ID에
더해 `pipeline-run-outputs.local.jsonl`에 보존하지만, DB 정본에 직접 FK로
적재하려면 별도 DBML 제안이 필요합니다.

DBML은 이 저장소에서 임의 수정하지 않습니다.
