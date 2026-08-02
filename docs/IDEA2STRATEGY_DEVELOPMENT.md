# Idea2Strategy Data Pipeline 개발 가이드

이 저장소는 기존 `market_hist_script`의 수집 이력을 보존하면서 Idea2Strategy의 공식 시장 데이터 파이프라인으로 확장합니다.

## 책임

- Alpaca 등 공급자 원본 수집과 원본 보존
- raw·adjusted 데이터 검증과 조정
- 일 단위 적재 후 주·월·연 단위 compaction
- S3 Parquet 객체 작성·검증
- `storage.objects`, `market_data.dataset_manifests`, `dataset_objects`, `dataset_lineage` 등록
- 기업행사 후보의 근거 수집과 관리자 검토 연결

## 현재 코드 실체 (2026-08, DP1 이후)

정본 구현은 `market_pipeline_lib/` + `market_pipeline.py` 하나뿐입니다.
아래 표의 "예정 구조"와 아직 일치하지 않으며, 어느 항목도 완료로 보지
않습니다.

- 중복 구현이던 `market_data_backfill/`은 삭제했습니다.
- `idea2strategy-market-loader/`는 정본이 아닙니다. Alpaca 클라이언트와
  S3 어댑터를 `market_pipeline_lib`로 이관하기 위해서만 남아 있습니다.
  개인 Flyway 이력(`db/migration/V001__*.sql`), `db/test-init/`,
  `docker-compose.test.yaml`은 COM07 위반이라 삭제했습니다.
- 각 repo는 개인 마이그레이션을 두지 않습니다. 스키마 변경은
  `db/migration-contributions/`에 기여하고 중앙 `backend/db-migration`
  모듈이 조립합니다. 파일명은
  `V<YYYYMMDDHHMMSS>__<owner>_<slug>.sql`이며 `V001__` 같은 legacy 번호는
  사용하지 않습니다.
- `market_data.pipeline_partitions`는 정본 DBML에 없습니다. 추가하지 않으며
  market-loader의 해당 의존은 제거했습니다.

권장 예정 구조:

```text
apps/pipeline_worker.py
lambdas/
  corporate_action_research/
  pipeline_trigger/
  lightweight_validation/
src/data_pipeline/
  ingestion/
  validation/
  adjustment/
  aggregation/
  compaction/
  corporate_actions/
  manifests/
  storage/
  persistence/
tests/
```

## 파일 분할과 공개 순서

- 임시 일 단위 객체를 먼저 쓰고 검증이 끝난 후에만 Manifest를 발행합니다.
- 주·월·연 compaction은 새 Parquet 객체와 새 Manifest를 만들며 기존 Manifest를 몰래 바꾸지 않습니다.
- 장기간 조회는 수천 개의 일 파일 대신 검증된 압축 Manifest를 선택합니다.
- S3 객체가 존재한다는 사실만으로 공식 데이터가 되지 않으며 검증 완료 Manifest가 공개 단위입니다.
- 기업행사 AI 결과는 후보일 뿐 자동으로 공식 데이터를 변경하지 않습니다. 권한·정책 검증을 통과한 관리자 결정이 필요합니다.

### 기업행사 조사 후보 경계

`market_pipeline_lib.corporate_action_research`는 외부 수집기가 전달한 공개
HTTP(S) 근거를 검증해 `REVIEW_REQUIRED` 후보로만 저장합니다. 근거 URL,
콘텐츠 SHA-256, UTC 조회 시각이 없으면 후보를 만들지 않으며, 후보 식별자는
정규화된 조사 결과로부터 결정론적으로 생성합니다.

실행 시각은 서로 다른 UTC 슬롯 두 개를 배포 환경에서 주입합니다. 모듈은
가장 최근 실행 대상 슬롯의 멱등 식별자만 계산하며 외부 스케줄러나 공급자를
직접 호출하지 않습니다. 공급자·라이선스 선택, `admin-mcp` 승인, 공식
corporate action 등록, adjusted dataset 재생성, 전략 판단 입력은 이
경계에 포함되지 않습니다.

## 데이터 접근

- 주 변경 스키마: `market_data`
- 객체 등록: `storage.objects`
- DB 접근은 필요한 범위의 SQLAlchemy Core만 사용합니다.
- Parquet 연산은 Polars·PyArrow가 담당합니다.
- Alembic은 사용하지 않으며 DBML·Flyway는 루트 저장소가 통합 관리합니다.

## Git Flow

- `develop`: 기본 개발·통합 브랜치
- `feature/*`, `fix/*`, `docs/*`, `chore/*`: `develop`에서 분기·병합
- `release/*`: 정식 릴리스 준비
- `main`: v1.0.0부터 검증된 정식 릴리스만
- `hotfix/*`: 정식 릴리스 이후 `main`에서 시작하고 `develop`에도 반영

