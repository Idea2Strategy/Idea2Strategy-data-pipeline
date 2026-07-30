# Idea2Strategy Data Pipeline 개발 가이드

이 저장소는 기존 `market_hist_script`의 수집 이력을 보존하면서 Idea2Strategy의 공식 시장 데이터 파이프라인으로 확장합니다.

## 책임

- Alpaca 등 공급자 원본 수집과 원본 보존
- raw·adjusted 데이터 검증과 조정
- 일 단위 적재 후 주·월·연 단위 compaction
- S3 Parquet 객체 작성·검증
- `storage.objects`, `market_data.dataset_manifests`, `dataset_objects`, `dataset_lineage` 등록
- 기업행사 후보의 근거 수집과 관리자 검토 연결

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

