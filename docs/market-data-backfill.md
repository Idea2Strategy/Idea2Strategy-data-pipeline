# Legacy Manifest 백필

`market_data_backfill.py`는 이전 단계에서 만든 기존 파일 변환 도구입니다.
ADJUSTED 중심의 과거 출력 검토와 호환을 위해 보존하지만 새 정본 파이프라인은
아닙니다.

새로운 RAW/ADJUSTED 30분봉 보존, DAY 증분, WEEK/MONTH/YEAR Compaction,
ObjectStore/Catalog 경계와 DBML export에는 다음 명령을 사용합니다.

```powershell
python market_pipeline.py migrate ...
python market_pipeline.py backfill ...
python market_pipeline.py incremental ...
python market_pipeline.py compact ...
```

전체 계약과 운영 절차는
[`db-aligned-market-pipeline.md`](db-aligned-market-pipeline.md)에 있습니다.
