FROM python:3.12.13-slim-bookworm@sha256:d50fb7611f86d04a3b0471b46d7557818d88983fc3136726336b2a4c657aa30b

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN groupadd --gid 10001 pipeline \
    && useradd --uid 10001 --gid 10001 --no-create-home --shell /usr/sbin/nologin pipeline

WORKDIR /app

COPY pyproject.toml README.md ./
COPY apps ./apps
COPY lambdas ./lambdas
COPY market_pipeline_lib ./market_pipeline_lib
COPY data_collection ./data_collection
COPY data_filtering ./data_filtering
COPY data_validation ./data_validation
COPY daily_pipeline.py market_pipeline.py pipeline_reporting.py pipeline_state.py ./

RUN python -m pip install --no-cache-dir . \
    && mkdir -p /var/lib/idea2strategy/catalog /var/lib/idea2strategy/objects \
    && chown -R 10001:10001 /var/lib/idea2strategy

USER 10001:10001

ENTRYPOINT ["pipeline-worker"]
