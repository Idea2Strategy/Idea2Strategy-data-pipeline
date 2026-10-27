FROM python:3.12.13-slim-bookworm@sha256:d50fb7611f86d04a3b0471b46d7557818d88983fc3136726336b2a4c657aa30b

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN groupadd --gid 10001 pipeline \
    && useradd --uid 10001 --gid 10001 --no-create-home --shell /usr/sbin/nologin pipeline

WORKDIR /app

# Third-party packages come only from the hash-locked runtime set, as wheels, so
# a rebuild installs the same bytes and a tampered or substituted file fails.
# See docs/IDEA2STRATEGY_DEVELOPMENT.md for how the lock is regenerated.
COPY requirements/runtime.txt ./requirements/runtime.txt
RUN python -m pip install --no-cache-dir --require-hashes --only-binary=:all: \
        -r requirements/runtime.txt

COPY pyproject.toml README.md ./
COPY apps ./apps
COPY lambdas ./lambdas
COPY market_pipeline_lib ./market_pipeline_lib
COPY data_collection ./data_collection
COPY data_filtering ./data_filtering
COPY data_validation ./data_validation
COPY daily_pipeline.py market_pipeline.py pipeline_reporting.py pipeline_state.py ./

# perl-base is an Essential Debian package, so apt refuses to remove it by
# default. The worker does not invoke Perl or package-management tools at
# runtime; remove it only after Python dependencies are installed to keep the
# vulnerable interpreter out of the shipped image.
# `--no-deps`: the project adds nothing that is not already locked, and
# `pip check` fails the build if pyproject.toml declares something the lock lacks.
RUN python -m pip install --no-cache-dir --no-deps . \
    && python -m pip check \
    && dpkg --purge --force-remove-essential perl-base \
    && mkdir -p /var/lib/idea2strategy/catalog /var/lib/idea2strategy/objects \
    && chown -R 10001:10001 /var/lib/idea2strategy

USER 10001:10001

ENTRYPOINT ["pipeline-worker"]
