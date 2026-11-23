# Image for the Cloud Run jobs. Build context: the repository root.
#   gcloud builds submit --config deploy/cloudbuild.yaml ...   (see deploy/deploy.sh)
FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONPATH=/app

WORKDIR /app

# Exact, hash-checked versions exported from uv.lock (deploy.sh regenerates this file).
COPY deploy/requirements.txt /app/requirements.txt
RUN pip install --require-hashes -r requirements.txt

COPY alembic.ini /app/
COPY trading /app/trading
COPY strategies /app/strategies

ARG GIT_SHA=unknown
ENV GIT_SHA=${GIT_SHA}

RUN useradd --create-home --uid 10001 trader
USER trader

ENTRYPOINT ["python", "-m", "trading"]
CMD ["--help"]
