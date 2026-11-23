#!/usr/bin/env bash
# Build the image, run database migrations, and create/update the jobs and their schedules.
# Run from the repository root after deploy/setup.sh:
#   deploy/deploy.sh
set -euo pipefail
cd "$(dirname "$0")/.."
source deploy/env.sh

if [[ -n "$(git status --porcelain --untracked-files=no)" ]]; then
  echo "Uncommitted changes: commit first so the deployed image matches a git commit."
  exit 1
fi
GIT_SHA="$(git rev-parse --short HEAD)"
IMAGE="$IMAGE_BASE:$GIT_SHA"

log "Exporting locked dependencies"
.venv/bin/uv export --frozen --no-dev --no-emit-project --format requirements-txt -o deploy/requirements.txt -q
if [[ -n "$(git status --porcelain deploy/requirements.txt)" ]]; then
  echo "deploy/requirements.txt was out of date with uv.lock; commit the regenerated file and re-run."
  exit 1
fi

log "Building $IMAGE on Cloud Build"
gc builds submit --config deploy/cloudbuild.yaml --substitutions "_IMAGE=$IMAGE,_GIT_SHA=$GIT_SHA" .

JOB_SECRETS="DATABASE_URL=database-url:latest,SCHWAB_APP_KEY=schwab-app-key:latest"
JOB_SECRETS+=",SCHWAB_APP_SECRET=schwab-app-secret:latest,/secrets/schwab/tokens.db=schwab-tokens:latest"
JOB_ENV="SCHWAB_TOKENS_FILE=/secrets/schwab/tokens.db"

# Daily summary email: only once the app password exists (deploy/set-email-password.sh).
if gc secrets describe smtp-password >/dev/null 2>&1; then
  JOB_SECRETS+=",SMTP_PASSWORD=smtp-password:latest"
  JOB_ENV+=";SMTP_USER=$SMTP_USER;NOTIFY_TO=$NOTIFY_TO"  # ";" because NOTIFY_TO may contain commas
  NOTIFY=yes
else
  echo "(no smtp-password secret yet: skipping the notify job; run deploy/set-email-password.sh)"
  NOTIFY=no
fi

COMMON=(
  --image "$IMAGE" --region "$REGION" --service-account "$SA"
  --set-cloudsql-instances "$SQL_CONNECTION"
  --set-secrets "$JOB_SECRETS" --set-env-vars "^;^$JOB_ENV"
  --memory 1Gi --cpu 1 --task-timeout 600s
)

deploy_job() {  # name, retries, args...
  local name="$1" retries="$2"; shift 2
  gc run jobs deploy "$name" "${COMMON[@]}" --max-retries "$retries" --args "$(IFS=,; echo "$*")" >/dev/null
  echo "  $name -> trading $*"
}

log "Jobs"
deploy_job trading-migrate 0 db upgrade
deploy_job trading-trade 1 job trade        # idempotent per day, so one retry is safe
deploy_job trading-ingest 2 job ingest
deploy_job trading-reconcile 2 job reconcile
if [[ $NOTIFY == yes ]]; then deploy_job trading-notify 1 job notify; fi

log "Running database migrations"
gc run jobs execute trading-migrate --region "$REGION" --wait

log "Schedules ($SCHEDULE_TZ, Monday-Friday; holidays exit as 'skipped')"
schedule() {  # name, cron, job
  local uri="https://run.googleapis.com/v2/projects/$PROJECT/locations/$REGION/jobs/$3:run"
  local args=(--location "$REGION" --schedule "$2" --time-zone "$SCHEDULE_TZ" --uri "$uri"
    --http-method POST --oauth-service-account-email "$SA")
  gc scheduler jobs describe "$1" --location "$REGION" >/dev/null 2>&1 &&
    gc scheduler jobs update http "$1" "${args[@]}" >/dev/null ||
    gc scheduler jobs create http "$1" "${args[@]}" >/dev/null
  echo "  $1: '$2' -> $3"
}
schedule trade-0935 "35 9 * * 1-5" trading-trade
schedule ingest-1630 "30 16 * * 1-5" trading-ingest
schedule reconcile-1645 "45 16 * * 1-5" trading-reconcile
if [[ $NOTIFY == yes ]]; then schedule notify-1700 "0 17 * * 1-5" trading-notify; fi

log "Deployed $GIT_SHA"
echo "First time only, create the shadow portfolio:"
echo "  deploy/run-job.sh portfolio create shadow-vol -s vol_filter --allocation 10000"
