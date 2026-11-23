#!/usr/bin/env bash
# One-time infrastructure for the trading jobs. Safe to re-run: every step checks first.
# Creates BILLABLE resources (mainly the Cloud SQL instance, roughly $10-15/month at
# db-f1-micro; check current pricing). Run from the repository root:
#   deploy/setup.sh
set -euo pipefail
cd "$(dirname "$0")/.."
source deploy/env.sh

if [[ "$(gc billing projects describe "$PROJECT" --format='value(billingEnabled)')" != "True" ]]; then
  echo "Billing is not enabled for $PROJECT. Link a billing account first:"
  echo "  https://console.cloud.google.com/billing/linkedaccount?project=$PROJECT"
  exit 1
fi

log "Enabling APIs"
gc services enable run.googleapis.com cloudscheduler.googleapis.com sqladmin.googleapis.com \
  secretmanager.googleapis.com artifactregistry.googleapis.com cloudbuild.googleapis.com \
  logging.googleapis.com monitoring.googleapis.com iam.googleapis.com

log "Artifact Registry repository: $REPO"
gc artifacts repositories describe "$REPO" --location "$REGION" >/dev/null 2>&1 ||
  gc artifacts repositories create "$REPO" --location "$REGION" --repository-format docker \
    --description "trading job images"

log "Service account: $SA"
gc iam service-accounts describe "$SA" >/dev/null 2>&1 ||
  gc iam service-accounts create "$SA_NAME" --display-name "trading jobs"
for role in roles/cloudsql.client roles/logging.logWriter roles/run.invoker; do
  gc projects add-iam-policy-binding "$PROJECT" --member "serviceAccount:$SA" --role "$role" \
    --condition None >/dev/null
done

log "Cloud SQL instance: $SQL_INSTANCE (takes ~10 minutes the first time)"
# Public IP but no authorized networks: only reachable through the Cloud SQL connector
# (IAM-authenticated, encrypted), which is how Cloud Run connects.
gc sql instances describe "$SQL_INSTANCE" >/dev/null 2>&1 ||
  gc sql instances create "$SQL_INSTANCE" --database-version POSTGRES_16 --edition ENTERPRISE \
    --tier db-f1-micro --region "$REGION" --availability-type zonal --storage-size 10 \
    --storage-type SSD --backup-start-time 07:00 --retained-backups-count 7 --ssl-mode ENCRYPTED_ONLY
gc sql databases describe "$DB_NAME" --instance "$SQL_INSTANCE" >/dev/null 2>&1 ||
  gc sql databases create "$DB_NAME" --instance "$SQL_INSTANCE"

log "Secrets"
secret_exists() { gc secrets describe "$1" >/dev/null 2>&1; }
if ! secret_exists database-url; then
  # New random DB password; it only ever lives in Secret Manager.
  password="$(openssl rand -hex 24)"
  gc sql users create "$DB_USER" --instance "$SQL_INSTANCE" --password "$password" >/dev/null 2>&1 ||
    gc sql users set-password "$DB_USER" --instance "$SQL_INSTANCE" --password "$password" >/dev/null
  printf 'postgresql+psycopg://%s:%s@/%s?host=/cloudsql/%s' "$DB_USER" "$password" "$DB_NAME" "$SQL_CONNECTION" |
    gc secrets create database-url --data-file - --replication-policy automatic
  unset password
fi
# Schwab app key / secret come from this machine's .env (read by the app's own settings
# loader, so legacy lowercase keys work too). Values are piped, never printed.
for pair in "schwab-app-key:schwab_app_key" "schwab-app-secret:schwab_app_secret"; do
  name="${pair%%:*}"; field="${pair##*:}"
  if ! secret_exists "$name"; then
    .venv/bin/python -c "from trading.config import get_settings as g; import sys; v=getattr(g(),'$field'); \
sys.exit('missing $field in .env') if not v else sys.stdout.write(v)" |
      gc secrets create "$name" --data-file - --replication-policy automatic
  fi
done
if ! secret_exists schwab-tokens; then
  gc secrets create schwab-tokens --replication-policy automatic
  deploy/upload-schwab-tokens.sh
fi
for name in "${SECRETS[@]}"; do
  gc secrets add-iam-policy-binding "$name" --member "serviceAccount:$SA" \
    --role roles/secretmanager.secretAccessor >/dev/null
done

log "Alert: email $ALERT_EMAIL on any job warning or error"
token="$(gcloud auth print-access-token)"
api="https://monitoring.googleapis.com/v3/projects/$PROJECT"
channel="$(curl -sS -H "Authorization: Bearer $token" "$api/notificationChannels" |
  .venv/bin/python -c "import json,sys; d=json.load(sys.stdin); print(next((c['name'] for c in d.get('notificationChannels',[]) if c.get('labels',{}).get('email_address')=='$ALERT_EMAIL'),''))")"
if [[ -z "$channel" ]]; then
  channel="$(curl -sS -X POST -H "Authorization: Bearer $token" -H 'Content-Type: application/json' \
    "$api/notificationChannels" \
    -d "{\"type\":\"email\",\"displayName\":\"trading alerts\",\"labels\":{\"email_address\":\"$ALERT_EMAIL\"}}" |
    .venv/bin/python -c "import json,sys; print(json.load(sys.stdin)['name'])")"
fi
existing="$(curl -sS -H "Authorization: Bearer $token" "$api/alertPolicies" |
  .venv/bin/python -c "import json,sys; d=json.load(sys.stdin); print(any(p['displayName']=='trading: job warning or error' for p in d.get('alertPolicies',[])))")"
if [[ "$existing" != "True" ]]; then
  sed "s#__CHANNEL__#$channel#" deploy/alerts/job-errors.json |
    curl -sS -X POST -H "Authorization: Bearer $token" -H 'Content-Type: application/json' \
      "$api/alertPolicies" -d @- >/dev/null
fi

log "Setup complete. Next: deploy/deploy.sh"
