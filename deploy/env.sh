# Shared settings for the deploy scripts. Override any of them in the environment.
PROJECT="${PROJECT:-schwab-trader-wk}"
REGION="${REGION:-us-central1}"
REPO="${REPO:-trading}"
IMAGE_BASE="${REGION}-docker.pkg.dev/${PROJECT}/${REPO}/trading"
SQL_INSTANCE="${SQL_INSTANCE:-trading-db}"
SQL_CONNECTION="${PROJECT}:${REGION}:${SQL_INSTANCE}"
DB_NAME="${DB_NAME:-trading}"
DB_USER="${DB_USER:-trading}"
SA_NAME="${SA_NAME:-trading-jobs}"
SA="${SA_NAME}@${PROJECT}.iam.gserviceaccount.com"
ALERT_EMAIL="${ALERT_EMAIL:-wkokomoor@gmail.com}"
SMTP_USER="${SMTP_USER:-wkokomoor@gmail.com}"   # sends the daily summary (Gmail + app password)
NOTIFY_TO="${NOTIFY_TO:-wkokomoor@gmail.com}"   # comma-separated; e.g. add 6082289829@msg.fi.google.com for a text
SCHEDULE_TZ="America/New_York"
SECRETS=(database-url schwab-app-key schwab-app-secret schwab-tokens)

gc() { gcloud --project "$PROJECT" --quiet "$@"; }
log() { printf '\n==> %s\n' "$*"; }
