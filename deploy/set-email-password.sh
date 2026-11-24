#!/usr/bin/env bash
# Store the Gmail app password used for the daily summary email in Secret Manager.
# Create one first at https://myaccount.google.com/apppasswords (needs 2-Step Verification).
# The password is read without echoing and piped straight to Secret Manager; it's never
# written to disk. Then run deploy/deploy.sh to give it to the notify job.
set -euo pipefail
cd "$(dirname "$0")/.."
source deploy/env.sh

read -r -s -p "Gmail app password for $SMTP_USER (16 letters, spaces optional): " pw; echo
pw="${pw// /}"
[[ ${#pw} -eq 16 ]] || { echo "expected 16 letters, got ${#pw}; nothing stored"; exit 1; }

if gc secrets describe smtp-password >/dev/null 2>&1; then
  printf '%s' "$pw" | gc secrets versions add smtp-password --data-file - >/dev/null
else
  printf '%s' "$pw" | gc secrets create smtp-password --data-file - --replication-policy automatic >/dev/null
fi
unset pw
gc secrets add-iam-policy-binding smtp-password --member "serviceAccount:$SA" \
  --role roles/secretmanager.secretAccessor >/dev/null
echo "stored. Next: deploy/deploy.sh, then test with: JOB=trading-notify deploy/run-job.sh job notify --date <a past trading day>"
