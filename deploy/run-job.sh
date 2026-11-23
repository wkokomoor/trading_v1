#!/usr/bin/env bash
# Run any `trading` command once in the cloud and print its output. Examples:
#   deploy/run-job.sh portfolio show shadow-vol
#   deploy/run-job.sh kill-switch on --reason "checking something"
#   deploy/run-job.sh schwab check
# With no arguments, runs the job's normal command (JOB defaults to trading-trade).
#
# Calls the Cloud Run API directly instead of `gcloud run jobs execute --args`, which in
# gcloud 548 sends a field the API rejects ("Unknown name priorityTier at overrides").
set -euo pipefail
cd "$(dirname "$0")/.."
source deploy/env.sh

job="${JOB:-trading-trade}"
api="https://run.googleapis.com/v2/projects/$PROJECT/locations/$REGION"
token="$(gcloud auth print-access-token)"
py() { .venv/bin/python -c "$@"; }

if (($#)); then
  body="$(py 'import json,sys; print(json.dumps({"overrides": {"containerOverrides": [{"args": sys.argv[1:]}]}}))' "$@")"
else
  body="{}"
fi
op="$(curl -sS -X POST -H "Authorization: Bearer $token" -H 'Content-Type: application/json' "$api/jobs/$job:run" -d "$body")"
execution="$(py 'import json,sys; d=json.load(sys.stdin); print(d["metadata"]["name"]) if "metadata" in d else sys.exit(json.dumps(d, indent=2))' <<<"$op")"
short="${execution##*/}"
echo "started $short ($job ${*:-<default command>})"

while :; do
  state="$(curl -sS -H "Authorization: Bearer $token" "https://run.googleapis.com/v2/$execution")"
  done_="$(py 'import json,sys; d=json.load(sys.stdin); print("yes" if d.get("completionTime") else "no", d.get("succeededCount",0), d.get("failedCount",0))' <<<"$state")"
  read -r finished ok failed <<<"$done_"
  [[ "$finished" == yes ]] && break
  sleep 5
done

sleep 15  # logs reach Cloud Logging a few seconds after the job ends
gc logging read "resource.type=\"cloud_run_job\" AND labels.\"run.googleapis.com/execution_name\"=\"$short\"" \
  --order asc --format 'value(textPayload,jsonPayload.message)' | tr -d '\t' | awk 'NF'

if ((failed > 0)); then
  echo "FAILED ($short)"; exit 1
fi
echo "succeeded ($short)"
