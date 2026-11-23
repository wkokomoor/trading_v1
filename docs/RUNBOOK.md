# Runbook: trading jobs on Google Cloud

Project `schwab-trader-wk`, region `us-central1`. All settings are in `deploy/env.sh`.

## What runs, and when (America/New_York, Monday to Friday)

| Time | Scheduler job | Cloud Run job | Command |
|---|---|---|---|
| 09:35 | `trade-0935` | `trading-trade` | `trading job trade`: decide at the open, risk-check, simulated (shadow) fills |
| 16:30 | `ingest-1630` | `trading-ingest` | `trading job ingest`: refresh daily bars from Schwab |
| 16:45 | `reconcile-1645` | `trading-reconcile` | `trading job reconcile`: mark shadow portfolios at the close |

On market holidays the jobs run and exit with `status: skipped`. That's normal and doesn't alert.

The jobs use the Cloud SQL Postgres instance `trading-db`, connecting through the Cloud SQL connector, and these secrets: `database-url`, `schwab-app-key`, `schwab-app-secret` and `schwab-tokens` (the Schwab login). An email goes to the alert address whenever a job logs a WARNING or ERROR.

## Weekly: renew the Schwab login (required)

Schwab requires a browser login every 7 days, and a cloud job can't do it. Jobs start warning 2 days before expiry, which triggers an alert email. Renew from this Mac:

```sh
.venv/bin/trading schwab auth            # browser login; paste the redirect URL back
deploy/upload-schwab-tokens.sh           # uploads it; older copies are destroyed
.venv/bin/trading schwab check           # optional: shows the new expiry
```

If the login lapses, the trade job fails safely (no orders) and alerts. Renew, then re-run the day's trade with `deploy/run-job.sh`.

## Common tasks

`deploy/run-job.sh <trading command...>` runs any `trading` command once in the cloud and **prints its output**. It calls the Cloud Run API directly, because `gcloud run jobs execute --args` is broken in gcloud 548 ("Unknown name priorityTier at overrides").

| Task | Command |
|---|---|
| **Stop all trading now** | `deploy/run-job.sh kill-switch on --reason "<why>"` |
| Resume trading | `deploy/run-job.sh kill-switch off` |
| Kill switch status | `deploy/run-job.sh kill-switch status` |
| See the shadow portfolio | `deploy/run-job.sh portfolio show shadow-vol` |
| Check Schwab from the cloud | `deploy/run-job.sh schwab check` |
| Re-run today's trade by hand | `deploy/run-job.sh` (safe: at most once per day) |
| Re-run ingest / reconcile | `JOB=trading-ingest deploy/run-job.sh`, `JOB=trading-reconcile deploy/run-job.sh` |
| Pause the schedule | `gcloud scheduler jobs pause trade-0935 --project schwab-trader-wk --location us-central1` (`resume` to undo) |
| Read job logs | Cloud Console → Cloud Run → Jobs → job → Logs, or `gcloud logging read 'resource.type="cloud_run_job"' --project schwab-trader-wk --limit 50` |
| Deploy new code | commit, then `deploy/deploy.sh` (builds, migrates, updates jobs and schedules) |
| Roll back | `git checkout <older commit>` and `deploy/deploy.sh`; images are tagged by commit |

## When something goes wrong

| Symptom | Likely cause | What to do |
|---|---|---|
| Alert: "schwab login expires in N hours" | Weekly login due | The weekly routine above |
| Trade job failed with `LoginRequired` | Login expired | Renew, then re-run today's trade (a failed run with no orders can be retried) |
| Trade run `blocked`, "kill switch is on" | Kill switch | Check why it was turned on before turning it off |
| Trade run `blocked`, "stale" / "no opening price" / "suspect data" | Schwab quote problem at 09:35 | Check quotes with `schwab check`, then re-run the trade job later only if you accept a later fill |
| Reconcile: "no bar for today yet" | Ingest ran before Schwab published the daily bar | Re-run ingest, then reconcile |
| Cloud SQL errors | Instance stopped or maintenance | Cloud Console → SQL → `trading-db` |

## Restoring the database

Cloud SQL keeps 7 daily automated backups (taken at 07:00 UTC). Restore from Cloud Console → SQL → `trading-db` → Backups. A restore replaces the instance's data, so turn the kill switch on first.

## Costs to watch

The Cloud SQL instance (db-f1-micro, 10 GB SSD, backups) is most of the bill. The jobs run for seconds a day, and Scheduler, Secret Manager and Artifact Registry are cents. Check Billing → Reports monthly.
