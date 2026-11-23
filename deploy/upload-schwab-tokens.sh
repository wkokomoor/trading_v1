#!/usr/bin/env bash
# Upload this machine's Schwab login (tokens.db) for the cloud jobs, then remove older copies.
# Weekly routine:
#   .venv/bin/trading schwab auth          # browser login (Schwab requires it every 7 days)
#   deploy/upload-schwab-tokens.sh
set -euo pipefail
cd "$(dirname "$0")/.."
source deploy/env.sh

TOKENS="${TOKENS:-$HOME/.schwabdev/tokens.db}"
[[ -f "$TOKENS" ]] || { echo "no $TOKENS: run '.venv/bin/trading schwab auth' first"; exit 1; }

.venv/bin/python - "$TOKENS" <<'PY'
import datetime as dt, sys
from trading.schwab import login_expires_at
exp = login_expires_at(sys.argv[1])
if exp is None or exp <= dt.datetime.now(dt.UTC):
    sys.exit(f"the local Schwab login has expired ({exp}); run '.venv/bin/trading schwab auth' first")
print(f"uploading login valid until {exp:%Y-%m-%d %H:%M} UTC")
PY

new="$(gc secrets versions add schwab-tokens --data-file "$TOKENS" --format 'value(name)')"
new="${new##*/}"
for v in $(gc secrets versions list schwab-tokens --filter 'state=ENABLED' --format 'value(name)'); do
  v="${v##*/}"
  [[ "$v" == "$new" ]] || gc secrets versions destroy "$v" --secret schwab-tokens >/dev/null
done
echo "schwab-tokens version $new is live; older versions destroyed"
