#!/usr/bin/env bash
set -euo pipefail

API_URL="${API_URL:-http://127.0.0.1:9091/v1/stats/users}"
AUTH_HEADER="${AUTH_HEADER:-}"   # например: Bearer MY_TOKEN

STATE_DIR="${STATE_DIR:-/var/lib/telemt-monthly}"
OUT_DIR="${OUT_DIR:-/var/log/telemt-monthly}"
LOCK_FILE="${STATE_DIR}/.telemt-monthly.lock"

DRY_RUN=0
if [[ "${1:-}" == "--dry-run" ]]; then
  DRY_RUN=1
  echo "[dry-run] No state files will be modified"
fi

mkdir -p "$STATE_DIR" "$OUT_DIR"

# --- 1) flock-based locking to prevent concurrent runs ---
exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  echo "ERROR: another instance is already running (lock: $LOCK_FILE)" >&2
  exit 1
fi

today="$(date +%F)"
month="$(date +%Y-%m)"

tmp_json="$(mktemp)"
tmp_tsv="$(mktemp)"
tmp_agg="$(mktemp)"

prev_tsv="$STATE_DIR/last.tsv"
curr_tsv="$STATE_DIR/current.tsv"
state_month_file="$STATE_DIR/month"   # tracks which month the baseline belongs to

monthly_log="$OUT_DIR/${month}.csv"
monthly_totals="$OUT_DIR/${month}-totals.csv"

cleanup() {
  rm -f "$tmp_json" "$tmp_tsv" "$tmp_agg"
}
trap cleanup EXIT

# --- 2) Fetch stats from API ---
if [[ -n "$AUTH_HEADER" ]]; then
  curl -fsS -H "Authorization: $AUTH_HEADER" "$API_URL" -o "$tmp_json"
else
  curl -fsS "$API_URL" -o "$tmp_json"
fi

# --- 3) Extract username + total_octets ---
jq -er '
  if .ok != true then
    error("API returned ok != true")
  else
    .data[]
    | [.username, (.total_octets // 0)]
    | @tsv
  end
' "$tmp_json" | sort > "$tmp_tsv"

# --- 4) Month-boundary handling ---
# If the stored month differs from the current month, archive the old state
# and start a fresh baseline for the new month.
if [[ -f "$state_month_file" ]]; then
  stored_month="$(cat "$state_month_file")"
  if [[ "$stored_month" != "$month" ]]; then
    echo "[info] Month changed: $stored_month -> $month. Archiving old state."

    # Finalize previous month's totals (they stay in OUT_DIR as-is)
    # Archive baseline so it can be inspected later
    if [[ -f "$prev_tsv" ]]; then
      cp "$prev_tsv" "$STATE_DIR/last-${stored_month}-archived.tsv"
    fi

    # Remove baseline so step 5 treats this as a fresh start for the new month
    rm -f "$prev_tsv" "$curr_tsv"
  fi
fi

# --- 5) First run or new month — create baseline ---
if [[ ! -f "$prev_tsv" ]]; then
  if [[ ! -f "$monthly_log" ]]; then
    echo "date,username,delta_bytes,total_bytes,note" > "$monthly_log"
  fi

  if (( DRY_RUN )); then
    echo "[dry-run] Would create baseline with $(wc -l < "$tmp_tsv") users"
  else
    awk -F'\t' -v d="$today" '{
      printf "%s,%s,%s,%s,%s\n", d, $1, 0, $2, "baseline"
    }' "$tmp_tsv" >> "$monthly_log"

    cp "$tmp_tsv" "$prev_tsv"
    cp "$tmp_tsv" "$curr_tsv"
    echo "$month" > "$state_month_file"
  fi

  # Rebuild totals even on baseline (shows 0 for everyone)
  _rebuild_totals() {
    {
      echo "month,username,month_bytes,month_gb"
      awk -F',' -v OFS=',' -v m="$month" '
        NR == 1 { next }
        {
          user=$2
          delta=$3 + 0
          sum[user] += delta
        }
        END {
          for (u in sum) {
            gb = sum[u] / 1073741824
            printf "%s,%s,%d,%.3f\n", m, u, sum[u], gb
          }
        }
      ' "$monthly_log" | sort -t',' -k4 -rn
    } > "$tmp_agg"
    if (( ! DRY_RUN )); then
      mv "$tmp_agg" "$monthly_totals"
    else
      echo "[dry-run] Totals would be:"
      cat "$tmp_agg"
    fi
  }
  _rebuild_totals
  exit 0
fi

# --- 6) Create monthly log header if missing ---
if [[ ! -f "$monthly_log" ]]; then
  echo "date,username,delta_bytes,total_bytes,note" > "$monthly_log"
fi

# --- 7) Compute deltas with improved counter-reset handling ---
awk -F'\t' -v OFS=',' -v d="$today" '
  FNR==NR {
    prev[$1]=$2
    next
  }
  {
    user=$1
    curr=$2 + 0
    note="ok"

    if (user in prev) {
      delta = curr - prev[user]
      if (delta < 0) {
        # Counter was reset (service restart, etc.)
        # curr = traffic since restart only; gap before reset is lost
        delta = curr
        lost = prev[user] - 0   # upper bound of lost traffic
        note = "counter_reset:lost_up_to=" lost
      }
    } else {
      delta = curr
      note = "new_user"
    }

    print d, user, delta, curr, note
    seen[user] = 1
  }
  END {
    for (u in prev) {
      if (!(u in seen)) {
        print d, u, 0, 0, "missing_in_current_snapshot"
      }
    }
  }
' "$prev_tsv" "$tmp_tsv" > "$tmp_agg"

if (( DRY_RUN )); then
  echo "[dry-run] Deltas that would be appended:"
  cat "$tmp_agg"
else
  cat "$tmp_agg" >> "$monthly_log"
fi

# --- 8) Rebuild monthly totals with human-readable GB column ---
{
  echo "month,username,month_bytes,month_gb"
  awk -F',' -v OFS=',' -v m="$month" '
    NR == 1 { next }
    {
      user=$2
      delta=$3 + 0
      sum[user] += delta
    }
    END {
      for (u in sum) {
        gb = sum[u] / 1073741824
        printf "%s,%s,%d,%.3f\n", m, u, sum[u], gb
      }
    }
  ' "$monthly_log" | sort -t',' -k4 -rn
} > "$tmp_agg"

if (( DRY_RUN )); then
  echo "[dry-run] Monthly totals would be:"
  cat "$tmp_agg"
else
  mv "$tmp_agg" "$monthly_totals"
fi

# --- 9) Update baseline ---
if (( ! DRY_RUN )); then
  cp "$tmp_tsv" "$prev_tsv"
  cp "$tmp_tsv" "$curr_tsv"
  echo "$month" > "$state_month_file"
fi
