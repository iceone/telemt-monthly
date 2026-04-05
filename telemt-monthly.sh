#!/usr/bin/env bash
set -euo pipefail

API_URL="${API_URL:-http://127.0.0.1:9091/v1/stats/users}"
AUTH_HEADER="${AUTH_HEADER:-}"   # например: Bearer MY_TOKEN

STATE_DIR="${STATE_DIR:-/var/lib/telemt-monthly}"
OUT_DIR="${OUT_DIR:-/var/log/telemt-monthly}"
LOCK_FILE="${STATE_DIR}/.telemt-monthly.lock"

# --- Google Sheets settings ---
GSHEET_ENABLED="${GSHEET_ENABLED:-1}"
GSHEET_SA_KEY="${GSHEET_SA_KEY:-}"
GSHEET_SPREADSHEET_ID="${GSHEET_SPREADSHEET_ID:-YOUR_SPREADSHEET_ID}"
GSHEET_SHEET_NAME="${GSHEET_SHEET_NAME:-Totals}"

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
  rm -f "$tmp_json" "$tmp_tsv" "$tmp_agg" "$_gsheet_tmp_token" "$_gsheet_tmp_jwt" 2>/dev/null || true
}
trap cleanup EXIT
_gsheet_tmp_token="$(mktemp)"
_gsheet_tmp_jwt="$(mktemp)"

# --- Google Sheets: JWT auth + upload functions ---

# Generate a Google OAuth2 access token from a service account JSON key.
# Uses openssl for RS256 signing — no Python/SDK needed.
_b64url() {
  openssl base64 -e -A | tr '+/' '-_' | tr -d '=\n'
}

gsheet_get_token() {
  local sa_key="$1"
  local sa_email scope aud now exp header payload
  local b64_header b64_payload sig_input signature jwt
  local private_key_file

  sa_email="$(jq -r '.client_email' "$sa_key")"
  private_key_file="$(mktemp)"
  jq -r '.private_key' "$sa_key" > "$private_key_file"

  scope="https://www.googleapis.com/auth/spreadsheets"
  aud="https://oauth2.googleapis.com/token"
  now="$(date +%s)"
  exp=$(( now + 3600 ))

  header='{"alg":"RS256","typ":"JWT"}'
  payload="$(printf '{"iss":"%s","scope":"%s","aud":"%s","iat":%d,"exp":%d}' \
    "$sa_email" "$scope" "$aud" "$now" "$exp")"

  b64_header="$(printf '%s' "$header" | _b64url)"
  b64_payload="$(printf '%s' "$payload" | _b64url)"

  sig_input="${b64_header}.${b64_payload}"
  signature="$(printf '%s' "$sig_input" | \
    openssl dgst -sha256 -sign "$private_key_file" -binary | _b64url)"

  rm -f "$private_key_file"

  jwt="${sig_input}.${signature}"

  # Request access token; show error body on failure
  local http_code
  http_code="$(curl -sS -X POST "$aud" \
    -H "Content-Type: application/x-www-form-urlencoded" \
    -d "grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer&assertion=${jwt}" \
    -o "$_gsheet_tmp_token" \
    -w "%{http_code}")"

  if [[ "$http_code" != "200" ]]; then
    echo "ERROR: Google OAuth2 token request failed (HTTP $http_code):" >&2
    cat "$_gsheet_tmp_token" >&2
    return 1
  fi

  jq -r '.access_token' "$_gsheet_tmp_token"
}

# Ensure the target sheet exists; create it if missing.
gsheet_ensure_sheet() {
  local access_token="$1"
  local spreadsheet_id="$2"
  local sheet_name="$3"
  local sheets_api="https://sheets.googleapis.com/v4/spreadsheets"

  # Check if sheet already exists
  local meta
  meta="$(curl -sS -X GET \
    "${sheets_api}/${spreadsheet_id}?fields=sheets.properties.title" \
    -H "Authorization: Bearer ${access_token}")"

  if echo "$meta" | jq -e --arg s "$sheet_name" '.sheets[]? | select(.properties.title == $s)' > /dev/null 2>&1; then
    return 0
  fi

  echo "[gsheet] Sheet '${sheet_name}' not found — creating..."
  local resp
  resp="$(curl -sS -X POST \
    "${sheets_api}/${spreadsheet_id}:batchUpdate" \
    -H "Authorization: Bearer ${access_token}" \
    -H "Content-Type: application/json" \
    -d "{\"requests\":[{\"addSheet\":{\"properties\":{\"title\":\"${sheet_name}\"}}}]}")"

  if echo "$resp" | jq -e '.error' > /dev/null 2>&1; then
    echo "ERROR: Failed to create sheet '${sheet_name}':" >&2
    echo "$resp" | jq '.error' >&2
    return 1
  fi
  echo "[gsheet] Sheet '${sheet_name}' created."
}

# Push totals CSV to Google Sheets (clear sheet, then write all rows).
gsheet_upload_totals() {
  local totals_file="$1"
  local access_token="$2"
  local spreadsheet_id="$3"
  local sheet_name="$4"

  local sheets_api="https://sheets.googleapis.com/v4/spreadsheets"
  local range="${sheet_name}!A1"

  # Ensure target sheet exists
  gsheet_ensure_sheet "$access_token" "$spreadsheet_id" "$sheet_name"

  # Build JSON payload from CSV: [ [row1col1, row1col2, ...], [row2col1, ...], ... ]
  local values_json
  values_json="$(awk -F',' '
    BEGIN { printf "[" }
    NR > 1 { printf "," }
    {
      printf "["
      for (i = 1; i <= NF; i++) {
        if (i > 1) printf ","
        gsub(/"/, "\\\"", $i)
        printf "\"%s\"", $i
      }
      printf "]"
    }
    END { printf "]" }
  ' "$totals_file")"

  # Clear existing data
  local resp
  resp="$(curl -sS -X POST \
    "${sheets_api}/${spreadsheet_id}/values/${sheet_name}:clear" \
    -H "Authorization: Bearer ${access_token}" \
    -H "Content-Type: application/json" \
    -d '{}')"

  # Write new data
  resp="$(curl -sS -X PUT \
    "${sheets_api}/${spreadsheet_id}/values/${range}?valueInputOption=RAW" \
    -H "Authorization: Bearer ${access_token}" \
    -H "Content-Type: application/json" \
    -d "{\"range\":\"${range}\",\"majorDimension\":\"ROWS\",\"values\":${values_json}}")"

  if echo "$resp" | jq -e '.error' > /dev/null 2>&1; then
    echo "ERROR: Failed to write to Google Sheets:" >&2
    echo "$resp" | jq '.error' >&2
    return 1
  fi

  echo "[gsheet] Uploaded $(( $(wc -l < "$totals_file") - 1 )) users to sheet '${sheet_name}'"
}

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

  # Upload baseline totals to Google Sheets
  if (( GSHEET_ENABLED )) && (( ! DRY_RUN )); then
    token="$(gsheet_get_token "$GSHEET_SA_KEY")"
    gsheet_upload_totals "$monthly_totals" "$token" "$GSHEET_SPREADSHEET_ID" "$GSHEET_SHEET_NAME"
  fi

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

# --- 9) Upload totals to Google Sheets ---
if (( GSHEET_ENABLED )) && (( ! DRY_RUN )); then
  token="$(gsheet_get_token "$GSHEET_SA_KEY")"
  gsheet_upload_totals "$monthly_totals" "$token" "$GSHEET_SPREADSHEET_ID" "$GSHEET_SHEET_NAME"
fi

# --- 10) Update baseline ---
if (( ! DRY_RUN )); then
  cp "$tmp_tsv" "$prev_tsv"
  cp "$tmp_tsv" "$curr_tsv"
  echo "$month" > "$state_month_file"
fi
