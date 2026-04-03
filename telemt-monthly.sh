#!/usr/bin/env bash
set -euo pipefail

API_URL="${API_URL:-http://127.0.0.1:9091/v1/stats/users}"
AUTH_HEADER="${AUTH_HEADER:-}"   # например: Bearer MY_TOKEN

STATE_DIR="${STATE_DIR:-/var/lib/telemt-monthly}"
OUT_DIR="${OUT_DIR:-/var/log/telemt-monthly}"

mkdir -p "$STATE_DIR" "$OUT_DIR"

today="$(date +%F)"
month="$(date +%Y-%m)"

tmp_json="$(mktemp)"
tmp_tsv="$(mktemp)"
tmp_agg="$(mktemp)"

prev_tsv="$STATE_DIR/last.tsv"
curr_tsv="$STATE_DIR/current.tsv"

monthly_log="$OUT_DIR/${month}.csv"
monthly_totals="$OUT_DIR/${month}-totals.csv"

cleanup() {
  rm -f "$tmp_json" "$tmp_tsv" "$tmp_agg"
}
trap cleanup EXIT

# 1) Забираем статистику из API
if [[ -n "$AUTH_HEADER" ]]; then
  curl -fsS -H "Authorization: $AUTH_HEADER" "$API_URL" -o "$tmp_json"
else
  curl -fsS "$API_URL" -o "$tmp_json"
fi

# 2) Вытаскиваем username + total_octets
jq -er '
  if .ok != true then
    error("API returned ok != true")
  else
    .data[]
    | [.username, (.total_octets // 0)]
    | @tsv
  end
' "$tmp_json" | sort > "$tmp_tsv"

# 3) Если это первый запуск — создаём baseline
if [[ ! -f "$prev_tsv" ]]; then
  cp "$tmp_tsv" "$prev_tsv"
  cp "$tmp_tsv" "$curr_tsv"

  if [[ ! -f "$monthly_log" ]]; then
    echo "date,username,delta_bytes,total_bytes,note" > "$monthly_log"
  fi

  awk -F'\t' -v d="$today" '{
    printf "%s,%s,%s,%s,%s\n", d, $1, 0, $2, "baseline"
  }' "$tmp_tsv" >> "$monthly_log"

  if [[ ! -f "$monthly_totals" ]]; then
    echo "month,username,month_bytes" > "$monthly_totals"
  fi

  exit 0
fi

# 4) Создаём файл месячного лога, если его ещё нет
if [[ ! -f "$monthly_log" ]]; then
  echo "date,username,delta_bytes,total_bytes,note" > "$monthly_log"
fi

# 5) Считаем дельту между прошлым и текущим снимком и дописываем в лог месяца
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
      delta=curr - prev[user]
      if (delta < 0) {
        delta=curr
        note="counter_reset_or_restart"
      }
    } else {
      delta=curr
      note="new_user"
    }

    print d, user, delta, curr, note
    seen[user]=1
  }
  END {
    for (u in prev) {
      if (!(u in seen)) {
        print d, u, 0, 0, "missing_in_current_snapshot"
      }
    }
  }
' "$prev_tsv" "$tmp_tsv" >> "$monthly_log"

# 6) Пересчитываем агрегат за месяц
{
  echo "month,username,month_bytes"
  awk -F',' -v OFS=',' -v m="$month" '
    NR == 1 { next }   # пропускаем заголовок
    {
      user=$2
      delta=$3 + 0
      sum[user] += delta
    }
    END {
      for (u in sum) {
        print m, u, sum[u]
      }
    }
  ' "$monthly_log" | sort
} > "$tmp_agg"

mv "$tmp_agg" "$monthly_totals"

# 7) Обновляем baseline
cp "$tmp_tsv" "$prev_tsv"
cp "$tmp_tsv" "$curr_tsv"
