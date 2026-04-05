#!/usr/bin/env python3
"""Telemt monthly traffic billing: accumulates per-user traffic deltas,
writes monthly CSV logs/totals, and optionally uploads to Google Sheets."""

import csv
import fcntl
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import base64
import urllib.request
import urllib.error
import urllib.parse
from datetime import date
from pathlib import Path

# ---------------------------------------------------------------------------
# Configuration (environment variables with defaults)
# ---------------------------------------------------------------------------
API_URL = os.environ.get("API_URL", "http://127.0.0.1:9091/v1/stats/users")
AUTH_HEADER = os.environ.get("AUTH_HEADER", "")

STATE_DIR = Path(os.environ.get("STATE_DIR", "/var/lib/telemt-monthly"))
OUT_DIR = Path(os.environ.get("OUT_DIR", "/var/log/telemt-monthly"))

GSHEET_ENABLED = os.environ.get("GSHEET_ENABLED", "1") == "1"
GSHEET_SA_KEY = Path(os.environ.get("GSHEET_SA_KEY", ""))
GSHEET_SPREADSHEET_ID = os.environ.get(
    "GSHEET_SPREADSHEET_ID", "YOUR_SPREADSHEET_ID"
)
GSHEET_SHEET_NAME = os.environ.get("GSHEET_SHEET_NAME", "Totals")

DRY_RUN = "--dry-run" in sys.argv

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
LOCK_FILE = STATE_DIR / ".telemt-monthly.lock"
PREV_TSV = STATE_DIR / "last.tsv"
CURR_TSV = STATE_DIR / "current.tsv"
STATE_MONTH_FILE = STATE_DIR / "month"

TODAY = date.today().isoformat()          # YYYY-MM-DD
MONTH = date.today().strftime("%Y-%m")    # YYYY-MM

MONTHLY_LOG = OUT_DIR / f"{MONTH}.csv"
MONTHLY_TOTALS = OUT_DIR / f"{MONTH}-totals.csv"

LOG_HEADER = ["date", "username", "delta_bytes", "total_bytes", "note"]
TOTALS_HEADER = ["month", "username", "month_bytes", "month_gb"]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def die(msg: str) -> None:
    print(f"ERROR: {msg}", file=sys.stderr)
    sys.exit(1)


def info(msg: str) -> None:
    print(f"[info] {msg}")


def api_get(url: str, auth: str = "") -> dict:
    """GET JSON from the stats API."""
    req = urllib.request.Request(url)
    if auth:
        req.add_header("Authorization", auth)
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())


def read_tsv(path: Path) -> dict[str, int]:
    """Read username\\ttotal_octets TSV into {username: octets}."""
    result: dict[str, int] = {}
    if not path.exists():
        return result
    for line in path.read_text().splitlines():
        if not line.strip():
            continue
        user, octets = line.split("\t", 1)
        result[user] = int(octets)
    return result


def write_tsv(path: Path, data: dict[str, int]) -> None:
    """Write {username: octets} as sorted TSV."""
    lines = [f"{u}\t{o}" for u, o in sorted(data.items())]
    path.write_text("\n".join(lines) + "\n" if lines else "")


def ensure_csv_header(path: Path, header: list[str]) -> None:
    if not path.exists():
        with open(path, "w", newline="") as f:
            csv.writer(f).writerow(header)


def append_csv_rows(path: Path, rows: list[list]) -> None:
    with open(path, "a", newline="") as f:
        csv.writer(f).writerows(rows)


def read_csv_rows(path: Path) -> list[list[str]]:
    """Read CSV skipping the header row."""
    with open(path, newline="") as f:
        reader = csv.reader(f)
        next(reader, None)  # skip header
        return list(reader)


def compute_deltas(
    snapshot: dict[str, int],
    prev: dict[str, int],
    today: str,
) -> list[list[str]]:
    """Compute per-user traffic deltas between two snapshots.

    Returns rows: [date, username, delta_bytes, total_bytes, note]
    """
    rows: list[list[str]] = []
    seen: set[str] = set()

    for user, curr in sorted(snapshot.items()):
        seen.add(user)
        if user in prev:
            delta = curr - prev[user]
            if delta < 0:
                note = f"counter_reset:lost_up_to={prev[user]}"
                delta = curr
            else:
                note = "ok"
        else:
            delta = curr
            note = "new_user"
        rows.append([today, user, str(delta), str(curr), note])

    for user in sorted(prev):
        if user not in seen:
            rows.append([today, user, "0", "0", "missing_in_current_snapshot"])

    return rows


def rebuild_totals(monthly_log: Path, monthly_totals: Path) -> None:
    """Re-aggregate month_bytes per user from the daily log."""
    sums: dict[str, int] = {}
    for row in read_csv_rows(monthly_log):
        # date, username, delta_bytes, total_bytes, note
        user = row[1]
        delta = int(float(row[2]))
        sums[user] = sums.get(user, 0) + delta

    rows = []
    for user, total in sums.items():
        gb = total / 1_073_741_824
        rows.append([MONTH, user, str(total), f"{gb:.3f}"])

    rows.sort(key=lambda r: float(r[3]), reverse=True)

    with open(monthly_totals, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(TOTALS_HEADER)
        w.writerows(rows)


# ---------------------------------------------------------------------------
# Google Sheets (JWT + Sheets API v4, stdlib only)
# ---------------------------------------------------------------------------

def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def _sheets_request(method: str, url: str, token: str,
                    body: dict | None = None) -> dict:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Authorization", f"Bearer {token}")
    if data is not None:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        err_body = e.read().decode()
        try:
            err = json.loads(err_body)
            print(f"ERROR: Sheets API {e.code}: "
                  f"{json.dumps(err.get('error', err), indent=2)}", file=sys.stderr)
        except json.JSONDecodeError:
            print(f"ERROR: Sheets API {e.code}: {err_body}", file=sys.stderr)
        raise


def _rsa_sign_sha256(private_key_pem: str, message: bytes) -> bytes:
    """RS256 signing via system openssl command."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".pem") as kf:
        kf.write(private_key_pem)
        kf.flush()
        proc = subprocess.run(
            ["openssl", "dgst", "-sha256", "-sign", kf.name, "-binary"],
            input=message,
            capture_output=True,
        )
    if proc.returncode != 0:
        die(f"openssl dgst failed: {proc.stderr.decode()}")
    return proc.stdout


def gsheet_get_token(sa_key_path: Path) -> str:
    """Get an OAuth2 access token using a service account JSON key."""
    sa = json.loads(sa_key_path.read_text())
    now = int(time.time())

    header = _b64url(json.dumps({"alg": "RS256", "typ": "JWT"}).encode())
    payload = _b64url(json.dumps({
        "iss": sa["client_email"],
        "scope": "https://www.googleapis.com/auth/spreadsheets",
        "aud": "https://oauth2.googleapis.com/token",
        "iat": now,
        "exp": now + 3600,
    }).encode())

    sig_input = f"{header}.{payload}".encode()
    signature = _b64url(_rsa_sign_sha256(sa["private_key"], sig_input))
    jwt = f"{header}.{payload}.{signature}"

    data = urllib.parse.urlencode({
        "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
        "assertion": jwt,
    }).encode()

    req = urllib.request.Request("https://oauth2.googleapis.com/token",
                                data=data, method="POST")
    req.add_header("Content-Type", "application/x-www-form-urlencoded")

    try:
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read())["access_token"]
    except urllib.error.HTTPError as e:
        die(f"OAuth2 token request failed ({e.code}): {e.read().decode()}")


def gsheet_ensure_sheet(token: str, spreadsheet_id: str, sheet_name: str) -> None:
    base = "https://sheets.googleapis.com/v4/spreadsheets"
    meta = _sheets_request(
        "GET",
        f"{base}/{spreadsheet_id}?fields=sheets.properties.title",
        token,
    )
    for s in meta.get("sheets", []):
        if s.get("properties", {}).get("title") == sheet_name:
            return

    info(f"Sheet '{sheet_name}' not found — creating...")
    _sheets_request("POST", f"{base}/{spreadsheet_id}:batchUpdate", token, {
        "requests": [{"addSheet": {"properties": {"title": sheet_name}}}]
    })
    info(f"Sheet '{sheet_name}' created.")


def gsheet_upload_totals(totals_path: Path, token: str,
                         spreadsheet_id: str, sheet_name: str) -> None:
    base = "https://sheets.googleapis.com/v4/spreadsheets"
    gsheet_ensure_sheet(token, spreadsheet_id, sheet_name)

    # Read CSV into list of lists
    values = []
    with open(totals_path, newline="") as f:
        for row in csv.reader(f):
            values.append(row)

    range_a1 = f"{sheet_name}!A1"

    # Clear
    _sheets_request("POST",
                    f"{base}/{spreadsheet_id}/values/{sheet_name}:clear",
                    token, {})

    # Write
    _sheets_request("PUT",
                    f"{base}/{spreadsheet_id}/values/{range_a1}"
                    "?valueInputOption=RAW",
                    token, {
                        "range": range_a1,
                        "majorDimension": "ROWS",
                        "values": values,
                    })

    info(f"Uploaded {len(values) - 1} users to sheet '{sheet_name}'")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    if DRY_RUN:
        info("dry-run mode — no state files will be modified")

    STATE_DIR.mkdir(parents=True, exist_ok=True)
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # 1) Flock
    lock_fd = open(LOCK_FILE, "w")
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        die(f"another instance is already running (lock: {LOCK_FILE})")

    # 2) Fetch stats from API
    try:
        data = api_get(API_URL, AUTH_HEADER)
    except Exception as e:
        die(f"Failed to fetch stats from {API_URL}: {e}")

    if data.get("ok") is not True:
        die("API returned ok != true")

    snapshot: dict[str, int] = {}
    for entry in data.get("data", []):
        snapshot[entry["username"]] = int(entry.get("total_octets", 0))

    # 3) Month-boundary handling
    if STATE_MONTH_FILE.exists():
        stored_month = STATE_MONTH_FILE.read_text().strip()
        if stored_month != MONTH:
            info(f"Month changed: {stored_month} -> {MONTH}. Archiving old state.")
            if PREV_TSV.exists():
                shutil.copy2(PREV_TSV,
                             STATE_DIR / f"last-{stored_month}-archived.tsv")
            PREV_TSV.unlink(missing_ok=True)
            CURR_TSV.unlink(missing_ok=True)

    # 4) First run or new month — create baseline
    if not PREV_TSV.exists():
        ensure_csv_header(MONTHLY_LOG, LOG_HEADER)

        if DRY_RUN:
            info(f"Would create baseline with {len(snapshot)} users")
        else:
            rows = [[TODAY, user, "0", str(octets), "baseline"]
                    for user, octets in sorted(snapshot.items())]
            append_csv_rows(MONTHLY_LOG, rows)
            write_tsv(PREV_TSV, snapshot)
            write_tsv(CURR_TSV, snapshot)
            STATE_MONTH_FILE.write_text(MONTH)

        rebuild_totals(MONTHLY_LOG, MONTHLY_TOTALS)

        if DRY_RUN:
            info("Totals would be:")
            print(MONTHLY_TOTALS.read_text())
        elif GSHEET_ENABLED:
            token = gsheet_get_token(GSHEET_SA_KEY)
            gsheet_upload_totals(MONTHLY_TOTALS, token,
                                GSHEET_SPREADSHEET_ID, GSHEET_SHEET_NAME)
        return

    # 5) Regular run — compute deltas
    ensure_csv_header(MONTHLY_LOG, LOG_HEADER)
    prev = read_tsv(PREV_TSV)
    delta_rows = compute_deltas(snapshot, prev, TODAY)

    if DRY_RUN:
        info("Deltas that would be appended:")
        for r in delta_rows:
            print(",".join(r))
    else:
        append_csv_rows(MONTHLY_LOG, delta_rows)

    # 6) Rebuild totals
    rebuild_totals(MONTHLY_LOG, MONTHLY_TOTALS)

    if DRY_RUN:
        info("Monthly totals would be:")
        print(MONTHLY_TOTALS.read_text())
    else:
        # 7) Upload to Google Sheets
        if GSHEET_ENABLED:
            token = gsheet_get_token(GSHEET_SA_KEY)
            gsheet_upload_totals(MONTHLY_TOTALS, token,
                                GSHEET_SPREADSHEET_ID, GSHEET_SHEET_NAME)

        # 8) Update baseline
        write_tsv(PREV_TSV, snapshot)
        write_tsv(CURR_TSV, snapshot)
        STATE_MONTH_FILE.write_text(MONTH)


if __name__ == "__main__":
    main()
