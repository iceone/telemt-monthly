# telemt-monthly

Monthly per-user traffic billing for telemetry services. Polls a stats API, computes daily deltas, accumulates them into monthly totals, and optionally uploads results to Google Sheets.

## How it works

1. **Fetch** -- pulls user stats from the API (`/v1/stats/users`), extracts `username` + `total_octets` (cumulative counter)
2. **Diff** -- compares current snapshot against the previous one to compute a delta (bytes since last run)
3. **Log** -- appends deltas to a daily CSV log (`OUT_DIR/YYYY-MM.csv`)
4. **Aggregate** -- rebuilds monthly totals (`YYYY-MM-totals.csv`) with a human-readable GB column
5. **Upload** -- (optional) pushes totals to a Google Sheets spreadsheet
6. **Rotate** -- saves current snapshot as the new baseline

Counter resets (e.g. service restarts) are detected and logged. Month boundaries trigger automatic archival and a fresh baseline.

## Requirements

- Python 3.8+
- `openssl` CLI (used for JWT signing when Google Sheets is enabled)
- No third-party Python packages (stdlib only)

## Installation

```bash
cp telemt_monthly.py /usr/local/bin/telemt-monthly
chmod +x /usr/local/bin/telemt-monthly
```

## Configuration

All settings are controlled via environment variables:

| Variable | Default | Description |
|---|---|---|
| `API_URL` | `http://127.0.0.1:9091/v1/stats/users` | Stats API endpoint |
| `AUTH_HEADER` | *(empty)* | Authorization header value (e.g. `Bearer TOKEN`) |
| `STATE_DIR` | `/var/lib/telemt-monthly` | Directory for baseline snapshots |
| `OUT_DIR` | `/var/log/telemt-monthly` | Directory for CSV logs and totals |
| `GSHEET_ENABLED` | `0` | Set to `1` to enable Google Sheets upload |
| `GSHEET_SA_KEY` | *(empty)* | Path to Google service account JSON key |
| `GSHEET_SPREADSHEET_ID` | *(empty)* | Target Google Sheets spreadsheet ID |
| `GSHEET_SHEET_NAME` | `Totals` | Sheet name within the spreadsheet |

## Usage

```bash
# Basic run (local CSV only)
telemt-monthly

# With Google Sheets upload
GSHEET_ENABLED=1 \
GSHEET_SA_KEY=/path/to/sa-key.json \
GSHEET_SPREADSHEET_ID=your_spreadsheet_id \
telemt-monthly

# Dry run (no files modified, prints what would happen)
telemt-monthly --dry-run
```

### Cron example

```cron
*/30 * * * * GSHEET_ENABLED=1 GSHEET_SA_KEY=/etc/telemt/sa-key.json GSHEET_SPREADSHEET_ID=your_id /usr/local/bin/telemt-monthly >> /var/log/telemt-monthly/cron.log 2>&1
```

## Google Sheets setup

### Step 1: Create a Google Cloud project

1. Go to [Google Cloud Console](https://console.cloud.google.com)
2. Click the project selector dropdown at the top of the page
3. Click **New Project**
4. Enter a project name (e.g. `telemt-billing`) and click **Create**
5. Make sure the new project is selected in the dropdown

### Step 2: Enable the Google Sheets API

1. In the left sidebar, go to **APIs & Services > Library**
2. Search for `Google Sheets API`
3. Click on **Google Sheets API** in the results
4. Click the **Enable** button
5. Wait for the API to be enabled (you will be redirected to the API overview page)

### Step 3: Create a Service Account

1. In the left sidebar, go to **APIs & Services > Credentials**
2. Click **+ Create Credentials** at the top and select **Service Account**
3. Fill in the details:
   - **Service account name**: `telemt-billing` (or any name you prefer)
   - **Service account ID**: auto-generated from the name
   - **Description**: optional (e.g. `Uploads monthly traffic totals to Sheets`)
4. Click **Create and Continue**
5. Skip the "Grant this service account access to project" step (click **Continue**)
6. Skip the "Grant users access to this service account" step (click **Done**)

### Step 4: Download the JSON key

1. On the **Credentials** page, find your new service account under **Service Accounts**
2. Click on the service account email
3. Go to the **Keys** tab
4. Click **Add Key > Create new key**
5. Select **JSON** and click **Create**
6. A `.json` file will be downloaded automatically -- this is your service account key
7. Copy the key to your server:
   ```bash
   # Copy the key file to the server
   scp ~/Downloads/telemt-billing-XXXXX.json root@your-server:/etc/telemt/sa-key.json

   # Secure the file permissions
   chmod 600 /etc/telemt/sa-key.json
   ```

The JSON key file looks like this (the important fields are `client_email` and `private_key`):

```json
{
  "type": "service_account",
  "project_id": "telemt-XXXXXX",
  "private_key_id": "...",
  "private_key": "-----BEGIN RSA PRIVATE KEY-----\n...\n-----END RSA PRIVATE KEY-----\n",
  "client_email": "telemt-billing@telemt-XXXXXX.iam.gserviceaccount.com",
  "client_id": "...",
  ...
}
```

### Step 5: Create and share the spreadsheet

1. Go to [Google Sheets](https://sheets.google.com) and create a new spreadsheet
2. Name it (e.g. `Telemt Monthly Billing`)
3. Copy the **Spreadsheet ID** from the URL:
   ```
   https://docs.google.com/spreadsheets/d/SPREADSHEET_ID_HERE/edit
                                           ^^^^^^^^^^^^^^^^^^^^
   ```
4. Click **Share** (top right)
5. Open the JSON key file and copy the `client_email` value
6. Paste the service account email into the share dialog
7. Set the role to **Editor**
8. Uncheck "Notify people" and click **Share**

> The script will automatically create a sheet named `Totals` (or whatever `GSHEET_SHEET_NAME` is set to) if it does not exist.

### Step 6: Configure and test

```bash
# Test with dry-run first
GSHEET_ENABLED=1 \
GSHEET_SA_KEY=/etc/telemt/sa-key.json \
GSHEET_SPREADSHEET_ID=your_spreadsheet_id_here \
telemt-monthly --dry-run

# Run for real
GSHEET_ENABLED=1 \
GSHEET_SA_KEY=/etc/telemt/sa-key.json \
GSHEET_SPREADSHEET_ID=your_spreadsheet_id_here \
telemt-monthly
```

If everything works, you should see:
```
[info] Uploaded 6 users to sheet 'Totals'
```

### Troubleshooting

| Error | Cause | Fix |
|---|---|---|
| `OAuth2 token request failed (400)` | Invalid or corrupted JSON key | Re-download the key from GCP Console |
| `Sheets API 403: PERMISSION_DENIED` | Service account has no access | Share the spreadsheet with the `client_email` as Editor |
| `Sheets API 404: NOT_FOUND` | Wrong spreadsheet ID | Check the ID in the spreadsheet URL |
| `openssl dgst failed` | `openssl` not installed | Install OpenSSL: `apt install openssl` |
| `GSHEET_SPREADSHEET_ID is required` | Environment variable not set | Set `GSHEET_SPREADSHEET_ID` |

## Output format

### Daily log (`YYYY-MM.csv`)

```
date,username,delta_bytes,total_bytes,note
2026-04-01,alice,0,1000,baseline
2026-04-02,alice,500,1500,ok
2026-04-03,alice,100,100,counter_reset:lost_up_to=1500
```

### Monthly totals (`YYYY-MM-totals.csv`)

```
month,username,month_bytes,month_gb
2026-04,alice,64424509440,60.000
2026-04,bob,4764729344,4.438
```

## Safety features

- **flock-based locking** prevents concurrent runs from double-counting
- **Month-boundary detection** archives the previous month's baseline and starts fresh
- **Counter-reset handling** logs the upper bound of lost traffic for manual review
- **Dry-run mode** previews all changes without modifying state

## Development

### Install uv

**macOS:**

```bash
# Homebrew
brew install uv

# or standalone installer
curl -LsSf https://astral.sh/uv/install.sh | sh
```

**Linux:**

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

After installation, restart your shell or run `source $HOME/.local/bin/env`.

### Setup

```bash
# Clone and install dev dependencies
git clone <repo-url>
cd telemt-monthly
uv sync
```

### Lint

```bash
uv run ruff check .          # check
uv run ruff check --fix .    # auto-fix
```

### Tests

```bash
# Run on current Python
uv run python -m unittest test_telemt_monthly -v

# Run full matrix (Python 3.8–3.13 + lint) via tox
uv run tox
```

Unit tests covering: TSV/CSV I/O, delta computation, counter resets, month boundaries, totals aggregation, scientific notation parsing, Google Sheets helpers.

CI runs the same tox matrix on every push and pull request via GitHub Actions.
