#!/usr/bin/env python3
"""Unit tests for telemt-monthly billing script."""

from __future__ import annotations

import csv
import json
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

# Import module under test
import telemt_monthly as tm


class TempDirMixin:
    """Mixin that creates a temp directory and cleans it up."""

    def setUp(self):
        super().setUp()
        self.tmpdir = Path(tempfile.mkdtemp())

    def tearDown(self):
        shutil.rmtree(self.tmpdir)
        super().tearDown()


# -----------------------------------------------------------------------
# read_tsv / write_tsv
# -----------------------------------------------------------------------

class TestReadWriteTsv(TempDirMixin, unittest.TestCase):

    def test_write_then_read(self):
        path = self.tmpdir / "test.tsv"
        data = {"alice": 1000, "bob": 2000}
        tm.write_tsv(path, data)
        result = tm.read_tsv(path)
        self.assertEqual(result, data)

    def test_read_nonexistent(self):
        result = tm.read_tsv(self.tmpdir / "missing.tsv")
        self.assertEqual(result, {})

    def test_write_empty(self):
        path = self.tmpdir / "empty.tsv"
        tm.write_tsv(path, {})
        result = tm.read_tsv(path)
        self.assertEqual(result, {})

    def test_sorted_output(self):
        path = self.tmpdir / "sorted.tsv"
        tm.write_tsv(path, {"charlie": 3, "alice": 1, "bob": 2})
        lines = path.read_text().strip().splitlines()
        users = [line.split("\t")[0] for line in lines]
        self.assertEqual(users, ["alice", "bob", "charlie"])

    def test_large_octets(self):
        path = self.tmpdir / "large.tsv"
        data = {"user1": 2_147_483_647_000}
        tm.write_tsv(path, data)
        self.assertEqual(tm.read_tsv(path), data)


# -----------------------------------------------------------------------
# CSV helpers
# -----------------------------------------------------------------------

class TestCsvHelpers(TempDirMixin, unittest.TestCase):

    def test_ensure_csv_header_creates_file(self):
        path = self.tmpdir / "log.csv"
        tm.ensure_csv_header(path, ["a", "b", "c"])
        self.assertTrue(path.exists())
        with open(path, newline="") as f:
            reader = csv.reader(f)
            header = next(reader)
        self.assertEqual(header, ["a", "b", "c"])

    def test_ensure_csv_header_does_not_overwrite(self):
        path = self.tmpdir / "log.csv"
        path.write_text("existing,data\n")
        tm.ensure_csv_header(path, ["a", "b", "c"])
        self.assertEqual(path.read_text(), "existing,data\n")

    def test_append_and_read_csv_rows(self):
        path = self.tmpdir / "log.csv"
        tm.ensure_csv_header(path, ["x", "y"])
        tm.append_csv_rows(path, [["1", "2"], ["3", "4"]])
        rows = tm.read_csv_rows(path)
        self.assertEqual(rows, [["1", "2"], ["3", "4"]])

    def test_read_csv_rows_empty_file(self):
        path = self.tmpdir / "log.csv"
        tm.ensure_csv_header(path, ["x"])
        rows = tm.read_csv_rows(path)
        self.assertEqual(rows, [])


# -----------------------------------------------------------------------
# compute_deltas
# -----------------------------------------------------------------------

class TestComputeDeltas(unittest.TestCase):

    def test_normal_delta(self):
        prev = {"alice": 100, "bob": 200}
        curr = {"alice": 150, "bob": 350}
        rows = tm.compute_deltas(curr, prev, "2026-04-05")
        self.assertEqual(len(rows), 2)
        # alice: 150 - 100 = 50
        alice = next(r for r in rows if r[1] == "alice")
        self.assertEqual(alice, ["2026-04-05", "alice", "50", "150", "ok"])
        # bob: 350 - 200 = 150
        bob = next(r for r in rows if r[1] == "bob")
        self.assertEqual(bob, ["2026-04-05", "bob", "150", "350", "ok"])

    def test_counter_reset(self):
        prev = {"alice": 5000}
        curr = {"alice": 100}  # counter reset: 100 < 5000
        rows = tm.compute_deltas(curr, prev, "2026-04-05")
        alice = rows[0]
        self.assertEqual(alice[2], "100")  # delta = curr value
        self.assertIn("counter_reset:lost_up_to=5000", alice[4])

    def test_new_user(self):
        prev = {"alice": 100}
        curr = {"alice": 150, "bob": 300}
        rows = tm.compute_deltas(curr, prev, "2026-04-05")
        bob = next(r for r in rows if r[1] == "bob")
        self.assertEqual(bob[2], "300")  # delta = full curr
        self.assertEqual(bob[4], "new_user")

    def test_missing_user(self):
        prev = {"alice": 100, "bob": 200}
        curr = {"alice": 150}  # bob disappeared
        rows = tm.compute_deltas(curr, prev, "2026-04-05")
        bob = next(r for r in rows if r[1] == "bob")
        self.assertEqual(bob[2], "0")
        self.assertEqual(bob[3], "0")
        self.assertEqual(bob[4], "missing_in_current_snapshot")

    def test_zero_delta(self):
        prev = {"alice": 100}
        curr = {"alice": 100}
        rows = tm.compute_deltas(curr, prev, "2026-04-05")
        self.assertEqual(rows[0][2], "0")
        self.assertEqual(rows[0][4], "ok")

    def test_empty_snapshots(self):
        rows = tm.compute_deltas({}, {}, "2026-04-05")
        self.assertEqual(rows, [])

    def test_all_new_users(self):
        rows = tm.compute_deltas({"a": 10, "b": 20}, {}, "2026-04-05")
        self.assertEqual(len(rows), 2)
        self.assertTrue(all(r[4] == "new_user" for r in rows))

    def test_all_missing_users(self):
        rows = tm.compute_deltas({}, {"a": 10, "b": 20}, "2026-04-05")
        self.assertEqual(len(rows), 2)
        self.assertTrue(all(r[4] == "missing_in_current_snapshot" for r in rows))

    def test_rows_sorted_by_username(self):
        curr = {"charlie": 3, "alice": 1, "bob": 2}
        rows = tm.compute_deltas(curr, {}, "2026-04-05")
        users = [r[1] for r in rows]
        self.assertEqual(users, ["alice", "bob", "charlie"])


# -----------------------------------------------------------------------
# rebuild_totals
# -----------------------------------------------------------------------

class TestRebuildTotals(TempDirMixin, unittest.TestCase):

    def _make_log(self, rows):
        path = self.tmpdir / "2026-04.csv"
        with open(path, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(tm.LOG_HEADER)
            w.writerows(rows)
        return path

    def test_single_day(self):
        log = self._make_log([
            ["2026-04-01", "alice", "100", "100", "ok"],
            ["2026-04-01", "bob", "200", "200", "ok"],
        ])
        totals = self.tmpdir / "totals.csv"
        with patch.object(tm, "MONTH", "2026-04"):
            tm.rebuild_totals(log, totals)

        rows = tm.read_csv_rows(totals)
        alice = next(r for r in rows if r[1] == "alice")
        bob = next(r for r in rows if r[1] == "bob")
        self.assertEqual(int(alice[2]), 100)
        self.assertEqual(int(bob[2]), 200)

    def test_multi_day_accumulation(self):
        log = self._make_log([
            ["2026-04-01", "alice", "100", "100", "ok"],
            ["2026-04-02", "alice", "200", "300", "ok"],
            ["2026-04-03", "alice", "50", "350", "ok"],
        ])
        totals = self.tmpdir / "totals.csv"
        with patch.object(tm, "MONTH", "2026-04"):
            tm.rebuild_totals(log, totals)

        rows = tm.read_csv_rows(totals)
        self.assertEqual(len(rows), 1)
        self.assertEqual(int(rows[0][2]), 350)  # 100 + 200 + 50

    def test_gb_column(self):
        one_gb = 1_073_741_824
        log = self._make_log([
            ["2026-04-01", "alice", str(one_gb), str(one_gb), "ok"],
        ])
        totals = self.tmpdir / "totals.csv"
        with patch.object(tm, "MONTH", "2026-04"):
            tm.rebuild_totals(log, totals)

        rows = tm.read_csv_rows(totals)
        self.assertEqual(rows[0][3], "1.000")

    def test_sorted_by_gb_descending(self):
        gb = 1_073_741_824
        log = self._make_log([
            ["2026-04-01", "small", str(gb), str(gb), "ok"],
            ["2026-04-01", "big", str(10 * gb), str(10 * gb), "ok"],
            ["2026-04-01", "medium", str(5 * gb), str(5 * gb), "ok"],
        ])
        totals = self.tmpdir / "totals.csv"
        with patch.object(tm, "MONTH", "2026-04"):
            tm.rebuild_totals(log, totals)

        rows = tm.read_csv_rows(totals)
        users = [r[1] for r in rows]
        self.assertEqual(users, ["big", "medium", "small"])

    def test_scientific_notation_in_delta(self):
        """Deltas written in scientific notation (e.g. by awk) must parse correctly."""
        log = self._make_log([
            ["2026-04-01", "alice", "4.91986e+09", "4919860000", "ok"],
            ["2026-04-02", "alice", "1.5e+08", "5069860000", "ok"],
        ])
        totals = self.tmpdir / "totals.csv"
        with patch.object(tm, "MONTH", "2026-04"):
            tm.rebuild_totals(log, totals)

        rows = tm.read_csv_rows(totals)
        self.assertEqual(int(rows[0][2]), 4919860000 + 150000000)

    def test_zero_deltas_only(self):
        log = self._make_log([
            ["2026-04-01", "alice", "0", "500", "baseline"],
        ])
        totals = self.tmpdir / "totals.csv"
        with patch.object(tm, "MONTH", "2026-04"):
            tm.rebuild_totals(log, totals)

        rows = tm.read_csv_rows(totals)
        self.assertEqual(int(rows[0][2]), 0)


# -----------------------------------------------------------------------
# _b64url
# -----------------------------------------------------------------------

class TestB64Url(unittest.TestCase):

    def test_no_padding(self):
        result = tm._b64url(b"test")
        self.assertNotIn("=", result)

    def test_url_safe_chars(self):
        # bytes that produce + and / in standard base64
        data = b"\xfb\xff\xfe"
        result = tm._b64url(data)
        self.assertNotIn("+", result)
        self.assertNotIn("/", result)

    def test_roundtrip(self):
        data = b'{"alg":"RS256","typ":"JWT"}'
        encoded = tm._b64url(data)
        # add back padding for decode
        padded = encoded + "=" * (-len(encoded) % 4)
        decoded = __import__("base64").urlsafe_b64decode(padded)
        self.assertEqual(decoded, data)


# -----------------------------------------------------------------------
# api_get (mock HTTP)
# -----------------------------------------------------------------------

class TestApiGet(unittest.TestCase):

    def test_successful_response(self):
        body = json.dumps({"ok": True, "data": [{"username": "u1", "total_octets": 42}]})

        with patch.object(tm.urllib.request, "urlopen") as mock_urlopen:
            mock_resp = MagicMock()
            mock_resp.read.return_value = body.encode()
            mock_resp.__enter__ = lambda s: s
            mock_resp.__exit__ = MagicMock(return_value=False)
            mock_urlopen.return_value = mock_resp

            result = tm.api_get("http://localhost/test")

        self.assertTrue(result["ok"])
        self.assertEqual(result["data"][0]["username"], "u1")

    def test_auth_header_set(self):
        body = json.dumps({"ok": True, "data": []})

        with patch.object(tm.urllib.request, "urlopen") as mock_urlopen:
            mock_resp = MagicMock()
            mock_resp.read.return_value = body.encode()
            mock_resp.__enter__ = lambda s: s
            mock_resp.__exit__ = MagicMock(return_value=False)
            mock_urlopen.return_value = mock_resp

            tm.api_get("http://localhost/test", "Bearer TOKEN123")

            req = mock_urlopen.call_args[0][0]
            self.assertEqual(req.get_header("Authorization"), "Bearer TOKEN123")


# -----------------------------------------------------------------------
# Month-boundary logic (integration-style)
# -----------------------------------------------------------------------

class TestMonthBoundary(TempDirMixin, unittest.TestCase):

    def setUp(self):
        super().setUp()
        self.state_dir = self.tmpdir / "state"
        self.out_dir = self.tmpdir / "out"
        self.state_dir.mkdir()
        self.out_dir.mkdir()

    def test_month_change_archives_baseline(self):
        """When month changes, prev baseline is archived and removed."""
        prev_tsv = self.state_dir / "last.tsv"
        curr_tsv = self.state_dir / "current.tsv"
        month_file = self.state_dir / "month"

        # Simulate existing state from March
        tm.write_tsv(prev_tsv, {"alice": 500})
        tm.write_tsv(curr_tsv, {"alice": 500})
        month_file.write_text("2026-03")

        # Simulate month-boundary check (April)
        stored_month = month_file.read_text().strip()
        new_month = "2026-04"
        if stored_month != new_month:
            if prev_tsv.exists():
                shutil.copy2(prev_tsv,
                             self.state_dir / f"last-{stored_month}-archived.tsv")
            prev_tsv.unlink(missing_ok=True)
            curr_tsv.unlink(missing_ok=True)

        # Verify
        self.assertFalse(prev_tsv.exists())
        self.assertFalse(curr_tsv.exists())
        archived = self.state_dir / "last-2026-03-archived.tsv"
        self.assertTrue(archived.exists())
        self.assertEqual(tm.read_tsv(archived), {"alice": 500})

    def test_same_month_no_archive(self):
        """When month is the same, baseline stays intact."""
        prev_tsv = self.state_dir / "last.tsv"
        month_file = self.state_dir / "month"

        tm.write_tsv(prev_tsv, {"alice": 500})
        month_file.write_text("2026-04")

        stored_month = month_file.read_text().strip()
        self.assertEqual(stored_month, "2026-04")
        self.assertTrue(prev_tsv.exists())


# -----------------------------------------------------------------------
# End-to-end: baseline + regular run
# -----------------------------------------------------------------------

class TestEndToEnd(TempDirMixin, unittest.TestCase):

    def setUp(self):
        super().setUp()
        self.state_dir = self.tmpdir / "state"
        self.out_dir = self.tmpdir / "out"
        self.state_dir.mkdir()
        self.out_dir.mkdir()

    def _run_cycle(self, snapshot, today, month):
        """Simulate one script cycle without API/GSheet calls."""
        prev_tsv = self.state_dir / "last.tsv"
        curr_tsv = self.state_dir / "current.tsv"
        month_file = self.state_dir / "month"
        monthly_log = self.out_dir / f"{month}.csv"
        monthly_totals = self.out_dir / f"{month}-totals.csv"

        # Month boundary
        if month_file.exists():
            stored = month_file.read_text().strip()
            if stored != month:
                if prev_tsv.exists():
                    shutil.copy2(prev_tsv,
                                 self.state_dir / f"last-{stored}-archived.tsv")
                prev_tsv.unlink(missing_ok=True)
                curr_tsv.unlink(missing_ok=True)

        # Baseline or delta
        if not prev_tsv.exists():
            tm.ensure_csv_header(monthly_log, tm.LOG_HEADER)
            rows = [[today, u, "0", str(o), "baseline"]
                    for u, o in sorted(snapshot.items())]
            tm.append_csv_rows(monthly_log, rows)
            tm.write_tsv(prev_tsv, snapshot)
            tm.write_tsv(curr_tsv, snapshot)
            month_file.write_text(month)
        else:
            tm.ensure_csv_header(monthly_log, tm.LOG_HEADER)
            prev = tm.read_tsv(prev_tsv)
            delta_rows = tm.compute_deltas(snapshot, prev, today)
            tm.append_csv_rows(monthly_log, delta_rows)
            tm.write_tsv(prev_tsv, snapshot)
            tm.write_tsv(curr_tsv, snapshot)
            month_file.write_text(month)

        with patch.object(tm, "MONTH", month):
            tm.rebuild_totals(monthly_log, monthly_totals)

        return monthly_totals

    def test_three_day_accumulation(self):
        # Day 1: baseline
        self._run_cycle(
            {"alice": 1000, "bob": 2000}, "2026-04-01", "2026-04")
        # Day 2: traffic grew
        self._run_cycle(
            {"alice": 1500, "bob": 2800}, "2026-04-02", "2026-04")
        # Day 3: more traffic
        totals_path = self._run_cycle(
            {"alice": 2000, "bob": 3500}, "2026-04-03", "2026-04")

        rows = tm.read_csv_rows(totals_path)
        totals = {r[1]: int(r[2]) for r in rows}
        # alice: 0 (baseline) + 500 + 500 = 1000
        self.assertEqual(totals["alice"], 1000)
        # bob: 0 (baseline) + 800 + 700 = 1500
        self.assertEqual(totals["bob"], 1500)

    def test_counter_reset_mid_month(self):
        self._run_cycle({"alice": 5000}, "2026-04-01", "2026-04")
        self._run_cycle({"alice": 8000}, "2026-04-02", "2026-04")
        # Counter reset: alice drops from 8000 to 100
        totals_path = self._run_cycle(
            {"alice": 100}, "2026-04-03", "2026-04")

        rows = tm.read_csv_rows(totals_path)
        total = int(rows[0][2])
        # 0 (baseline) + 3000 (normal) + 100 (reset, only curr counted)
        self.assertEqual(total, 3100)

    def test_new_user_mid_month(self):
        self._run_cycle({"alice": 100}, "2026-04-01", "2026-04")
        # bob appears on day 2
        totals_path = self._run_cycle(
            {"alice": 200, "bob": 500}, "2026-04-02", "2026-04")

        rows = tm.read_csv_rows(totals_path)
        totals = {r[1]: int(r[2]) for r in rows}
        self.assertEqual(totals["alice"], 100)
        self.assertEqual(totals["bob"], 500)

    def test_month_transition(self):
        # March: accumulate traffic
        self._run_cycle({"alice": 1000}, "2026-03-30", "2026-03")
        self._run_cycle({"alice": 2000}, "2026-03-31", "2026-03")

        # April 1: new month, should start fresh
        totals_path = self._run_cycle(
            {"alice": 2500}, "2026-04-01", "2026-04")

        rows = tm.read_csv_rows(totals_path)
        # Baseline only — delta should be 0
        self.assertEqual(int(rows[0][2]), 0)

        # April 2: first real delta
        totals_path = self._run_cycle(
            {"alice": 3000}, "2026-04-02", "2026-04")

        rows = tm.read_csv_rows(totals_path)
        self.assertEqual(int(rows[0][2]), 500)  # 3000 - 2500

        # March archive should exist
        archived = self.state_dir / "last-2026-03-archived.tsv"
        self.assertTrue(archived.exists())


# -----------------------------------------------------------------------
# Google Sheets helpers (mocked network)
# -----------------------------------------------------------------------

class TestGsheetEnsureSheet(unittest.TestCase):

    @patch.object(tm, "_sheets_request")
    def test_sheet_exists(self, mock_req):
        mock_req.return_value = {
            "sheets": [{"properties": {"title": "Totals"}}]
        }
        tm.gsheet_ensure_sheet("tok", "spreadsheet_id", "Totals")
        # Only one GET call, no create
        mock_req.assert_called_once()

    @patch.object(tm, "_sheets_request")
    def test_sheet_missing_creates(self, mock_req):
        mock_req.side_effect = [
            {"sheets": [{"properties": {"title": "Other"}}]},  # GET
            {"replies": [{}]},  # POST batchUpdate
        ]
        tm.gsheet_ensure_sheet("tok", "sid", "Totals")
        self.assertEqual(mock_req.call_count, 2)
        create_call = mock_req.call_args_list[1]
        self.assertEqual(create_call[0][0], "POST")
        self.assertIn("batchUpdate", create_call[0][1])


class TestGsheetUploadTotals(TempDirMixin, unittest.TestCase):

    @patch.object(tm, "_sheets_request")
    @patch.object(tm, "gsheet_ensure_sheet")
    def test_upload_clears_and_writes(self, mock_ensure, mock_req):
        totals = self.tmpdir / "totals.csv"
        with open(totals, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["month", "username", "month_bytes", "month_gb"])
            w.writerow(["2026-04", "alice", "1000", "0.001"])

        mock_req.return_value = {}

        tm.gsheet_upload_totals(totals, "tok", "sid", "Totals")

        # Should call clear then write
        self.assertEqual(mock_req.call_count, 2)
        clear_call = mock_req.call_args_list[0]
        self.assertIn("clear", clear_call[0][1])
        write_call = mock_req.call_args_list[1]
        self.assertEqual(write_call[0][0], "PUT")
        body = write_call[1]["body"] if "body" in write_call[1] else write_call[0][3]
        self.assertEqual(len(body["values"]), 2)  # header + 1 row


if __name__ == "__main__":
    unittest.main()
