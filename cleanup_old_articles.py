#!/usr/bin/env python3
"""
cleanup_old_articles.py
Archives old rows to 'Archive' sheet and deletes them safely in one batch update.
"""

import os
import json
from datetime import datetime, timedelta, timezone
import dateutil.parser
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# ---------------- CONFIG ----------------
SPREADSHEET_ID = "1y_DXPvLZVC843ED6mXmCq2NsL5pF83JJSi_6C0W3L98"
MAIN_SHEET_NAME = "Sheet1"
ARCHIVE_SHEET_NAME = "Archive"
MAX_AGE_DAYS = 3
DATE_COL_INDEX = 2  # 0-based (Column C)
# ---------------------------------------

IST = timezone(timedelta(hours=5, minutes=30))

def now_ist():
    return datetime.now(IST)

# ---------------- AUTH ----------------
GSHEET_CREDS_JSON = os.environ.get("GSHEET_CREDS")
if not GSHEET_CREDS_JSON:
    raise RuntimeError("GSHEET_CREDS not set")

creds_dict = json.loads(GSHEET_CREDS_JSON)
scope = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
gc = gspread.authorize(creds)

spreadsheet = gc.open_by_key(SPREADSHEET_ID)
main_sheet = spreadsheet.worksheet(MAIN_SHEET_NAME)
worksheet_id = main_sheet._properties["sheetId"]

# Ensure archive sheet exists
try:
    archive_sheet = spreadsheet.worksheet(ARCHIVE_SHEET_NAME)
except gspread.WorksheetNotFound:
    archive_sheet = spreadsheet.add_worksheet(
        title=ARCHIVE_SHEET_NAME,
        rows="1000",
        cols="20"
    )
    archive_sheet.append_row(main_sheet.row_values(1))  # copy headers

# ---------------- HELPERS ----------------
def merge_contiguous_rows(rows):
    if not rows:
        return []

    rows = sorted(rows)
    ranges = []
    start = prev = rows[0]

    for r in rows[1:]:
        if r == prev + 1:
            prev = r
        else:
            ranges.append((start, prev + 1))
            start = prev = r

    ranges.append((start, prev + 1))
    return ranges

# ---------------- CLEANUP ----------------
def cleanup_old_articles():
    rows = main_sheet.get_all_values()
    total_rows = len(rows)

    if total_rows <= 1:
        print("ℹ️ No data rows found")
        return

    cutoff = now_ist() - timedelta(days=MAX_AGE_DAYS)

    rows_to_delete_idx = []
    rows_to_archive = []

    for sheet_row_num, row in enumerate(rows[1:], start=2):
        if len(row) <= DATE_COL_INDEX:
            continue

        date_str = row[DATE_COL_INDEX].strip()
        if not date_str:
            continue

        try:
            dt = dateutil.parser.parse(date_str)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=IST)
        except Exception:
            continue

        if dt < cutoff:
            rows_to_delete_idx.append(sheet_row_num - 1)  # 0-based
            rows_to_archive.append(row)

    if not rows_to_delete_idx:
        print("✅ No rows older than threshold")
        return

    # -------- ARCHIVE (single write) --------
    archive_sheet.append_rows(rows_to_archive, value_input_option="RAW")
    print(f"📦 Archived {len(rows_to_archive)} rows")

    # -------- DELETE (single batch) --------
    delete_ranges = merge_contiguous_rows(rows_to_delete_idx)
    delete_ranges.sort(reverse=True)  # CRITICAL

    requests = []
    for start, end in delete_ranges:
        if start >= total_rows:
            continue
        requests.append({
            "deleteDimension": {
                "range": {
                    "sheetId": worksheet_id,
                    "dimension": "ROWS",
                    "startIndex": start,
                    "endIndex": min(end, total_rows)
                }
            }
        })

    spreadsheet.batch_update({"requests": requests})
    print(f"🧹 Deleted {len(rows_to_delete_idx)} rows safely")

# ---------------- MAIN ----------------
if __name__ == "__main__":
    cleanup_old_articles()
