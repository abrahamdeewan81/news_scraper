#!/usr/bin/env python3
"""
cleanup_old_articles.py
Deletes Google Sheet rows where article date is older than 3 days
"""

import os
import json
from datetime import datetime, timedelta, timezone
import dateutil.parser
import gspread
from gspread.exceptions import APIError
from oauth2client.service_account import ServiceAccountCredentials

# ---------------- CONFIG ----------------
SPREADSHEET_ID = "1y_DXPvLZVC843ED6mXmCq2NsL5pF83JJSi_6C0W3L98"
MAX_AGE_DAYS = 3
DATE_COL_INDEX = 2  # 0-based index (Column C: date_text)
# ---------------------------------------

IST = timezone(timedelta(hours=5, minutes=30))

def now_ist():
    return datetime.now(IST)

# ---------------- GOOGLE SHEET AUTH ----------------
GSHEET_CREDS_JSON = os.environ.get("GSHEET_CREDS")
if not GSHEET_CREDS_JSON:
    raise RuntimeError("Environment variable GSHEET_CREDS is not set!")

creds_dict = json.loads(GSHEET_CREDS_JSON)
scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
gc = gspread.authorize(creds)
sheet = gc.open_by_key(SPREADSHEET_ID).sheet1

# ---------------- CLEANUP LOGIC ----------------
def cleanup_old_articles():
    rows = sheet.get_all_values()
    if len(rows) <= 1:
        print("ℹ️ Sheet is empty or only has header")
        return

    cutoff = now_ist() - timedelta(days=MAX_AGE_DAYS)
    rows_to_delete = []

    # Skip header (row index starts at 1 in Sheets)
    for idx, row in enumerate(rows[1:], start=2):
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
            rows_to_delete.append(idx)

    if not rows_to_delete:
        print("✅ No articles older than 3 days found")
        return

    # Delete from bottom to top
    rows_to_delete.reverse()

    deleted = 0
    for row_num in rows_to_delete:
        try:
            sheet.delete_rows(row_num)
            deleted += 1
        except APIError as e:
            print(f"❌ Failed to delete row {row_num}: {e}")

    print(f"🧹 Cleanup complete. Deleted {deleted} old articles.")

if __name__ == "__main__":
    cleanup_old_articles()
