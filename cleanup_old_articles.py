"""
cleanup_old_articles.py
Deletes Google Sheet rows older than N days using batchUpdate (single write)
"""

import os
import json
from datetime import datetime, timedelta, timezone
import dateutil.parser
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# ---------------- CONFIG ----------------
SPREADSHEET_ID = "1y_DXPvLZVC843ED6mXmCq2NsL5pF83JJSi_6C0W3L98"
MAX_AGE_DAYS = 3
DATE_COL_INDEX = 2  # Column C (0-based)
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
sheet = spreadsheet.sheet1
worksheet_id = sheet._properties["sheetId"]

# ---------------- HELPERS ----------------
def merge_contiguous_rows(rows):
    """
    Convert [2,3,4,7,8] → [(2,5), (7,9)]
    (Google API endIndex is exclusive)
    """
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
    rows = sheet.get_all_values()
    if len(rows) <= 1:
        print("ℹ️ No data rows found")
        return

    cutoff = now_ist() - timedelta(days=MAX_AGE_DAYS)
    rows_to_delete = []

    for idx, row in enumerate(rows[1:], start=2):  # Sheet rows start at 1
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
            rows_to_delete.append(idx - 1)  # API is 0-based

    if not rows_to_delete:
        print("✅ No articles older than 3 days")
        return

    ranges = merge_contiguous_rows(rows_to_delete)

    requests = []
    for start, end in ranges:
        requests.append({
            "deleteDimension": {
                "range": {
                    "sheetId": worksheet_id,
                    "dimension": "ROWS",
                    "startIndex": start,
                    "endIndex": end
                }
            }
        })

    spreadsheet.batch_update({"requests": requests})

    print(f"🧹 Deleted {len(rows_to_delete)} rows in ONE batch request")

# ---------------- MAIN ----------------
if __name__ == "__main__":
    cleanup_old_articles()
