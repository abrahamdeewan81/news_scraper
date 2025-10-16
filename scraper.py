#!/usr/bin/env python3
"""
scraper.py
- Reads JSON config files from ./config/*.json
- Scrapes article data using Playwright
- Appends new articles to a single Google Sheet (dedup by link)
"""

import os
import json
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urljoin
import dateutil.parser
from dateutil.relativedelta import relativedelta
from playwright.sync_api import sync_playwright
import gspread
from gspread.exceptions import APIError
from oauth2client.service_account import ServiceAccountCredentials

# ---------------- CONFIG ----------------
SHEET_NAME = "jharkhand_news"
CONFIG_DIR = "config"
SPREADSHEET_ID = "1y_DXPvLZVC843ED6mXmCq2NsL5pF83JJSi_6C0W3L98"
CUT_OFF_HOURS = 25
# ----------------------------------------

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

# ---------------- EXISTING LINKS ----------------
def load_existing_links():
    """Fetch only last 2 days of links from Google Sheet to speed up duplicate checking."""
    try:
        rows = sheet.get_all_values()
    except APIError as e:
        print(f"⚠️ Failed to load sheet data: {e}")
        return set()

    links = set()
    cutoff_date = now_ist() - timedelta(days=2)

    for row in rows[1:]:
        # Assuming the date is stored in column index 2 (3rd column) → 'date_text'
        # and link in index 3 (4th column)
        if len(row) > 3 and row[3].strip():
            date_str = row[2].strip() if len(row) > 2 else ""
            link = row[3].strip().rstrip("/")

            # Parse date to filter recent rows
            try:
                if date_str:
                    dt = dateutil.parser.parse(date_str)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=IST)
                else:
                    dt = None
            except Exception:
                dt = None

            # Keep link only if published in last 2 days or date missing (safe fallback)
            if not dt or dt >= cutoff_date:
                links.add(link)

    print(f"✅ Loaded {len(links)} recent links (last 2 days) from sheet")
    return links

existing_links = load_existing_links()
print(f"✅ Loaded {len(existing_links)} existing links from sheet")

# ---------------- DATE PARSER ----------------
class DateParser:
    @staticmethod
    def parse_date(date_text):
        if not date_text:
            return None
        text = str(date_text).strip()
        text = re.sub(r"[,|•]", " ", text)
        now = now_ist()
        if "ago" in text.lower():
            return DateParser.parse_relative(text, now)
        if "today" in text.lower():
            return now.replace(hour=12, minute=0, second=0, microsecond=0)
        if "yesterday" in text.lower():
            return (now - timedelta(days=1)).replace(hour=12, minute=0, second=0, microsecond=0)
        try:
            parsed = dateutil.parser.parse(text, fuzzy=True)
            return parsed.replace(tzinfo=IST, hour=12, minute=0, second=0, microsecond=0)
        except Exception:
            return None

    @staticmethod
    def parse_relative(text, now):
        patterns = [
            (r"(\d+)\s+hour", "hours"),
            (r"(\d+)\s+hr", "hours"),
            (r"(\d+)\s+minute", "minutes"),
            (r"(\d+)\s+min", "minutes"),
            (r"(\d+)\s+day", "days"),
            (r"(\d+)\s+week", "weeks"),
            (r"(\d+)\s+month", "months"),
            (r"(\d+)\s+year", "years"),
        ]
        for patt, unit in patterns:
            m = re.search(patt, text, re.IGNORECASE)
            if m:
                n = int(m.group(1))
                delta = {
                    "hours": timedelta(hours=n),
                    "minutes": timedelta(minutes=n),
                    "days": timedelta(days=n),
                    "weeks": timedelta(weeks=n),
                    "months": relativedelta(months=n),
                    "years": relativedelta(years=n),
                }[unit]
                return now - delta
        return None

    @staticmethod
    def format(dt):
        return dt.astimezone(IST).strftime("%Y-%m-%d %H:%M:%S") if dt else ""

# ---------------- SCRAPER ----------------
class SheetNewsScraper:
    def __init__(self, config_dir=CONFIG_DIR):
        self.config_dir = Path(config_dir)
        if not self.config_dir.exists():
            raise FileNotFoundError(f"Config dir not found: {self.config_dir}")
        self.cutoff_time = now_ist() - timedelta(hours=CUT_OFF_HOURS)

    def get_configs(self):
        return [p for p in self.config_dir.glob("*.json")]

    def load_config(self, path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    def run(self):
        configs = self.get_configs()
        total_found = 0
        total_saved = 0
        for cfg_path in configs:
            config = self.load_config(cfg_path)
            site_name = config.get("site", cfg_path.stem)
            print(f"\n🚀 Starting: {site_name}")
            try:
                articles = self.scrape_site(config)
                total_found += len(articles)
                saved = self.save_articles(articles)
                total_saved += saved
                print(f"📊 {site_name}: found={len(articles)} | saved={saved}")
            except Exception as e:
                print(f"❌ Error processing {site_name}: {e}")
        print(f"\n🎉 Done. Total found={total_found}, saved={total_saved} new rows.")

    def scrape_site(self, config):
        base_url = config["base_url"]
        selectors = config["article"]
        limit = config.get("limit", 20)
        articles = []
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])
            page = browser.new_page()
            try:
                page.goto(base_url, wait_until="domcontentloaded", timeout=45000)
                page.wait_for_selector(selectors["container"], timeout=10000)
                elements = page.query_selector_all(selectors["container"])
                print(f"🔍 Found {len(elements)} elements")
                for el in elements[:limit]:
                    data = self.extract_article(el, base_url, config)
                    if self.is_valid(data):
                        if data.get("published_date") and data["published_date"] < self.cutoff_time:
                            continue
                        articles.append(data)
            except Exception as e:
                print(f"⚠️ Scrape error for {base_url}: {e}")
            finally:
                browser.close()
        return articles

    def extract_article(self, el, base_url, config):
        selectors = config["article"]
        site_name = config.get("site", "Unknown")

        def q(sel):
            return el.query_selector(sel) if sel else None

        link_el = q(selectors.get("link", "")) or q(selectors.get("title", ""))
        title_el = q(selectors.get("title", ""))
        date_el = q(selectors.get("date", ""))
        snippet_el = q(selectors.get("snippet", ""))
        image_el = q(selectors.get("image", ""))
        author_el = q(selectors.get("author", ""))

        link = None
        if link_el:
            href = link_el.get_attribute("href") or link_el.get_attribute("data-href")
            if href:
                link = urljoin(base_url, href.strip()).rstrip("/")

        title = title_el.text_content().strip() if title_el else None
        snippet = snippet_el.text_content().strip() if snippet_el else None
        author = author_el.text_content().strip() if author_el else None

        image = None
        if image_el:
            src = (
                image_el.get_attribute("src") or 
                image_el.get_attribute("data-src") or
                image_el.get_attribute("data-lazy-src") or
                image_el.get_attribute("srcset")
            )
            if src and not src.startswith("data:"):
                image = urljoin(base_url, src.strip())
        image_el = q(selectors.get("image", ""))

        published_date = None
        if date_el:
            published_date = DateParser.parse_date(date_el.text_content())

        return {
            "source": site_name,
            "title": title,
            "link": link,
            "date_text": DateParser.format(published_date),
            "snippet": snippet,
            "author": author,
            "image": image,
            "published_date": published_date,
            "scraped_at": DateParser.format(now_ist()),
        }

    def is_valid(self, art):
        if not art.get("title") or not art.get("link"):
            return False
        if len(art["title"]) < 6:
            return False
        link_clean = art["link"].strip().rstrip("/")
        if link_clean in existing_links:
            print(f"⏩ Skipping duplicate: {link_clean}")
            return False
        return True

    def save_articles(self, articles):
        global existing_links
        saved = 0
        for a in articles:
            link_clean = a["link"].strip().rstrip("/")
            if link_clean in existing_links:
                continue
            row = [
                a.get("source", ""),
                a.get("title", ""),
                a.get("date_text", ""),
                link_clean,
                a.get("author", ""),
                a.get("snippet", ""),
                a.get("image", ""),
                a.get("scraped_at", ""),
            ]
            try:
                sheet.append_row(row)
                existing_links.add(link_clean)
                saved += 1
                print(f"💾 Saved: {a.get('title')[:70]}")
            except APIError as e:
                print(f"❌ Error saving row: {e}")
                if e.response.status_code == 429:
                    print("⚠️ Rate limit hit. Retrying in 30s...")
                    time.sleep(30)
        return saved

if __name__ == "__main__":
    scraper = SheetNewsScraper(CONFIG_DIR)
    scraper.run()
