#!/usr/bin/env python3
"""
scraper.py
- Reads JSON config files from ./config/*.json
- Scrapes article data using Playwright
- Appends new articles to a single Google Sheet (dedup by link)

Requirements:
  pip install playwright gspread oauth2client python-dateutil
  playwright install
"""

import os
import json
import re
import time
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import urljoin
from gspread.exceptions import APIError
import dateutil.parser
from dateutil.relativedelta import relativedelta
from playwright.sync_api import sync_playwright
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# ---------------- CONFIG ----------------
SHEET_NAME = "jharkhand_news"
CONFIG_DIR = "config"
SPREADSHEET_ID = "1y_DXPvLZVC843ED6mXmCq2NsL5pF83JJSi_6C0W3L98"
CUT_OFF_HOURS = 25
# ----------------------------------------

# Google Sheets Auth from GitHub Secret
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

# ---------------- Load Existing Links ----------------
def load_existing_links(days_limit=2):
    all_rows = sheet.get_all_values()
    links = set()
    if len(all_rows) <= 1:
        return links
    now = datetime.now()
    cutoff = now - timedelta(days=days_limit)
    for row in all_rows[1:]:
        if len(row) < 4:
            continue
        date_str = row[2].strip() if len(row) > 2 else ""
        link = row[3].strip().rstrip("/") if len(row) > 3 else ""
        if not link:
            continue
        try:
            if date_str:
                parsed_date = dateutil.parser.parse(date_str)
                if parsed_date >= cutoff:
                    links.add(link)
        except Exception:
            continue
    return links

existing_links = load_existing_links()
print(f"✅ Loaded {len(existing_links)} existing links from sheet")

# ---------------- Date Parser ----------------
class DateParser:
    @staticmethod
    def parse_date(date_text):
        if not date_text:
            return None
        text = str(date_text).strip()
        for p in [
            r"BY\s+[\w\s]+",
            r"by\s+[\w\s]+",
            r"Posted\s+on",
            r"Published\s+on",
            r"Updated\s+on",
            r"•",
            r"\|\s*",
            r"\s-\s",
        ]:
            text = re.sub(p, " ", text, flags=re.IGNORECASE).strip()
        text = re.sub(r"[,|•]", " ", text).strip()
        now = datetime.now()
        if "ago" in text.lower():
            return DateParser.parse_relative(text, now)
        if "today" in text.lower():
            return now.replace(hour=12, minute=0, second=0, microsecond=0)
        if "yesterday" in text.lower():
            return (now - timedelta(days=1)).replace(hour=12, minute=0, second=0, microsecond=0)
        try:
            parsed = dateutil.parser.parse(text, fuzzy=True)
            return parsed.replace(hour=12, minute=0, second=0, microsecond=0)
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
                if unit == "hours":
                    return now - timedelta(hours=n)
                if unit == "minutes":
                    return now - timedelta(minutes=n)
                if unit == "days":
                    return now - timedelta(days=n)
                if unit == "weeks":
                    return now - timedelta(weeks=n)
                if unit == "months":
                    return now - relativedelta(months=n)
                if unit == "years":
                    return now - relativedelta(years=n)
        return None

    @staticmethod
    def format(dt):
        return dt.strftime("%Y-%m-%d %H:%M:%S") if dt else ""

# ---------------- Scraper ----------------
class SheetNewsScraper:
    def __init__(self, config_dir=CONFIG_DIR):
        self.config_dir = Path(config_dir)
        if not self.config_dir.exists():
            raise FileNotFoundError(f"Config dir not found: {self.config_dir}")
        self.cutoff_time = datetime.now() - timedelta(hours=CUT_OFF_HOURS)

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
            try:
                config = self.load_config(cfg_path)
                site_articles = self.scrape_site(config)
                total_found += len(site_articles)
                saved = self.save_articles(site_articles)
                total_saved += saved
                print(f"📊 {config.get('site', cfg_path.stem)}: found={len(site_articles)} saved={saved}")
            except Exception as e:
                print(f"❌ Error processing {cfg_path}: {e}")
        print(f"\n🎉 Done. Found {total_found} articles, saved {total_saved} new rows.")

    def scrape_site(self, config):
        site_name = config.get("site", config.get("source_name", "Unknown"))
        base_url = config["base_url"]
        selectors = config["article"]
        limit = config.get("limit", 20)
        articles = []

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])
            context = browser.new_context(viewport={"width": 1280, "height": 800})
            page = context.new_page()
            try:
                print(f"🌐 Visiting {base_url}")
                page.goto(base_url, wait_until="domcontentloaded", timeout=45000)
                try:
                    page.wait_for_selector(selectors["container"], timeout=15000)
                except Exception:
                    pass
                time.sleep(1)
                elements = page.query_selector_all(selectors["container"])
                print(f"🔎 Found {len(elements)} candidate elements on {site_name}")
                for el in elements[:limit]:
                    try:
                        data = self.extract_article(el, page, config)
                        if self.is_valid(data):
                            if data.get("published_date") and data["published_date"] < self.cutoff_time:
                                continue
                            articles.append(data)
                    except Exception:
                        continue
            except Exception as e:
                print(f"❌ Scrape error for {site_name}: {e}")
            finally:
                browser.close()
        return articles

    def extract_article(self, el, page, config):
        selectors = config["article"]
        base = config["base_url"]
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
                link = urljoin(base, href.strip()).rstrip("/")

        title = title_el.text_content().strip() if title_el else (link_el.text_content().strip() if link_el else None)
        snippet = snippet_el.text_content().strip() if snippet_el else None
        author = author_el.text_content().strip() if author_el else None

        image = None
        if image_el:
            src = image_el.get_attribute("src") or image_el.get_attribute("data-src") or image_el.get_attribute("data-lazy")
            if src:
                image = urljoin(base, src.strip())

        published_date = None
        if date_el:
            raw = date_el.text_content().strip()
            published_date = DateParser.parse_date(raw)

        return {
            "source": site_name,
            "title": title,
            "link": link,
            "date_text": DateParser.format(published_date) if published_date else "",
            "snippet": snippet,
            "author": author,
            "image": image,
            "published_date": published_date,
            "scraped_at": DateParser.format(datetime.now()),
        }

    def is_valid(self, art):
        if not art.get("title") or not art.get("link"):
            return False
        if len(art["title"]) < 6:
            return False
        if not art["link"].startswith("http"):
            return False
        if art["link"].strip().rstrip("/") in existing_links:
            return False
        return True

    def save_articles(self, articles):
        global existing_links
        existing_links = load_existing_links()  # refresh links before saving
        saved = 0
        for a in articles:
            link_clean = a["link"].strip().rstrip("/")
            if link_clean in existing_links:
                continue
            row = [
                a.get("source") or "",
                a.get("title") or "",
                a.get("date_text") or "",
                link_clean,
                a.get("author") or "",
                a.get("snippet") or "",
                a.get("image") or "",
                a.get("scraped_at") or "",
            ]
            while True:
                try:
                    sheet.append_row(row)
                    existing_links.add(link_clean)
                    saved += 1
                    print(f"💾 Saved: {a.get('title')[:70]}")
                    break
                except APIError as e:
                    if e.response.status_code == 429:
                        print("⚠️ Quota exceeded. Waiting 30 seconds before retry...")
                        time.sleep(30)
                    else:
                        print(f"❌ Error saving row: {e}")
                        break
        return saved

# ---------------- Run ----------------
if __name__ == "__main__":
    scraper = SheetNewsScraper(CONFIG_DIR)
    scraper.run()
