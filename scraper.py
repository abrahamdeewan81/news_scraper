#!/usr/bin/env python3
"""
scraper.py
- Reads JSON config files from ./config/*.json
- Scrapes article data using Playwright
- Publication date/time is extracted directly from each article URL
- Appends new articles to a single Google Sheet (dedup by link)
"""

import os
import json
import re
import time
import requests

from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urljoin

import dateutil.parser
from dateutil.relativedelta import relativedelta
from bs4 import BeautifulSoup

from playwright.sync_api import sync_playwright

import gspread
from gspread.exceptions import APIError
from oauth2client.service_account import ServiceAccountCredentials


# ---------------- CONFIG ----------------

SHEET_NAME = "jharkhand_news"
CONFIG_DIR = "config"
SPREADSHEET_ID = "1y_DXPvLZVC843ED6mXmCq2NsL5pF83JJSi_6C0W3L98"

# Only articles published within the last 25 hours are saved
CUT_OFF_HOURS = 25

# ----------------------------------------


IST = timezone(timedelta(hours=5, minutes=30))


def now_ist():
    return datetime.now(IST)


# ============================================================
# GOOGLE SHEET AUTH
# ============================================================

GSHEET_CREDS_JSON = os.environ.get("GSHEET_CREDS")

if not GSHEET_CREDS_JSON:
    raise RuntimeError("Environment variable GSHEET_CREDS is not set!")

creds_dict = json.loads(GSHEET_CREDS_JSON)

scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]

creds = ServiceAccountCredentials.from_json_keyfile_dict(
    creds_dict,
    scope
)

gc = gspread.authorize(creds)

sheet = gc.open_by_key(SPREADSHEET_ID).sheet1


# ============================================================
# EXISTING LINKS
# ============================================================

def load_existing_links():
    """
    Fetch only last 2 days of links from Google Sheet
    to speed up duplicate checking.
    """

    try:
        rows = sheet.get_all_values()

    except APIError as e:
        print(f"⚠️ Failed to load sheet data: {e}")
        return set()

    links = set()

    cutoff_date = now_ist() - timedelta(days=2)

    for row in rows[1:]:

        # Date is column index 2 (3rd column)
        # Link is column index 3 (4th column)

        if len(row) > 3 and row[3].strip():
            date_str = row[2].strip() if len(row) > 2 else ""
            link = row[3].strip().rstrip("/")
            # Parse date to filter recent rows
            try:
                if date_str:
                    dt = dateutil.parser.parse(date_str)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=IST)
                    dt = dt.astimezone(IST)
                else:
                    dt = None

            except Exception:
                dt = None

            # Keep link only if published in last 2 days
            # or date is missing (safe fallback)

            if not dt or dt >= cutoff_date:
                links.add(link)

    print(
        f"✅ Loaded {len(links)} recent links "
        f"(last 2 days) from sheet"
    )

    return links


existing_links = load_existing_links()

print(
    f"✅ Loaded {len(existing_links)} existing links from sheet"
)

# DATE / TIME UTILITIES

class DateParser:

    @staticmethod
    def format(dt):
        """
        Convert datetime to IST and return standard
        Google Sheet date/time format.
        """

        if not dt:
            return ""

        return dt.astimezone(IST).strftime(
            "%Y-%m-%d %H:%M:%S"
        )

# SCRAPER

class SheetNewsScraper:

    def __init__(self, config_dir=CONFIG_DIR):

        self.config_dir = Path(config_dir)

        if not self.config_dir.exists():
            raise FileNotFoundError(
                f"Config dir not found: {self.config_dir}"
            )

        self.cutoff_time = (
            now_ist() -
            timedelta(hours=CUT_OFF_HOURS)
        )

  
    # CONFIG

    def get_configs(self):

        return [
            p for p in self.config_dir.glob("*.json")
        ]


    def load_config(self, path):

        with open(
            path,
            "r",
            encoding="utf-8"
        ) as f:

            return json.load(f)


    # ========================================================
    # MAIN RUN
    # ========================================================

    def run(self):

        configs = self.get_configs()

        total_found = 0
        total_saved = 0

        for cfg_path in configs:

            config = self.load_config(cfg_path)

            site_name = config.get(
                "site",
                cfg_path.stem
            )

            print(
                f"\n🚀 Starting: {site_name}"
            )

            try:

                articles = self.scrape_site(config)

                total_found += len(articles)

                saved = self.save_articles(articles)

                total_saved += saved

                print(
                    f"📊 {site_name}: "
                    f"found={len(articles)} | "
                    f"saved={saved}"
                )

            except Exception as e:

                print(
                    f"❌ Error processing "
                    f"{site_name}: {e}"
                )

        print(
            f"\n🎉 Done. "
            f"Total found={total_found}, "
            f"saved={total_saved} new rows."
        )


    # ========================================================
    # ARTICLE PUBLICATION DATE FROM ARTICLE URL
    # ========================================================

    def get_article_published_date(self, article_url):
        """
        Extract publication date/time directly from the article URL.

        Priority:
        1. JSON-LD datePublished
        2. JSON-LD @graph datePublished
        3. Meta publication time
        4. <time datetime>

        Converts timezone-aware timestamps to IST.

        Returns:
            timezone-aware datetime in IST
            or None if publication date cannot be found.
        """

        if not article_url:
            return None

        try:

            headers = {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/139.0.0.0 Safari/537.36"
                ),
                "Accept": (
                    "text/html,application/xhtml+xml,"
                    "application/xml;q=0.9,image/avif,"
                    "image/webp,*/*;q=0.8"
                ),
                "Accept-Language": "en-US,en;q=0.9"
            }

            print(
                f"🌐 Fetching article date: {article_url}"
            )

            response = requests.get(
                article_url,
                headers=headers,
                timeout=20
            )

            response.raise_for_status()

            soup = BeautifulSoup(
                response.text,
                "html.parser"
            )

            published_value = None


            # ------------------------------------------------
            # 1. JSON-LD datePublished
            # ------------------------------------------------

            for script in soup.find_all(
                "script",
                type="application/ld+json"
            ):

                try:

                    raw_data = (
                        script.string
                        or script.get_text()
                    )

                    if not raw_data:
                        continue

                    data = json.loads(raw_data)

                    items = (
                        data
                        if isinstance(data, list)
                        else [data]
                    )

                    for item in items:

                        if not isinstance(item, dict):
                            continue


                        # ------------------------------------
                        # Direct datePublished
                        # ------------------------------------

                        if item.get("datePublished"):

                            published_value = (
                                item["datePublished"]
                            )

                            break


                        # ------------------------------------
                        # Search inside @graph
                        # ------------------------------------

                        graph = item.get(
                            "@graph",
                            []
                        )

                        if isinstance(graph, list):

                            for graph_item in graph:

                                if (
                                    isinstance(
                                        graph_item,
                                        dict
                                    )
                                    and
                                    graph_item.get(
                                        "datePublished"
                                    )
                                ):

                                    published_value = (
                                        graph_item[
                                            "datePublished"
                                        ]
                                    )

                                    break


                        if published_value:
                            break

                except Exception:
                    # Some websites contain malformed
                    # JSON-LD. Ignore and continue.
                    continue


                if published_value:
                    break


            # ------------------------------------------------
            # 2. META TAGS
            # ------------------------------------------------

            if not published_value:

                meta_selectors = [

                    'meta[property="article:published_time"]',

                    'meta[name="datePublished"]',

                    'meta[name="publish-date"]',

                    'meta[name="published_time"]',

                    'meta[name="pubdate"]',

                ]

                for selector in meta_selectors:

                    meta = soup.select_one(
                        selector
                    )

                    if (
                        meta
                        and
                        meta.get("content")
                    ):

                        published_value = (
                            meta["content"]
                        )

                        break


            # 3. <time datetime>

            if not published_value:

                time_el = soup.find(
                    "time",
                    datetime=True
                )

                if time_el:

                    published_value = (
                        time_el.get("datetime")
                    )

            # Nothing found

            if not published_value:

                print(
                    f"⚠️ Publication date not found: "
                    f"{article_url}"
                )

                return None

            # Normalize date string


            date_string = str(
                published_value
            ).strip()


            # Handle UTC "Z"

            if date_string.endswith("Z"):

                date_string = (
                    date_string[:-1]
                    + "+00:00"
                )

            # Parse timestamp


            try:

                dt = datetime.fromisoformat(
                    date_string
                )

            except ValueError:

                # Fallback for unusual formats

                dt = dateutil.parser.parse(
                    date_string
                )


            # If timezone is missing

            if dt.tzinfo is None:

                dt = dt.replace(
                    tzinfo=IST
                )

            # Convert to IST

            dt_ist = dt.astimezone(IST)
            print(
                f"📅 Article date: "
                f"{dt_ist.strftime('%Y-%m-%d %H:%M:%S')} IST"
            )
            return dt_ist
        except requests.RequestException as e:

            print(
                f"⚠️ Failed to fetch article "
                f"{article_url}: {e}"
            )
            return None
        except Exception as e:

            print(
                f"⚠️ Failed to extract date from "
                f"{article_url}: {e}"
            )

            return None

    # ========================================================
    # SCRAPE SITE
    # ========================================================

    def scrape_site(self, config):

        base_url = config["base_url"]

        selectors = config["article"]

        limit = config.get(
            "limit",
            20
        )

        articles = []


        with sync_playwright() as p:

            browser = p.chromium.launch(
                headless=True,
                args=[
                    "--no-sandbox",
                    "--disable-dev-shm-usage"
                ]
            )

            page = browser.new_page()
            try:
                page.goto(
                    base_url,
                    wait_until="domcontentloaded",
                    timeout=45000
                )
                page.wait_for_selector(
                    selectors["container"],
                    timeout=10000
                )
                elements = page.query_selector_all(
                    selectors["container"]
                )
                print(
                    f"🔍 Found {len(elements)} elements"
                )
                for el in elements[:limit]:
                    data = self.extract_article(
                        el,
                        base_url,
                        config
                    )


                    if self.is_valid(data):

                        # ------------------------------------
                        # Cutoff based on article URL date
                        # ------------------------------------

                        if (
                            data.get("published_date")
                            and
                            data["published_date"]
                            < self.cutoff_time
                        ):

                            print(
                                f"⏩ Skipping old article: "
                                f"{data.get('title', '')[:70]} "
                                f"| "
                                f"{data['published_date']}"
                            )

                            continue


                        articles.append(data)


            except Exception as e:

                print(
                    f"⚠️ Scrape error for "
                    f"{base_url}: {e}"
                )


            finally:

                browser.close()


        return articles

    # ========================================================
    # EXTRACT ARTICLE FROM DOM
    # ========================================================

    def extract_article(
        self,
        el,
        base_url,
        config
    ):
        selectors = config["article"]

        site_name = config.get(
            "site",
            "Unknown"
        )


        def q(sel):

            return (
                el.query_selector(sel)
                if sel
                else None
            )


        # ----------------------------------------------------
        # DOM elements
        # ----------------------------------------------------

        link_el = (
            q(selectors.get("link", ""))
            or
            q(selectors.get("title", ""))
        )

        title_el = q(
            selectors.get("title", "")
        )

        snippet_el = q(
            selectors.get("snippet", "")
        )

        image_el = q(
            selectors.get("image", "")
        )

        author_el = q(
            selectors.get("author", "")
        )


        # ----------------------------------------------------
        # LINK
        # ----------------------------------------------------

        link = None
        if link_el:
            href = (
                link_el.get_attribute("href")
                or
                link_el.get_attribute("data-href")
            )
            if href:
                link = urljoin(
                    base_url,
                    href.strip()
                ).rstrip("/")

        # TITLE

        title = (
            title_el.text_content().strip()
            if title_el
            else None
        )

        # SNIPPET

        snippet = (
            snippet_el.text_content().strip()
            if snippet_el
            else None
        )

        # AUTHOR

        author = (
            author_el.text_content().strip()
            if author_el
            else None
        )

        # IMAGE

        image = None

        if image_el:

            src = (
                image_el.get_attribute("src")
                or
                image_el.get_attribute("data-src")
                or
                image_el.get_attribute("data-lazy-src")
                or
                image_el.get_attribute("srcset")
            )
            if (
                src
                and
                not src.startswith("data:")
            ):
                if "," in src:

                    src = src.split(",")[0].strip()

                    # Remove optional resolution
                    # e.g. "image.jpg 640w"
                    src = src.split()[0]


                image = urljoin(
                    base_url,
                    src.strip()
                )

        # PUBLICATION DATE

        published_date = None
        if link:
            published_date = (
                self.get_article_published_date(
                    link
                )
            )
        return {
            "source": site_name,
            "title": title,
            "link": link,
            "date_text": DateParser.format(
                published_date
            ),
            "snippet": snippet,
            "author": author,
            "image": image,
            "published_date": published_date,
            "scraped_at": DateParser.format(
                now_ist()
            ),
        }

    # VALIDATE ARTICLE

    def is_valid(self, art):

        if (
            not art.get("title")
            or
            not art.get("link")
        ):

            return False


        if len(art["title"]) < 6:

            return False


        link_clean = (
            art["link"]
            .strip()
            .rstrip("/")
        )
        if link_clean in existing_links:
            print(
                f"⏩ Skipping duplicate: "
                f"{link_clean}"
            )
            return False
        return True

    # SAVE TO GOOGLE SHEET
    def save_articles(self, articles):
        global existing_links
        saved = 0
        for a in articles:

            link_clean = (
                a["link"]
                .strip()
                .rstrip("/")
            )
            if link_clean in existing_links:

                continue
            row = [

                a.get(
                    "source",
                    ""
                ),
                a.get(
                    "title",
                    ""
                ),
                a.get(
                    "date_text",
                    ""
                ),
                link_clean,
                a.get(
                    "author",
                    ""
                ),
                a.get(
                    "snippet",
                    ""
                ),
                a.get(
                    "image",
                    ""
                ),
                a.get(
                    "scraped_at",
                    ""
                ),
            ]
            try:
                sheet.append_row(row)
                existing_links.add(
                    link_clean
                )
                saved += 1
                print(
                    f"💾 Saved: "
                    f"{a.get('title', '')[:70]}"
                )
            except APIError as e:

                print(
                    f"❌ Error saving row: {e}"
                )
                if (
                    e.response
                    and
                    e.response.status_code == 429
                ):
                    print(
                        "⚠️ Rate limit hit. "
                        "Retrying in 30s..."
                    )
                    time.sleep(30)
        return saved

# MAIN

if __name__ == "__main__":
    scraper = SheetNewsScraper(
        CONFIG_DIR
    )
    scraper.run()
