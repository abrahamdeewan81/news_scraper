import asyncio
from playwright.async_api import async_playwright
import json
from urllib.parse import urljoin

# Load config
with open("config/hindustan.json", "r", encoding="utf-8") as f:
    config = json.load(f)

site = config["site"]
base_url = config["base_url"]
selectors = config["article"]

print(f"Scraping (JS-rendered): {site}")
print("=" * 60)

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-IN,en;q=0.9,hi-IN;q=0.8,hi;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
    "DNT": "1",
    "Upgrade-Insecure-Requests": "1",
    "Referer": "https://www.google.com/"
}


async def scrape_js_site():
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            slow_mo=50,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage"
            ]
        )

        context = await browser.new_context(
            user_agent=USER_AGENT,
            viewport={"width": 1366, "height": 768},
            locale="en-IN",
            timezone_id="Asia/Kolkata",
            extra_http_headers=HEADERS,
            java_script_enabled=True
        )

        page = await context.new_page()

        # Navigation with safety
        await page.goto(base_url, wait_until="domcontentloaded", timeout=90000)
        await page.wait_for_timeout(3000)

        # Try finding article containers
        articles = await page.query_selector_all(selectors["container"])

        if len(articles) <= 1:
            print("(Fallback) Trying sibling container pattern...")
            articles = await page.query_selector_all("div.entry-image + div.entry-details")

        for index, article in enumerate(articles, start=1):

            async def safe_text(selector):
                if not selector:
                    return None
                el = await article.query_selector(selector)
                return (await el.inner_text()) if el else None

            async def safe_attr(selector, attr):
                if not selector:
                    return None
                el = await article.query_selector(selector)
                return await el.get_attribute(attr) if el else None

            image = (
                await safe_attr(selectors.get("image"), "data-bgsrc")
                or await safe_attr(selectors.get("image"), "src")
            )

            title = await safe_text(selectors.get("title"))
            link = await safe_attr(selectors.get("link"), "href")
            author = await safe_text(selectors.get("author"))
            date = await safe_text(selectors.get("date"))
            snippet = await safe_text(selectors.get("snippet"))

            full_link = urljoin(base_url, link) if link else None

            print(f"📰 Title: {title}")
            print(f"🔗 Link: {full_link}")
            print(f"🖼️ Image: {image}")
            print(f"✍️ Author: {author}")
            print(f"📅 Date: {date}")
            print(f"📝 Snippet: {snippet}")
            print("-" * 60)

        await context.close()
        await browser.close()


asyncio.run(scrape_js_site())
