import asyncio
from playwright.async_api import async_playwright
import json
from urllib.parse import urljoin

# Load config JSON
with open("config/22scope.json", "r", encoding="utf-8") as f:
    config = json.load(f)

site = config["site"]
base_url = config["base_url"]
selectors = config["article"]

print(f"Scraping (JS-rendered): {site}")
print("=" * 60)

async def scrape_js_site():
    async with async_playwright() as p:
        browser = await p.firefox.launch(headless=True)
        page = await browser.new_page()

        await page.goto(base_url, timeout=30000)
        await page.wait_for_load_state("networkidle")

        articles = await page.query_selector_all(selectors["container"])

        for article in articles:
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

            title = await safe_text(selectors.get("title"))
            link = await safe_attr(selectors.get("link"), "href")
            image = await safe_attr(selectors.get("image"), "src")
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

        await browser.close()

asyncio.run(scrape_js_site())
