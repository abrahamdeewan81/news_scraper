import requests
from bs4 import BeautifulSoup
import json
from urllib.parse import urljoin

# Load config JSON
with open("config/thenewspost.json", "r", encoding="utf-8") as f:
    config = json.load(f)

site = config["site"]
base_url = config["base_url"]
selectors = config["article"]

print(f"Scraping from: {site}")
print("=" * 60)

# Fetch page
response = requests.get(base_url, timeout=15)
soup = BeautifulSoup(response.text, "html.parser")

# Find all article containers
articles = soup.select(selectors["container"])

for article in articles:
    def safe_select(selector):
        if not selector:
            return None
        el = article.select_one(selector)
        return el.get_text(strip=True) if el else None

    def safe_attr(selector, attr):
        if not selector:
            return None
        el = article.select_one(selector)
        return el.get(attr) if el and el.has_attr(attr) else None

    title_el = article.select_one(selectors["title"]) if selectors["title"] else None
    link_el = article.select_one(selectors["link"]) if selectors["link"] else None
    image_el = article.select_one(selectors["image"]) if selectors["image"] else None

    title = title_el.get_text(strip=True) if title_el else None
    link = urljoin(base_url, link_el["href"]) if link_el and link_el.has_attr("href") else None
    image = image_el["src"] if image_el and image_el.has_attr("src") else None
    author = safe_select(selectors.get("author"))
    date = safe_select(selectors.get("date"))
    snippet = safe_select(selectors.get("snippet"))

    print(f"📰 Title: {title}")
    print(f"🔗 Link: {link}")
    print(f"🖼️ Image: {image}")
    print(f"✍️ Author: {author}")
    print(f"📅 Date: {date}")
    print(f"📝 Snippet: {snippet}")
    print("-" * 60)
