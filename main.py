import os
import json
import time
import hashlib
import datetime
from urllib.parse import urljoin, urlparse

from google.cloud import storage
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup

# =========================
# CONFIGURATION
# =========================

BUCKET_NAME = "midc-general-chatbot-bucket-web-data"
START_URLS = [
    "https://www.midcindia.org",
    "https://cmhelpline.mp.gov.in/cmhlwhatsapp.aspx"
]

ALLOWED_DOMAINS = (
    "midcindia.org",
    "gov.in",
    "nic.in"
)

MAX_PAGES = 500
CRAWL_DELAY = 1.5

# =========================
# GCS CLIENT
# =========================

storage_client = storage.Client()
bucket = storage_client.bucket(BUCKET_NAME)

# =========================
# UTILITIES
# =========================

def hash_id(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()[:20]

def upload_json(path: str, payload: dict):
    blob = bucket.blob(path)
    blob.upload_from_string(
        json.dumps(payload, indent=2, ensure_ascii=False),
        content_type="application/json"
    )

def is_allowed(url: str) -> bool:
    parsed = urlparse(url)
    return parsed.scheme in ("http", "https") and any(
        d in parsed.netloc for d in ALLOWED_DOMAINS
    )

# =========================
# SELENIUM SETUP
# =========================

def create_driver():
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    return webdriver.Chrome(options=options)

# =========================
# EXTRACTION LOGIC
# =========================

def extract_page(driver, url):
    driver.get(url)
    time.sleep(2)

    soup = BeautifulSoup(driver.page_source, "html.parser")

    for tag in soup(["script", "style", "nav", "footer", "header", "aside"]):
        tag.decompose()

    text = " ".join(soup.get_text(separator=" ").split())

    links = set()
    for a in soup.find_all("a", href=True):
        full = urljoin(url, a["href"])
        if is_allowed(full):
            links.add(full)

    forms = []
    for form in soup.find_all("form"):
        fields = []
        for inp in form.find_all(["input", "select", "textarea"]):
            fields.append({
                "name": inp.get("name"),
                "type": inp.get("type", inp.name),
                "required": inp.has_attr("required"),
                "placeholder": inp.get("placeholder")
            })

        forms.append({
            "action": urljoin(url, form.get("action", "")),
            "method": form.get("method", "GET").upper(),
            "fields": fields
        })

    return {
        "url": url,
        "domain": urlparse(url).netloc,
        "title": soup.title.text.strip() if soup.title else "",
        "text": text,
        "forms": forms,
        "links": list(links),
        "timestamp": datetime.datetime.utcnow().isoformat()
    }

# =========================
# MAIN CRAWLER
# =========================

def run():
    driver = create_driver()
    visited = set()
    queue = START_URLS.copy()

    manifest = []

    try:
        while queue and len(visited) < MAX_PAGES:
            url = queue.pop(0)
            if url in visited:
                continue

            print(f"[CRAWLING] {url}")
            visited.add(url)

            try:
                data = extract_page(driver, url)
            except Exception as e:
                print(f"[FAILED] {url} → {e}")
                continue

            page_id = hash_id(url)
            domain_type = "external_govt" if "midcindia.org" not in url else "midc"

            upload_json(
                f"pages/{domain_type}/{page_id}.json",
                data
            )

            if data["forms"]:
                upload_json(
                    f"forms/{page_id}.json",
                    {
                        "source_url": url,
                        "forms": data["forms"]
                    }
                )

            manifest.append({
                "url": url,
                "page_id": page_id,
                "domain_type": domain_type
            })

            for link in data["links"]:
                if link not in visited and link not in queue:
                    queue.append(link)

            time.sleep(CRAWL_DELAY)

    finally:
        driver.quit()

    upload_json("crawl_manifest.json", {
        "total_pages": len(manifest),
        "generated_at": datetime.datetime.utcnow().isoformat(),
        "pages": manifest
    })

    print("Crawling completed successfully")

# =========================
# ENTRYPOINT
# =========================

if __name__ == "__main__":
    run()
