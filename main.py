import os
import json
import time
import hashlib
import io
from urllib.parse import urljoin, urlparse

from flask import Flask
from google.cloud import storage
from pypdf import PdfReader

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import WebDriverException, TimeoutException

from bs4 import BeautifulSoup
import requests

app = Flask(__name__)

# ======================
# CONFIGURATION
# ======================

START_URL = "https://www.midcindia.org"
ALLOWED_DOMAIN = "midcindia.org"
BUCKET_NAME = "midc-general-chatbot-bucket-web-data"

MAX_DEPTH = 5
PAGE_LOAD_WAIT = 3
TIMEOUT = 25

visited_urls = set()

# ======================
# GCS HELPERS
# ======================

def gcs_upload(path: str, payload: dict):
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(path)
    blob.upload_from_string(
        json.dumps(payload, indent=2, ensure_ascii=False),
        content_type="application/json"
    )

# ======================
# UTILITIES
# ======================

def url_id(url: str) -> str:
    return hashlib.sha256(url.encode()).hexdigest()[:16]

def is_internal(url: str) -> bool:
    try:
        return ALLOWED_DOMAIN in urlparse(url).netloc
    except Exception:
        return False

# ======================
# SELENIUM DRIVER
# ======================

def get_driver():
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    return webdriver.Chrome(options=options)

# ======================
# PDF EXTRACTION
# ======================

def extract_pdf_text(url: str):
    response = requests.get(url, timeout=30)
    reader = PdfReader(io.BytesIO(response.content))

    text = ""
    for page in reader.pages:
        page_text = page.extract_text()
        if page_text:
            text += page_text + "\n"

    return text

# ======================
# PAGE EXTRACTION
# ======================

def extract_page(driver, url: str):
    soup = BeautifulSoup(driver.page_source, "html.parser")

    page = {
        "url": url,
        "title": soup.title.text.strip() if soup.title else "",
        "meta": {},
        "text": " ".join(soup.stripped_strings),
        "links": [],
        "forms": [],
        "buttons": []
    }

    # Meta tags
    for meta in soup.find_all("meta"):
        if meta.get("name") and meta.get("content"):
            page["meta"][meta["name"]] = meta["content"]

    # Links
    for a in soup.find_all("a", href=True):
        href = urljoin(url, a["href"])
        page["links"].append({
            "text": a.get_text(strip=True),
            "url": href,
            "internal": is_internal(href)
        })

    # Forms
    for form in soup.find_all("form"):
        fields = []
        for inp in form.find_all(["input", "select", "textarea"]):
            fields.append({
                "name": inp.get("name"),
                "type": inp.get("type", inp.name),
                "required": inp.has_attr("required")
            })

        form_data = {
            "source_page": url,
            "action": form.get("action"),
            "method": form.get("method", "GET"),
            "fields": fields
        }

        gcs_upload(f"forms/{url_id(url)}.json", form_data)
        page["forms"].append(form_data)

    # Buttons
    for btn in soup.find_all("button"):
        page["buttons"].append(btn.get_text(strip=True))

    return page

# ======================
# CRAWLER
# ======================

def crawl(url: str, depth: int = 0):
    if url in visited_urls or depth > MAX_DEPTH:
        return

    visited_urls.add(url)
    driver = None

    try:
        driver = get_driver()
        driver.set_page_load_timeout(TIMEOUT)
        driver.get(url)
        time.sleep(PAGE_LOAD_WAIT)

        # Handle PDFs
        if url.lower().endswith(".pdf"):
            pdf_text = extract_pdf_text(url)
            gcs_upload(
                f"pdfs/{url_id(url)}.json",
                {
                    "url": url,
                    "content_type": "pdf",
                    "text": pdf_text
                }
            )
            return

        page = extract_page(driver, url)
        gcs_upload(f"pages/{url_id(url)}.json", page)

        for link in page["links"]:
            if link["internal"]:
                crawl(link["url"], depth + 1)
            else:
                gcs_upload(
                    f"external_links/{url_id(link['url'])}.json",
                    link
                )

    except Exception as e:
        gcs_upload(
            f"errors/{url_id(url)}.json",
            {
                "url": url,
                "error": str(e)
            }
        )

    finally:
        if driver:
            driver.quit()

# ======================
# CLOUD RUN ENTRYPOINT
# ======================

@app.route("/", methods=["GET"])
def run_indexer():
    crawl(START_URL)
    return "MIDC agentic crawling & indexing completed successfully", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
