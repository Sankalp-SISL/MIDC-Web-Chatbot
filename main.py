import os
import requests
import json
import datetime
import io
from urllib.parse import urljoin, urlparse

from flask import Flask
from bs4 import BeautifulSoup
from google.cloud import storage
from pypdf import PdfReader

app = Flask(__name__)

# ======================
# CONFIGURATION
# ======================

BUCKET_NAME = "midc-general-chatbot-bucket-web-data"
ROOT_URL = "https://www.midcindia.org"

MAX_PAGES = 300          # safety cap
REQUEST_TIMEOUT = 30

visited = set()
session = requests.Session()

# ======================
# HELPERS
# ======================

def clean_html(html: str):
    soup = BeautifulSoup(html, "html.parser")

    for tag in soup(["script", "style", "nav", "footer", "aside"]):
        tag.decompose()

    text = " ".join(soup.get_text(separator=" ").split())

    headings = [h.get_text(strip=True) for h in soup.find_all(["h1", "h2", "h3"])]
    links = [a.get("href") for a in soup.find_all("a", href=True)]

    forms = []
    for form in soup.find_all("form"):
        fields = []
        for inp in form.find_all(["input", "select", "textarea"]):
            fields.append({
                "name": inp.get("name"),
                "type": inp.get("type"),
                "required": inp.has_attr("required")
            })

        forms.append({
            "action": form.get("action"),
            "method": form.get("method", "GET"),
            "fields": fields
        })

    return text, headings, links, forms


def extract_pdf_text(url: str):
    try:
        response = session.get(url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        reader = PdfReader(io.BytesIO(response.content))
        return "\n".join(filter(None, [p.extract_text() for p in reader.pages]))
    except Exception:
        return ""


def chunk_text(text: str, size=450):
    words = text.split()
    return [
        " ".join(words[i:i+size])
        for i in range(0, len(words), size)
        if len(words[i:i+size]) > 50
    ]


def upload_payload(path: str, payload: dict):
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(path)

    blob.upload_from_string(
        json.dumps(payload, indent=2, ensure_ascii=False),
        content_type="application/json"
    )


def is_internal(url):
    return url.startswith(ROOT_URL)


# ======================
# CRAWLER
# ======================

def crawl(url):
    if url in visited or len(visited) >= MAX_PAGES:
        return

    visited.add(url)
    print(f"[CRAWLING] {url}")

    try:
        r = session.get(url, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
    except Exception:
        return

    content_type = r.headers.get("Content-Type", "")

    timestamp = datetime.datetime.utcnow().isoformat()
    parsed = urlparse(url)
    path_key = parsed.path.strip("/").replace("/", "_") or "home"

    # ---------- PDF ----------
    if "pdf" in content_type or url.lower().endswith(".pdf"):
        pdf_text = extract_pdf_text(url)
        upload_payload(
            f"pdf/{path_key}.json",
            {
                "url": url,
                "type": "pdf",
                "chunks": chunk_text(pdf_text, 400),
                "last_crawled": timestamp
            }
        )
        return

    # ---------- HTML ----------
    text, headings, links, forms = clean_html(r.text)

    payload = {
        "url": url,
        "type": "html",
        "headings": headings,
        "chunks": chunk_text(text),
        "forms": forms,
        "last_crawled": timestamp,
        "outbound_links": []
    }

    upload_payload(f"pages/{path_key}.json", payload)

    for link in links:
        absolute = urljoin(url, link)
        if is_internal(absolute):
            crawl(absolute)
        else:
            payload["outbound_links"].append(absolute)

    upload_payload(f"pages/{path_key}.json", payload)


# ======================
# CLOUD RUN JOB ENTRY
# ======================

@app.route("/", methods=["GET"])
def run_indexer():
    crawl(ROOT_URL)
    return f"Indexing completed. Pages indexed: {len(visited)}", 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
