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
BASE_DOMAIN = "https://www.midcindia.org"

SEED_URLS = [
    "https://www.midcindia.org/",
    "https://www.midcindia.org/en/investors/",
    "https://www.midcindia.org/en/customers/",
    "https://www.midcindia.org/en/contact/"
]

MAX_PAGES = 120  # safety cap

# ======================
# HELPERS
# ======================

def clean_html(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")

    for tag in soup([
        "script", "style", "nav", "footer", "header", "aside"
    ]):
        tag.decompose()

    return " ".join(soup.get_text(separator=" ").split())


def chunk_text(text: str, size=450):
    words = text.split()
    return [
        " ".join(words[i:i + size])
        for i in range(0, len(words), size)
        if len(words[i:i + size]) > 30
    ]


def extract_pdf_text(url: str):
    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        reader = PdfReader(io.BytesIO(r.content))
        return "\n".join(
            page.extract_text() or "" for page in reader.pages
        )
    except Exception:
        return ""


def upload(section, payload):
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(f"{section}/content.json")
    blob.upload_from_string(
        json.dumps(payload, indent=2, ensure_ascii=False),
        content_type="application/json"
    )


def is_internal(url):
    return url.startswith(BASE_DOMAIN)


def normalize(url):
    return url.split("#")[0].rstrip("/")


# ======================
# MAIN CRAWLER
# ======================

@app.route("/", methods=["GET"])
def run_indexer():
    visited = set()
    queue = list(SEED_URLS)
    timestamp = datetime.datetime.utcnow().isoformat()
    count = 0

    while queue and count < MAX_PAGES:
        url = normalize(queue.pop(0))
        if url in visited:
            continue

        visited.add(url)
        count += 1

        try:
            r = requests.get(url, timeout=20)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "html.parser")

            text = clean_html(r.text)
            chunks = chunk_text(text)

            links = []
            pdfs = []
            forms = []

            # Links
            for a in soup.find_all("a", href=True):
                link = normalize(urljoin(url, a["href"]))
                if link.endswith(".pdf"):
                    pdfs.append(link)
                elif is_internal(link):
                    queue.append(link)
                else:
                    links.append({
                        "title": a.get_text(strip=True) or "External Link",
                        "url": link
                    })

            # Forms
            for f in soup.find_all("form"):
                fields = []
                for inp in f.find_all(["input", "select", "textarea"]):
                    fields.append({
                        "name": inp.get("name"),
                        "type": inp.get("type", "text"),
                        "required": inp.has_attr("required")
                    })

                forms.append({
                    "action": urljoin(url, f.get("action", "")),
                    "method": f.get("method", "GET").upper(),
                    "fields": fields
                })

            payload = {
                "section": urlparse(url).path.strip("/").replace("/", "-") or "home",
                "content_type": "html",
                "source_url": url,
                "last_updated": timestamp,
                "chunks": chunks,
                "related_links": links,
                "forms": forms
            }

            upload(payload["section"], payload)

            # PDFs
            for pdf in pdfs:
                pdf_text = extract_pdf_text(pdf)
                if pdf_text.strip():
                    upload(
                        pdf.split("/")[-1].replace(".pdf", ""),
                        {
                            "section": pdf,
                            "content_type": "pdf",
                            "source_url": pdf,
                            "last_updated": timestamp,
                            "chunks": chunk_text(pdf_text, 400)
                        }
                    )

        except Exception:
            continue

    return f"Indexed {count} pages successfully", 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
