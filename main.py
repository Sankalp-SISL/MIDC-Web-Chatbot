import os
import requests
import json
import datetime
import io

from flask import Flask, jsonify
from bs4 import BeautifulSoup
from google.cloud import storage
from google.cloud import documentai_v1 as documentai
from pypdf import PdfReader

# ======================
# APP INIT (FIRST!)
# ======================

app = Flask(__name__)

# ======================
# CONFIG
# ======================

PROJECT_ID = os.environ.get("GOOGLE_CLOUD_PROJECT") or os.environ.get("GCP_PROJECT")
LOCATION = "us"  # must match processor
PROCESSOR_ID = os.environ.get("DOCUMENT_AI_PROCESSOR_ID")

BUCKET_NAME = "midc-general-chatbot-bucket-web-data"

HTML_URLS = {
    "home": "https://www.midcindia.org/",
    "about-maharashtra": "https://www.midcindia.org/en/about-maharashtra/",
    "about-midc": "https://www.midcindia.org/en/about-midc/",
    "departments-of-midc": "https://www.midcindia.org/en/about-midc/departments-of-midc/",
    "faq": "https://www.midcindia.org/en/faqs/",
    "investors": "https://www.midcindia.org/en/investors/",
    "customers": "https://www.midcindia.org/en/customers/",
    "country-desk": "https://www.midcindia.org/en/country-desk/",
    "focus-sectors": "https://www.midcindia.org/en/focus-sectors/",
    "contact": "https://www.midcindia.org/en/contact/",
    "important-notice": "https://www.midcindia.org/en/important-notice/"
}

PDF_URLS = {
    "right-to-public-service-act": "https://www.midcindia.org/wp-content/uploads/2024/07/Maharashtra_Right_to_public_services_Act_2015.pdf",
    "rts-gazette": "https://www.midcindia.org/wp-content/uploads/2024/07/RTS_Rules_Gazette.pdf",
    "list-of-services-under-rts-act": "https://www.midcindia.org/wp-content/uploads/2025/09/RTS_MergedGRs_compressed-combined-12092025.pdf"
}

# ======================
# HELPERS
# ======================

def clean_html(html):
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "nav", "footer", "header", "aside", "form"]):
        tag.decompose()
    return " ".join(soup.get_text(separator=" ").split())


def chunk_text(text, size=450):
    words = text.split()
    return [
        " ".join(words[i:i+size])
        for i in range(0, len(words), size)
        if len(words[i:i+size]) > 20
    ]


def upload_to_gcs(section, payload):
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(f"{section}/content.json")
    blob.upload_from_string(
        json.dumps(payload, ensure_ascii=False, indent=2),
        content_type="application/json"
    )


def extract_pdf_with_document_ai(url):
    response = requests.get(url, timeout=60)
    response.raise_for_status()

    client = documentai.DocumentProcessorServiceClient()
    name = client.processor_path(PROJECT_ID, LOCATION, PROCESSOR_ID)

    raw_document = documentai.RawDocument(
        content=response.content,
        mime_type="application/pdf"
    )

    request = documentai.ProcessRequest(
        name=name,
        raw_document=raw_document
    )

    result = client.process_document(request=request)
    return result.document.text


# ======================
# ROUTES
# ======================

@app.route("/", methods=["GET"])
def health():
    return "MIDC Indexer is running", 200


@app.route("/run-indexing", methods=["POST"])
def run_indexing():
    timestamp = datetime.datetime.utcnow().isoformat()

    # HTML
    for section, url in HTML_URLS.items():
        html = requests.get(url, timeout=30).text
        text = clean_html(html)
        upload_to_gcs(section, {
            "section": section,
            "content_type": "html",
            "source_url": url,
            "last_updated": timestamp,
            "chunks": chunk_text(text)
        })

    # PDFs (OCR)
    for section, url in PDF_URLS.items():
        text = extract_pdf_with_document_ai(url)
        upload_to_gcs(section, {
            "section": section,
            "content_type": "pdf",
            "source_url": url,
            "last_updated": timestamp,
            "chunks": chunk_text(text, 400)
        })

    return jsonify({"status": "Indexing completed"}), 200


# ======================
# START SERVER (LAST!)
# ======================

if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=int(os.environ.get("PORT", 8080))
    )

