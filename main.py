import os
import requests
import json
import datetime
import io

from flask import Flask
from bs4 import BeautifulSoup
from google.cloud import storage
from google.cloud import documentai_v1 as documentai
from pypdf import PdfReader

app = Flask(__name__)

# ======================================================
# CONFIGURATION
# ======================================================

PROJECT_ID = os.environ.get("GOOGLE_CLOUD_PROJECT")
LOCATION = "us"  # Document AI supports "us" or "eu"
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

# ======================================================
# HELPERS
# ======================================================

def clean_html(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "nav", "footer", "header", "aside", "form"]):
        tag.decompose()
    text = soup.get_text(separator=" ")
    return " ".join(text.split())


def extract_pdf_text_native(pdf_bytes: bytes) -> str:
    """Try normal text extraction first"""
    reader = PdfReader(io.BytesIO(pdf_bytes))
    text = ""
    for page in reader.pages:
        page_text = page.extract_text()
        if page_text:
            text += page_text + "\n"
    return text.strip()


def extract_pdf_text_document_ai(pdf_bytes: bytes) -> str:
    """OCR scanned PDFs using Document AI"""
    client = documentai.DocumentProcessorServiceClient()

    name = client.processor_path(
        PROJECT_ID,
        LOCATION,
        PROCESSOR_ID
    )

    request = documentai.ProcessRequest(
        name=name,
        raw_document=documentai.RawDocument(
            content=pdf_bytes,
            mime_type="application/pdf"
        )
    )

    result = client.process_document(request=request)
    return result.document.text


def extract_pdf_text(url: str) -> str:
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    pdf_bytes = response.content

    # 1️⃣ Try native extraction
    native_text = extract_pdf_text_native(pdf_bytes)

    # 2️⃣ Fallback to OCR if scanned
    if len(native_text) < 500:
        return extract_pdf_text_document_ai(pdf_bytes)

    return native_text


def chunk_text(text: str, chunk_size: int = 450):
    words = text.split()
    chunks = []
    for i in range(0, len(words), chunk_size):
        chunk = " ".join(words[i:i + chunk_size])
        if len(chunk.strip()) > 50:
            chunks.append(chunk)
    return chunks


def upload_to_gcs(section: str, payload: dict):
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(f"{section}/content.json")

    blob.upload_from_string(
        json.dumps(payload, indent=2, ensure_ascii=False),
        content_type="application/json"
    )

# ======================================================
# CLOUD RUN ENTRYPOINT
# ======================================================

@app.route("/", methods=["GET"])
def run_indexer():
    timestamp = datetime.datetime.utcnow().isoformat()

    # -------- HTML PAGES --------
    for section, url in HTML_URLS.items():
        try:
            r = requests.get(url, timeout=30)
            r.raise_for_status()

            cleaned_text = clean_html(r.text)
            chunks = chunk_text(cleaned_text)

            upload_to_gcs(section, {
                "section": section,
                "content_type": "html",
                "source_url": url,
                "last_updated": timestamp,
                "chunks": chunks
            })

        except Exception as e:
            print(f"[HTML ERROR] {section}: {e}")

    # -------- PDF DOCUMENTS (OCR ENABLED) --------
    for section, url in PDF_URLS.items():
        try:
            pdf_text = extract_pdf_text(url)
            chunks = chunk_text(pdf_text, chunk_size=400)

            upload_to_gcs(section, {
                "section": section,
                "content_type": "pdf",
                "source_url": url,
                "last_updated": timestamp,
                "chunks": chunks
            })

        except Exception as e:
            print(f"[PDF ERROR] {section}: {e}")

    return "MIDC content indexing completed successfully (Document AI OCR)", 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
