import os
import requests
import json
import datetime
import io

from flask import Flask
from bs4 import BeautifulSoup
from google.cloud import storage
from pypdf import PdfReader

app = Flask(__name__)

# ======================
# CONFIGURATION
# ======================

BUCKET_NAME = "midc-general-chatbot-bucket-web-data"

DOCUMENT_AI_PROCESSOR_ID = os.getenv("DOCUMENT_AI_PROCESSOR_ID")
DOCUMENT_AI_LOCATION = "us"  # MUST match processor region

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
# HELPER FUNCTIONS
# ======================

def clean_html(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "nav", "footer", "header", "aside", "form"]):
        tag.decompose()
    text = soup.get_text(separator=" ")
    return " ".join(text.split())


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


# ======================
# DOCUMENT AI (LAZY)
# ======================

def extract_pdf_text_with_ocr(pdf_bytes: bytes) -> str:
    """
    Uses Document AI OCR for scanned PDFs (English + Marathi).
    This is lazy-loaded to avoid Cloud Run startup failure.
    """
    from google.cloud import documentai_v1 as documentai

    if not DOCUMENT_AI_PROCESSOR_ID:
        raise RuntimeError("DOCUMENT_AI_PROCESSOR_ID environment variable not set")

    project_id = os.getenv("GOOGLE_CLOUD_PROJECT")
    client = documentai.DocumentProcessorServiceClient()

    processor_name = (
        f"projects/{project_id}/locations/{DOCUMENT_AI_LOCATION}"
        f"/processors/{DOCUMENT_AI_PROCESSOR_ID}"
    )

    raw_document = documentai.RawDocument(
        content=pdf_bytes,
        mime_type="application/pdf"
    )

    request = documentai.ProcessRequest(
        name=processor_name,
        raw_document=raw_document
    )

    result = client.process_document(request=request)
    document = result.document

    extracted_text = []
    for page in document.pages:
        for paragraph in page.paragraphs:
            text = "".join(
                document.text[segment.start_index:segment.end_index]
                for segment in paragraph.layout.text_anchor.text_segments
            )
            extracted_text.append(text)

    return "\n".join(extracted_text)


def extract_pdf_text(url: str) -> str:
    response = requests.get(url, timeout=30)
    response.raise_for_status()

    # First try native text extraction
    reader = PdfReader(io.BytesIO(response.content))
    native_text = ""

    for page in reader.pages:
        text = page.extract_text()
        if text:
            native_text += text + "\n"

    # If native text exists, use it
    if len(native_text.strip()) > 200:
        return native_text

    # Otherwise fallback to OCR
    return extract_pdf_text_with_ocr(response.content)


# ======================
# CLOUD RUN ENTRYPOINT
# ======================

@app.route("/", methods=["GET"])
def run_indexer():
    timestamp = datetime.datetime.utcnow().isoformat()

    # -------- HTML PAGES --------
    for section, url in HTML_URLS.items():
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()

            cleaned_text = clean_html(response.text)
            chunks = chunk_text(cleaned_text)

            payload = {
                "section": section,
                "content_type": "html",
                "source_url": url,
                "last_updated": timestamp,
                "chunks": chunks
            }

            upload_to_gcs(section, payload)

        except Exception as e:
            print(f"[ERROR][HTML] {section}: {str(e)}")

    # -------- PDF DOCUMENTS --------
    for section, url in PDF_URLS.items():
        try:
            pdf_text = extract_pdf_text(url)
            chunks = chunk_text(pdf_text, chunk_size=400)

            payload = {
                "section": section,
                "content_type": "pdf",
                "source_url": url,
                "last_updated": timestamp,
                "chunks": chunks
            }

            upload_to_gcs(section, payload)

        except Exception as e:
            print(f"[ERROR][PDF] {section}: {str(e)}")

    return "MIDC content indexing completed successfully", 200


# ======================
# MAIN
# ======================

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
