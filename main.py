import requests
import json
import datetime
from bs4 import BeautifulSoup
from google.cloud import storage

BUCKET_NAME = "midc-chatbot-content"

URLS = {
    "home": "https://www.midcindia.org/",
    "about-maharashtra": "https://www.midcindia.org/en/about-maharashtra/",
    "about-midc": "https://www.midcindia.org/en/about-midc/",
    "departments-of-midc": "https://www.midcindia.org/en/about-midc/departments-of-midc/",
    "faq": "https://www.midcindia.org/en/faqs/",
    "industrial-areas": "https://cmsportal.midcindia.org/",
    "investors": "https://www.midcindia.org/en/investors/",
    "milaap": "https://milaap.midcindia.org/",
    "customers": "https://www.midcindia.org/en/customers/",
    "country-desk": "https://www.midcindia.org/en/country-desk/",
    "focus-sectors": "https://www.midcindia.org/en/focus-sectors/",
    "recruitment": "https://recruitment.midcindia.org/default_2023.aspx",
    "rts-act": "https://www.midcindia.org/en/rts/",
    "contact": "https://www.midcindia.org/en/contact/",
    "important-notice": "https://www.midcindia.org/en/important-notice/",
    "right-to-public-service-act": "https://www.midcindia.org/wp-content/uploads/2024/07/Maharashtra_Right_to_public_services_Act_2015.pdf",
    "rts-gazzette": "https://www.midcindia.org/wp-content/uploads/2024/07/RTS_Rules_Gazette.pdf",
    "list-of-services-under-rts-act": "https://www.midcindia.org/wp-content/uploads/2025/09/RTS_MergedGRs_compressed-combined-12092025.pdf"
}

def clean_html(html):
    soup = BeautifulSoup(html, "html.parser")

    for tag in soup(["script", "style", "nav", "footer", "header"]):
        tag.decompose()

    return soup.get_text(separator="\n")

def chunk_text(text, chunk_size=500):
    words = text.split()
    chunks = []
    for i in range(0, len(words), chunk_size):
        chunks.append(" ".join(words[i:i+chunk_size]))
    return chunks

def upload_json(section, data):
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(f"{section}/content.json")
    blob.upload_from_string(
        json.dumps(data, indent=2),
        content_type="application/json"
    )

def run_indexer(request):
    for section, url in URLS.items():
        resp = requests.get(url, timeout=20)
        clean_text = clean_html(resp.text)
        chunks = chunk_text(clean_text)

        payload = {
            "section": section,
            "source_url": url,
            "last_updated": datetime.datetime.utcnow().isoformat(),
            "chunks": chunks
        }

        upload_json(section, payload)

    return ("Indexing completed", 200)