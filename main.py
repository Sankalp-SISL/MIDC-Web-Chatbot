import requests
import json
import datetime
from bs4 import BeautifulSoup
from google.cloud import storage
from flask import Flask

app = Flask(__name__)

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
    return soup.get_text(separator=" ")

def chunk_text(text, size=500):
    words = text.split()
    return [" ".join(words[i:i+size]) for i in range(0, len(words), size)]

@app.route("/")
def run_indexer():
    client = storage.Client()

    for section, url in URLS.items():
        html = requests.get(url, timeout=20).text
        clean = clean_html(html)
        chunks = chunk_text(clean)

        payload = {
            "section": section,
            "source_url": url,
            "last_updated": datetime.datetime.utcnow().isoformat(),
            "chunks": chunks
        }

        bucket = client.bucket(BUCKET_NAME)
        blob = bucket.blob(f"{section}/content.json")
        blob.upload_from_string(
            json.dumps(payload, indent=2),
            content_type="application/json"
        )

    return "Indexing completed", 200
