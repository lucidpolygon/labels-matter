import os
import json
import base64
import tempfile
import requests
from typing import List, Dict

from dotenv import load_dotenv

from google.cloud import documentai_v1 as documentai

load_dotenv()

# ----------------------------
# Config (ENV)
# ----------------------------
AIRTABLE_TOKEN = os.getenv("AIRTABLE_TOKEN")

AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID", "appXSrKtsX3ywefGu")
AIRTABLE_TABLE_NAME = os.getenv("AIRTABLE_TABLE_NAME", "tblWUi6wCQVFofac0")

AIRTABLE_ATTACHMENT_FIELD = os.getenv("AIRTABLE_ATTACHMENT_FIELD", "Notice File")
AIRTABLE_TEXT_FIELD = os.getenv("AIRTABLE_TEXT_FIELD", "OCR Text")
AIRTABLE_STATUS_FIELD = os.getenv("AIRTABLE_STATUS_FIELD", "OCR Status")

# Document AI
GCP_PROJECT_ID = os.getenv("GOOGLE_PROJECT_ID")
DOCUMENTAI_LOCATION = os.getenv("DOCUMENTAI_LOCATION", "us")  # e.g. "us" or "eu"
DOCUMENTAI_PROCESSOR_ID = os.getenv("GOOGLE_DOCUMENTAI_PROCESSOR_ID")

# Filters
ONLY_WHERE_NO_TEXT = int(os.getenv("ONLY_WHERE_NO_TEXT", "1"))
MAX_RECORDS = int(os.getenv("MAX_RECORDS", "1"))

if not AIRTABLE_TOKEN:
    raise RuntimeError("Missing AIRTABLE_TOKEN")
if not AIRTABLE_BASE_ID or not AIRTABLE_TABLE_NAME:
    raise RuntimeError("Missing AIRTABLE_BASE_ID / AIRTABLE_TABLE_NAME")
if not GCP_PROJECT_ID or not DOCUMENTAI_PROCESSOR_ID:
    raise RuntimeError("Missing GCP_PROJECT_ID (or GOOGLE_CLOUD_PROJECT) and DOCUMENTAI_PROCESSOR_ID")

AIRTABLE_URL = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_NAME}"
HEADERS = {
    "Authorization": f"Bearer {AIRTABLE_TOKEN}",
    "Content-Type": "application/json",
}


# ----------------------------
# Google creds helper
# ----------------------------
def extract_paragraphs(doc):
    text = doc.text
    out = []
    for page in doc.pages:
        for p in page.paragraphs:
            seg = p.layout.text_anchor.text_segments[0]
            out.append(text[seg.start_index:seg.end_index])
    return "\n\n".join(out)

def ensure_google_creds_file():
    """
    Render best practice: store base64 SA json in GCP_SA_JSON_B64.
    But if user put base64 into GOOGLE_APPLICATION_CREDENTIALS, support it:
      - If value is a path and exists -> OK
      - If value looks like JSON or base64 -> write temp file and set GOOGLE_APPLICATION_CREDENTIALS to that path
    """
    # Preferred
    sa_b64 = os.getenv("GCP_SA_JSON_B64")
    if sa_b64:
        sa_json = base64.b64decode(sa_b64).decode("utf-8")
        return _write_sa_json_to_temp(sa_json)

    gac = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if not gac:
        # allow ADC if running in GCP; on Render you typically need SA
        return None

    # If it's a file path and exists, great
    if os.path.exists(gac):
        return gac

    # If it's raw JSON
    if gac.strip().startswith("{") and '"type"' in gac:
        return _write_sa_json_to_temp(gac)

    # Otherwise treat as base64
    try:
        sa_json = base64.b64decode(gac).decode("utf-8")
        if sa_json.strip().startswith("{") and '"type"' in sa_json:
            return _write_sa_json_to_temp(sa_json)
    except Exception:
        pass

    raise RuntimeError(
        "GOOGLE_APPLICATION_CREDENTIALS must be a file path. "
        "If you're storing base64, put it in GCP_SA_JSON_B64 (preferred), "
        "or keep base64 in GOOGLE_APPLICATION_CREDENTIALS and this script will decode it — "
        "but your value didn't look like valid base64/json."
    )


def _write_sa_json_to_temp(sa_json_str: str) -> str:
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".json")
    tmp.write(sa_json_str.encode("utf-8"))
    tmp.close()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = tmp.name
    return tmp.name


# ----------------------------
# Airtable helpers
# ----------------------------
def airtable_list_records() -> List[Dict]:
    records = []
    offset = None

    formula_parts = []
    if ONLY_WHERE_NO_TEXT:
        formula_parts.append(f"OR({{{AIRTABLE_TEXT_FIELD}}} = '', {{{AIRTABLE_TEXT_FIELD}}} = BLANK())")
    formula_parts.append(f"NOT({{{AIRTABLE_ATTACHMENT_FIELD}}} = BLANK())")
    filter_by_formula = f"AND({', '.join(formula_parts)})"

    while True:
        params = {"pageSize": 100, "filterByFormula": filter_by_formula}
        if offset:
            params["offset"] = offset

        r = requests.get(AIRTABLE_URL, headers=HEADERS, params=params, timeout=60)
        r.raise_for_status()
        data = r.json()
        batch = data.get("records", [])
        records.extend(batch)

        if len(records) >= MAX_RECORDS:
            return records[:MAX_RECORDS]

        offset = data.get("offset")
        if not offset:
            return records


def airtable_update_record(record_id: str, fields: Dict) -> None:
    url = f"{AIRTABLE_URL}/{record_id}"
    payload = {"fields": fields}
    r = requests.patch(url, headers=HEADERS, data=json.dumps(payload), timeout=60)
    r.raise_for_status()


# ----------------------------
# Document AI OCR
# ----------------------------
def download_attachment(url: str) -> bytes:
    with requests.get(url, stream=True, timeout=180) as r:
        r.raise_for_status()
        return r.content


def run_documentai_ocr(pdf_bytes: bytes):
    client = documentai.DocumentProcessorServiceClient()
    name = client.processor_path(
        GCP_PROJECT_ID,
        DOCUMENTAI_LOCATION,
        DOCUMENTAI_PROCESSOR_ID
    )

    raw_document = documentai.RawDocument(
        content=pdf_bytes,
        mime_type="application/pdf"
    )

    result = client.process_document(
        request=documentai.ProcessRequest(
            name=name,
            raw_document=raw_document
        )
    )

    return result.document


# ----------------------------
# Main pipeline
# ----------------------------
def process_record(rec: Dict) -> None:
    record_id = rec["id"]
    fields = rec.get("fields", {})
    attachments = fields.get(AIRTABLE_ATTACHMENT_FIELD) or []
    if not attachments:
        return

    att = attachments[0]
    att_url = att.get("url")
    if not att_url:
        return

    pdf_bytes = download_attachment(att_url)

    doc = run_documentai_ocr(pdf_bytes)
    text = extract_paragraphs(doc)

    update = {AIRTABLE_TEXT_FIELD: text}
    if AIRTABLE_STATUS_FIELD:
        update[AIRTABLE_STATUS_FIELD] = "done"
    airtable_update_record(record_id, update)


def main():
    ensure_google_creds_file()

    records = airtable_list_records()
    print(f"Found {len(records)} record(s) to process")

    for i, rec in enumerate(records, 1):
        rid = rec["id"]
        print(f"[{i}/{len(records)}] Processing {rid}")
        try:
            process_record(rec)
        except Exception as e:
            print(f"ERROR {rid}: {e}")
            if AIRTABLE_STATUS_FIELD:
                try:
                    airtable_update_record(rid, {AIRTABLE_STATUS_FIELD: f"error: {str(e)[:200]}"})
                except Exception:
                    pass


if __name__ == "__main__":
    main()
