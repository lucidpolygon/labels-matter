# This one does not use OCR we are trying to generate content directly from the PDF using OpenAI
import os
import re
import json
import time
import boto3
import signal
import base64
import tempfile
import requests
from openai import OpenAI
from typing import Dict, List
from dotenv import load_dotenv
from botocore.config import Config

load_dotenv()

# ----------------------------
# Config (ENV)
# ----------------------------
AIRTABLE_TOKEN = os.getenv("AIRTABLE_TOKEN")

AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID", "appXSrKtsX3ywefGu")
AIRTABLE_PROP65_TABLE = os.getenv("AIRTABLE_PROP65_TABLE")
AIRTABLE_CONTENT_MASTER_TABLE = os.environ["AIRTABLE_CONTENT_MASTER_TABLE"]
AIRTABLE_PROMPTS_TABLE = os.environ["AIRTABLE_PROMPTS_TABLE"]

AIRTABLE_ATTACHMENT_FIELD = "Notice File"
AIRTABLE_STATUS_FIELD = "Processed"
AIRTABLE_ERROR_FIELD = "Processing Error"

# Content master fields
F_CONTENT_TITLE = "Title"
F_CONTENT_IMAGE_PROMPT = "Image Prompt"
F_CONTENT_CATEGORY= "Category"
F_CONTENT_SUMMARY = "Summary"
F_CONTENT_FULL = "Full Article"
F_CONTENT_TAGS = "Tags"
F_CONTENT_SUITS_US = "Suits us"
F_CONTENT_FEATURED_IMAGE = "Featured Image"
F_CONTENT_PROP65_LINK = "Prop65 Notice"

R2_ACCOUNT_ID = os.environ["R2_ACCOUNT_ID"]
R2_ACCESS_KEY_ID = os.environ["R2_ACCESS_KEY_ID"]
R2_SECRET_ACCESS_KEY = os.environ["R2_SECRET_ACCESS_KEY"]
R2_BUCKET = os.environ["R2_BUCKET"]
R2_PUBLIC_BASE_URL = os.getenv("R2_PUBLIC_BASE_URL", "").rstrip("/")  # optional
_R2 = None

# Prompt table fields
F_PROMPT_TITLE="Title"
F_PROMPT_PROMPT="Prompt"
F_PROMPT_INSTRUCTIONS="Instructions"
F_PROMPT_OUTPUT_FORMAT="Output Format"

# Filters
MAX_RECORDS = int(os.getenv("MAX_RECORDS", "1"))

if not AIRTABLE_TOKEN:
    raise RuntimeError("Missing AIRTABLE_TOKEN")
if not AIRTABLE_BASE_ID or not AIRTABLE_PROP65_TABLE:
    raise RuntimeError("Missing AIRTABLE_BASE_ID / AIRTABLE_PROP65_TABLE")

# ----------------------------
# Airtable table URLs
# ----------------------------
AIRTABLE_URL = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_PROP65_TABLE}"
AIRTABLE_CONTENT_URL = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_CONTENT_MASTER_TABLE}"
AIRTABLE_PROMPTS_URL = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_PROMPTS_TABLE}"

HEADERS = {
    "Authorization": f"Bearer {AIRTABLE_TOKEN}",
    "Content-Type": "application/json",
}

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_MODEL_TEXT = os.getenv("OPENAI_MODEL_TEXT", "gpt-5")
OPENAI_MODEL_IMAGE = os.getenv("OPENAI_MODEL_IMAGE", "gpt-image-1.5")

# ----------------------------
# R2 helper
# ----------------------------
def _timeout_handler(signum, frame):
    raise TimeoutError("Global scrape timeout hit")

def r2_client():
    global _R2
    if _R2 is None:
        _R2 = boto3.client(
            "s3",
            endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
            aws_access_key_id=R2_ACCESS_KEY_ID,
            aws_secret_access_key=R2_SECRET_ACCESS_KEY,
            region_name="auto",
            config=Config(signature_version="s3v4"),
        )
    return _R2

def upload_bytes_to_r2_and_get_url(key: str, data: bytes, content_type: str) -> str:
    s3 = r2_client()
    s3.put_object(Bucket=R2_BUCKET, Key=key, Body=data, ContentType=content_type)

    return s3.generate_presigned_url(
        "get_object",
        Params={"Bucket": R2_BUCKET, "Key": key},
        ExpiresIn=60 * 60 * 24,
    )

def download_attachment(url: str) -> bytes:
    with requests.get(url, stream=True, timeout=180) as r:
        r.raise_for_status()
        return r.content
# ----------------------------
# Airtable helpers
# ----------------------------
def create_airtable_record(url: str, fields: dict):
    r = requests.post(url, headers=airtable_headers(), json={"fields": fields}, timeout=60)
    r.raise_for_status()
    return r.json()

def airtable_headers():
    return {
        "Authorization": f"Bearer {AIRTABLE_TOKEN}",
        "Content-Type": "application/json",
    }

def airtable_list_records() -> List[Dict]:
    records = []
    offset = None

    formula_parts = [
        f"NOT({{{AIRTABLE_ATTACHMENT_FIELD}}} = BLANK())",
        f"OR({{{AIRTABLE_STATUS_FIELD}}} = BLANK(), {{{AIRTABLE_STATUS_FIELD}}} != 'Yes')"
    ]
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
    if not r.ok:
        raise RuntimeError(f"Airtable {r.status_code}: {r.text}")
    return

# ----------------------------
# Open AI helpers
# ----------------------------
def fetch_prompt_by_title(title: str) -> dict:
    # Airtable formula: {Title} = 'pop65_article_create'
    safe = title.replace("'", "\\'")
    formula = f"{{{F_PROMPT_TITLE}}} = '{safe}'"
    params = {"pageSize": 1, "filterByFormula": formula}
    r = requests.get(AIRTABLE_PROMPTS_URL, headers=airtable_headers(), params=params, timeout=30)
    r.raise_for_status()
    recs = r.json().get("records", [])
    if not recs:
        raise RuntimeError(f"No prompt found in Airtable for Title='{title}'")
    fields = recs[0].get("fields", {})
    return {
        "title": fields.get(F_PROMPT_TITLE, title),
        "prompt": fields.get(F_PROMPT_PROMPT, "") or "",
        "instructions": fields.get(F_PROMPT_INSTRUCTIONS, "") or "",
        "output_format": fields.get(F_PROMPT_OUTPUT_FORMAT, "") or "",
    }

def summarize_complaint_pdf(pdf_bytes: bytes, prompt_tpl: dict) -> str:
    client = OpenAI()

    # The Files API needs a file-like object; easiest is a temp file
    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
            tmp.write(pdf_bytes)
            tmp_path = tmp.name

        with open(tmp_path, "rb") as fh:
            uploaded = client.files.create(
                file=fh,
                # "user_data" is the canonical purpose for model-input files in current docs/guides.
                # If you run into purpose-related issues, try "assistants" as a fallback.
                purpose="user_data",
            )

        user_text = (
            f'{prompt_tpl.get("prompt","")}\n\n'
            f'--- OUTPUT FORMAT (JSON) ---\n{prompt_tpl.get("output_format","")}\n\n'
            "Return ONLY valid JSON."
        )

        resp = client.responses.create(
            model=OPENAI_MODEL_TEXT,
            input=[
                {
                    "role": "system",
                    "content": [
                        {"type": "input_text", "text": prompt_tpl.get("instructions", "")}
                    ],
                },
                {
                    "role": "user",
                    "content": [
                        {"type": "input_text", "text": user_text},
                        {"type": "input_file", "file_id": uploaded.id},
                    ],
                },
            ],
        )


        return resp.output_text.strip()

    finally:
        if tmp_path:
            try:
                os.remove(tmp_path)
            except Exception:
                pass

def openai_generate_image_bytes(image_prompt: str) -> bytes:
    client = OpenAI()

    result = client.images.generate(
        model=OPENAI_MODEL_IMAGE,
        prompt=image_prompt,
        size="1536x1024",
    )

    b64 = result.data[0].b64_json
    return base64.b64decode(b64)

# ----------------------------
# Main
# ----------------------------
def process_record(rec: Dict, prompt_tpl: dict) -> None:

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

    ai_summary = summarize_complaint_pdf(pdf_bytes, prompt_tpl)

    ai_json = json.loads(ai_summary)

    image_prompt = ai_json.get("image_prompt", "")
    featured_image_attachment = []
    if image_prompt:
        png_bytes = openai_generate_image_bytes(image_prompt)

        safe_ag = re.sub(r"[^A-Za-z0-9._-]+", "_", record_id)
        filename = f"{safe_ag}_featured.png"
        key = f"GPT-images/prop65/{filename}"

        img_url = upload_bytes_to_r2_and_get_url(
            key=key,
            data=png_bytes,
            content_type="image/png",
        )

        featured_image_attachment = [{"url": img_url, "filename": filename}]
    print (f"stage: image generated and uploaded: {featured_image_attachment}", flush=True)
    
    title = ai_json.get("title", "")
    summary = ai_json.get("summary", "")
    full_article = ai_json.get("full_article", "")
    case_type = ai_json.get("case_type", "Other")
    suits_us = ai_json.get("suits_us", "No")
    tags = ai_json.get("tags", [])
    tags_str = ", ".join(map(str, tags)) if isinstance(tags, list) else str(tags)

    fields_to_create = {
        F_CONTENT_TITLE: title,
        F_CONTENT_SUMMARY: summary,
        F_CONTENT_FULL: full_article,
        F_CONTENT_CATEGORY: case_type,
        F_CONTENT_SUITS_US: suits_us,
        F_CONTENT_TAGS: tags_str,
        F_CONTENT_IMAGE_PROMPT: image_prompt,
        F_CONTENT_PROP65_LINK: [record_id],
        F_CONTENT_FEATURED_IMAGE: featured_image_attachment,
    }

    created = create_airtable_record(AIRTABLE_CONTENT_URL, fields_to_create)

    if AIRTABLE_STATUS_FIELD:
        airtable_update_record(record_id, {AIRTABLE_STATUS_FIELD: "Yes", AIRTABLE_ERROR_FIELD: ""})

def main():

    print("stage: start", flush=True)
    
    signal.signal(signal.SIGALRM, _timeout_handler)
    signal.alarm(15 * 60)

    prompt_tpl = fetch_prompt_by_title("pop65_article_create")
    print("stage: prompt loaded", flush=True)

    records = airtable_list_records()
    print(f"Found {len(records)} record(s) to process")

    for i, rec in enumerate(records, 1):
        record_id = rec["id"]
        print(f"[{i}/{len(records)}] Processing {record_id}")
        try:
            process_record(rec, prompt_tpl)
        except Exception as e:
            print(f"ERROR {record_id}: {e}")
            if AIRTABLE_ERROR_FIELD:
                try:
                    airtable_update_record(record_id, {
                        AIRTABLE_ERROR_FIELD: f"error: {str(e)[:200]}",
                        AIRTABLE_STATUS_FIELD: "No"
                        })        
                except Exception:
                    pass

if __name__ == "__main__":
    main()
