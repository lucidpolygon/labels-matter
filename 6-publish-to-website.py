import os
import re
import json
import time
import signal
import mimetypes
import requests
from urllib.parse import urljoin
from typing import Dict, List, Optional, Tuple, Any
from dotenv import load_dotenv

load_dotenv()

# ----------------------------
# Config (ENV)
# ----------------------------
AIRTABLE_TOKEN = os.getenv("AIRTABLE_TOKEN")

AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID", "appXSrKtsX3ywefGu")
AIRTABLE_CONTENT_MASTER_TABLE = os.environ["AIRTABLE_CONTENT_MASTER_TABLE"]

AIRTABLE_APPROVE_FIELD = "Approved"
AIRTABLE_PUBLISH_FIELD = "Published"
AIRTABLE_ERROR_FIELD = "Publishing Error"
AIRTABLE_WP_LINK_FIELD = "WP Link"
AIRTABLE_PROP65_FIELD = "Prop65 Notice"
AIRTABLE_LEXIS_FIELD = "Lexis Case"

# Content master fields
F_CONTENT_TITLE = "Title"
F_CONTENT_CATEGORY= "Category"
F_CONTENT_SUMMARY = "Summary"
F_CONTENT_FULL = "Full Article"
F_CONTENT_TAGS = "Tags"
F_CONTENT_FEATURED_IMAGE = "Featured Image"

# Filters
MAX_RECORDS = int(os.getenv("MAX_RECORDS", "1"))
  
# ----------------------------
# Airtable table URLs
# ----------------------------
AIRTABLE_URL = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_CONTENT_MASTER_TABLE}"

HEADERS = {
    "Authorization": f"Bearer {AIRTABLE_TOKEN}",
    "Content-Type": "application/json",
}

REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "60"))
PUBLISH_AS = os.getenv("WP_POST_STATUS", "publish")  # publish|draft



# ----------------------------
# WordPress
# ----------------------------
# 
WP_BASE_URL = os.getenv("WP_BASE_URL")
WP_USER = os.getenv("WP_USER")
WP_APP_PASSWORD = os.getenv("WP_APP_PASSWORD")
WP_AUTH = (WP_USER, WP_APP_PASSWORD)

# ----------------------------
# Validation
# ----------------------------

if not AIRTABLE_TOKEN:
    raise RuntimeError("Missing AIRTABLE_TOKEN")
if not AIRTABLE_BASE_ID or not AIRTABLE_CONTENT_MASTER_TABLE:
    raise RuntimeError("Missing AIRTABLE_BASE_ID / AIRTABLE_CONTENT_MASTER_TABLE")
if not WP_BASE_URL or not WP_USER or not WP_APP_PASSWORD:
    raise RuntimeError("Missing WP_BASE_URL / WP_USER / WP_APP_PASSWORD")  

# ----------------------------
# Helpers
# ----------------------------
def _timeout_handler(signum, frame):
    raise TimeoutError("Global scrape timeout hit")

def _sleep_backoff(attempt: int):
    time.sleep(min(2 ** attempt, 20))

def download_attachment(url: str) -> bytes:
    r = requests.get(url, timeout=180)
    r.raise_for_status()
    return r.content

def has_value(v: Any) -> bool:
    if v is None:
        return False
    if isinstance(v, str):
        return v.strip() != ""
    if isinstance(v, list):
        return len(v) > 0
    if isinstance(v, dict):
        return len(v.keys()) > 0
    return True

# ----------------------------
# Airtable helpers
# ----------------------------

def airtable_list_records() -> List[Dict]:
    records = []
    offset = None

    filter_by_formula = (
        f"AND("
        f"{{{AIRTABLE_APPROVE_FIELD}}}='Yes',"
        f"OR({{{AIRTABLE_PUBLISH_FIELD}}}=BLANK(),{{{AIRTABLE_PUBLISH_FIELD}}}!='Yes')"
        f")"
    )
    
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
# WordPress
# ----------------------------

WP_API_BASE = urljoin(WP_BASE_URL.rstrip("/") + "/", "wp-json/wp/v2/")
WP_MEDIA_URL = urljoin(WP_API_BASE, "media")
WP_POSTS_URL = urljoin(WP_API_BASE, "posts")
WP_CATEGORIES_URL = urljoin(WP_API_BASE, "categories")
WP_TAGS_URL = urljoin(WP_API_BASE, "tags")

def _to_str_list(v: Any) -> List[str]:
    if v is None:
        return []
    if isinstance(v, str):
        parts = [p.strip() for p in v.split(",")]
        return [p for p in parts if p]
    if isinstance(v, list):
        out = []
        for x in v:
            if isinstance(x, str):
                out.extend([p.strip() for p in x.split(",") if p.strip()])
        return out
    return []

def sanitize_term_name(name: str, max_len: int = 100) -> str:
    name = (name or "").strip()
    # remove control chars
    name = re.sub(r"[\x00-\x1F\x7F]", "", name)
    # collapse whitespace
    name = re.sub(r"\s+", " ", name)
    # hard cap
    return name[:max_len].strip()

def wp_request(method: str, url: str, **kwargs) -> requests.Response:
    last_status = None
    last_text = None

    for attempt in range(4):
        r = requests.request(
            method,
            url,
            auth=WP_AUTH,
            timeout=REQUEST_TIMEOUT,
            headers={"User-Agent": "RenderCron/1.0", **(kwargs.pop("headers", {}) or {})},
            **kwargs
        )

        last_status = r.status_code
        last_text = (r.text or "")[:500]

        # Retry only 5xx
        if r.status_code >= 500:
            _sleep_backoff(attempt)
            continue

        return r

    raise RuntimeError(f"WP request failed after retries: {last_status} {last_text}")

def wp_get_or_create_term(kind: str, name: str) -> Optional[int]:
    """
    kind: 'category' or 'tag'
    returns term id
    """
    name = (name or "").strip()
    name = sanitize_term_name(name)
    name = name if name.isupper() else name.title()
    if not name:
        return None

    if kind == "category":
        list_url = WP_CATEGORIES_URL
    elif kind == "tag":
        list_url = WP_TAGS_URL
    else:
        raise ValueError("kind must be category|tag")

    # Search existing
    r = wp_request("GET", list_url, params={"search": name, "per_page": 100})
    if not r.ok:
        raise RuntimeError(f"WP term lookup failed: {r.status_code} {r.text}")
    items = r.json() or []
    for it in items:
        if str(it.get("name", "")).strip().lower() == name.lower():
            return int(it["id"])

    # Create
    r = wp_request("POST", list_url, json={"name": name})
    if not r.ok:
        raise RuntimeError(f"WP term create failed: {r.status_code} {r.text}")
    return int(r.json()["id"])

def wp_upload_featured_image(image_bytes: bytes, filename: str) -> int:
    content_type, _ = mimetypes.guess_type(filename)
    if not content_type:
        content_type = "image/jpeg"

    headers = {
        "Content-Disposition": f'attachment; filename="{filename}"',
        "Content-Type": content_type,
    }

    r = wp_request("POST", WP_MEDIA_URL, headers=headers, data=image_bytes)
    if not r.ok:
        raise RuntimeError(f"WP media upload failed: {r.status_code} {r.text}")
    
    media_id = int(r.json()["id"])
    
    if alt_text:
        wp_request(
            "POST",
            f"{WP_MEDIA_URL}/{media_id}",
            json={"alt_text": alt_text}
        )
    
    return int(r.json()["id"])

def wp_create_post(title: str, content_html: str, excerpt: str,
                   category_ids: List[int], tag_ids: List[int],
                   featured_media_id: Optional[int]) -> Tuple[int, str]:
    payload: Dict[str, Any] = {
        "title": title,
        "content": content_html,
        "excerpt": excerpt or "",
        "status": PUBLISH_AS,
    }
    if category_ids:
        payload["categories"] = category_ids
    if tag_ids:
        payload["tags"] = tag_ids
    if featured_media_id:
        payload["featured_media"] = featured_media_id

    r = wp_request("POST", WP_POSTS_URL, json=payload)
    if not r.ok:
        raise RuntimeError(f"WP post create failed: {r.status_code} {r.text}")
    j = r.json()
    return int(j["id"]), str(j.get("link") or "")

# ----------------------------
# Main pipeline
# ----------------------------
def process_record(record: Dict) -> None:
    record_id = record["id"]
    fields = record.get("fields", {})

    title = str(fields.get(F_CONTENT_TITLE) or "").strip()
    summary = str(fields.get(F_CONTENT_SUMMARY) or "").strip()
    full = str(fields.get(F_CONTENT_FULL) or "").strip()

    if not title or not full:
        raise RuntimeError("Missing Title or Full Article")

    categories = _to_str_list(fields.get(F_CONTENT_CATEGORY))
    tags = _to_str_list(fields.get(F_CONTENT_TAGS))

    featured_media_id: Optional[int] = None
    attachments = fields.get(F_CONTENT_FEATURED_IMAGE) or []
    if isinstance(attachments, list) and attachments:
        att = attachments[0] or {}
        att_url = att.get("url")
        att_filename = att.get("filename") or "featured.jpg"
        if att_url:
            img_bytes = download_attachment(att_url)
            featured_media_id = wp_upload_featured_image(img_bytes, att_filename,alt_text=title)    

    # Source-driven category (from Airtable linked fields)
    source_category = None
    if has_value(fields.get(AIRTABLE_PROP65_FIELD)):
        source_category = "Prop65 Notice"
    elif has_value(fields.get(AIRTABLE_LEXIS_FIELD)):
        source_category = "Public Complaint"

    if source_category:
        categories.append(source_category)

    # Resolve terms
    category_ids: List[int] = []
    for c in categories[:5]:
        term_id = wp_get_or_create_term("category", c)
        if term_id and term_id not in category_ids:
            category_ids.append(term_id)

    
    tag_ids: List[int] = []
    for t in tags[:20]:
        term_id = wp_get_or_create_term("tag", t)
        if term_id:
            tag_ids.append(term_id)

    # Create WP post
    post_id, post_link = wp_create_post(
        title=title,
        content_html=full,
        excerpt=summary,
        category_ids=category_ids,
        tag_ids=tag_ids,
        featured_media_id=featured_media_id,
    )

    # Mark Airtable as published
    airtable_update_record(record_id, {
        AIRTABLE_PUBLISH_FIELD: "Yes",
        AIRTABLE_ERROR_FIELD: "",
        AIRTABLE_WP_LINK_FIELD: post_link,
    })

def main():

    print("stage: start", flush=True)
    signal.signal(signal.SIGALRM, _timeout_handler)
    signal.alarm(15 * 60)

    records = airtable_list_records()
    print(f"Found {len(records)} record(s) to process")

    for i, record in enumerate(records, 1):
        record_id = record["id"]
        print(f"[{i}/{len(records)}] Processing {record_id}")
        try:
            process_record(record)
            print(f"OK {record_id}", flush=True)
        except Exception as e:
            print(f"ERROR {record_id}: {e}")
            if AIRTABLE_ERROR_FIELD:
                try:
                    airtable_update_record(record_id, {
                        AIRTABLE_ERROR_FIELD: f"error: {str(e)[:200]}",
                        AIRTABLE_PUBLISH_FIELD: "No"
                        })        
                except Exception:
                    pass

if __name__ == "__main__":
    main()
