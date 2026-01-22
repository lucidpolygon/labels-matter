import os
import re
import json
import time
import boto3
import requests
import signal
from typing import Optional, Dict
from datetime import datetime, timedelta
from urllib.parse import urlsplit
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
from botocore.config import Config

load_dotenv()

HEADLESS = os.getenv("HEADLESS", "true").lower() == "true"

COURT_LINK  = os.getenv("LEXIS_URL")
LEXIS_ALERTS_URL = os.getenv("LEXIS_ALERTS_URL")
LEXIS_USER = os.getenv("LEXIS_USER")
LEXIS_PASS = os.getenv("LEXIS_PASS")

R2_ACCOUNT_ID = os.environ["R2_ACCOUNT_ID"]
R2_ACCESS_KEY_ID = os.environ["R2_ACCESS_KEY_ID"]
R2_SECRET_ACCESS_KEY = os.environ["R2_SECRET_ACCESS_KEY"]
R2_BUCKET = os.environ["R2_BUCKET"]
R2_PUBLIC_BASE_URL = os.getenv("R2_PUBLIC_BASE_URL", "").rstrip("/")
_R2 = None

ALERT_NAME = os.getenv("ALERT_NAME")
ALERT_FROM = os.getenv("ALERT_FROM")
ALERT_TO =os.getenv("ALERT_TO")
ALLOW_NATURE = {
    x.strip()
    for x in os.getenv("FILTER_BY_CASE_NATURE", "").split(",")
    if x.strip()
}

AIRTABLE_TOKEN = os.environ["AIRTABLE_TOKEN"]
AIRTABLE_BASE_ID = os.environ["AIRTABLE_BASE_ID"]
AIRTABLE_TABLE = os.environ["AIRTABLE_LEXIS_TABLE"]

AIRTABLE_URL = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE}"

# ---------- helpers ----------
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

def load_state_from_r2(key="state/lexis_state.json") -> Optional[Dict]:
    try:
        obj = r2_client().get_object(Bucket=R2_BUCKET, Key=key)
        return json.loads(obj["Body"].read().decode("utf-8"))
    except Exception:
        return None

def save_state_to_r2(state: dict, key="state/lexis_state.json"):
    r2_client().put_object(
        Bucket=R2_BUCKET,
        Key=key,
        Body=json.dumps(state).encode("utf-8"),
        ContentType="application/json",
    )

def fmt_date(d: datetime) -> str:
    return d.strftime("%b %d, %Y")

def chunked(lst, n=10):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

def login_lexis(page, username: str, password: str):
    page.wait_for_selector("#userid", timeout=60_000)
    page.fill("#userid", username)
    page.click("#signInSbmtBtn")

    page.wait_for_selector("#password", timeout=60_000)
    page.fill("#password", password)
    page.click("#next")

def is_logged_in(page) -> bool:
    return page.locator("#userid").count() == 0

def extract_results_from_table(page) -> list[dict]:
    # wait until at least one non-filter row exists OR table finished loading
    page.wait_for_selector("table[ln-table] tbody tr:not(.filter-row)", timeout=90_000)

    allow_all_nature = len(ALLOW_NATURE) == 0

    rows_data = page.evaluate("""
    () => {
      const rows = Array.from(document.querySelectorAll("table[ln-table] tbody tr:not(.filter-row)"));
      return rows.map(r => Array.from(r.querySelectorAll("td")).map(td => (td.innerText || "").trim()));
    }
    """)

    results = []
    for cols in rows_data:
        # expect at least 11 columns based on your indexing
        if len(cols) < 11:
            continue

        court       = cols[2]
        docket_no   = cols[3]
        defendant   = cols[4]
        case_name   = cols[5]
        nature_suit = " ".join(cols[6].split())
        cause       = cols[7]
        complaint   = " ".join(cols[8].split()).lower()
        date_hit    = cols[9]
        date_filed  = cols[10]

        if complaint.startswith("free") and (allow_all_nature or nature_suit in ALLOW_NATURE):
            results.append({
                "court": court,
                "docket_number": docket_no,
                "defendant": defendant,
                "case_name": case_name,
                "nature_of_suit": nature_suit,
                "cause": cause,
                "complaint": complaint,
                "date_hit": date_hit,
                "date_filed": date_filed,
                "key": f"{court}|{docket_no}",
            })

    return results

def send_rows_to_airtable(rows):
    """
    rows: list[dict] where keys are Airtable field names.
    """
    headers = {
        "Authorization": f"Bearer {AIRTABLE_TOKEN}",
        "Content-Type": "application/json",
    }

    created = 0
    for batch in chunked(rows, 10):
        # Airtable limit is 10 records/request
        payload = {"records": [{"fields": r} for r in batch]}
        resp = requests.post(AIRTABLE_URL, headers=headers, json=payload, timeout=30)

        if resp.status_code >= 300:
            raise RuntimeError(f"Airtable error {resp.status_code}: {resp.text}")

        created += len(resp.json().get("records", []))
        # be nice to rate limits (Render cron + Airtable free/team can be tight)
        time.sleep(0.25)

    return created

def has_next_page(page) -> bool:
    btn = page.locator("button.ln-pagination-next[aria-label='Next page']").first
    if btn.count() == 0:
        return False
    # matches your HTML snippets exactly
    return btn.get_attribute("disabled") is None

def wait_alert_not_blocking(page, timeout=180_000):
    # Wait until the loader isn't blocking pointer events (or doesn't exist / not visible)
    page.wait_for_function(
        """
        () => {
          const el = document.querySelector("alert-loadbox ln-loading, ln-loading.alertLoading, alert-loadbox");
          if (!el) return true;
          const r = el.getBoundingClientRect();
          const visible = r.width > 0 && r.height > 0 && getComputedStyle(el).visibility !== "hidden";
          if (!visible) return true;
          return getComputedStyle(el).pointerEvents === "none";
        }
        """,
        timeout=timeout,
    )

def click_next_page(page, timeout=180_000) -> bool:
    btn = page.locator("button.ln-pagination-next[aria-label='Next page']").first
    if btn.count() == 0:
        return False

    # Wait out Lexis loadbox before attempting the click
    wait_alert_not_blocking(page, timeout=timeout)

    # Re-check right before clicking
    if btn.get_attribute("disabled") is not None:
        return False

    try:
        btn.click(timeout=10_000)
    except Exception:
        # bypass "intercepts pointer events" (pointer interception doesn't block JS click)
        btn.evaluate("el => el.click()")

    # After click, Lexis loads again
    wait_alert_not_blocking(page, timeout=timeout)
    return True

def main():

    print("stage: start", flush=True)
    signal.signal(signal.SIGALRM, _timeout_handler)
    signal.alarm(15 * 60)

    today = datetime.now()
    yesterday = today - timedelta(days=1)

    if ALERT_FROM or ALERT_TO:
        date_from = ALERT_FROM or yesterday.strftime("%m/%d/%Y")
        date_to   = ALERT_TO   or yesterday.strftime("%m/%d/%Y")
    else:
        date_from = yesterday.strftime("%m/%d/%Y")
        date_to   = yesterday.strftime("%m/%d/%Y")

    print(date_from,date_to)
    playwright = sync_playwright().start()
    browser = None

    try:
        print("stage: launching chromium", flush=True)
        browser = playwright.chromium.launch(
            headless=HEADLESS,
            args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-popup-blocking", "--disable-features=IsolateOrigins,site-per-process"],
        ) 

        context_kwargs = dict(
            accept_downloads=True,
            viewport={"width": 1280, "height": 720},
        )           
        
        print("stage: chromium launched", flush=True)        

        state = load_state_from_r2()
        print("stage: state loaded", flush=True)

        if state:
            context = browser.new_context(storage_state=state, **context_kwargs)
        else:
            context = browser.new_context(**context_kwargs)

        page = context.new_page()

        # Render is slower. Increase your Playwright default timeout in Render. No need locally
        page.set_default_timeout(120_000)
        page.set_default_navigation_timeout(120_000)

        page.goto(COURT_LINK, wait_until="domcontentloaded", timeout=60_000)

        if not is_logged_in(page):
            print("Not Logged in");
            if not LEXIS_USER or not LEXIS_PASS:
                raise RuntimeError("Missing LEXIS_USER / LEXIS_PASS (set in .env or environment)")
            login_lexis(page, LEXIS_USER, LEXIS_PASS)
            page.wait_for_url(f"{COURT_LINK}*", timeout=120_000)            
            page.wait_for_function("() => !document.querySelector('#password')", timeout=120_000)
            save_state_to_r2(context.storage_state())

        print("Login Done")

        page.goto(LEXIS_ALERTS_URL, wait_until="domcontentloaded")
        page.wait_for_selector(f"a:has-text('{ALERT_NAME}')", timeout=60000)
        page.get_by_role("link", name=ALERT_NAME, exact=True).click()

        print("In the alert page")
        
        # ---- date filter ----
        from_input = page.locator('input[aria-label="from-date"]')
        to_input   = page.locator('input[aria-label="to-date"]')
        from_input.wait_for(state="visible", timeout=60_000)
        from_input.fill(date_from)
        to_input.fill(date_to)
        submit = page.locator("button.alert-btn-submit, button.ln-button.alert-btn-submit")
        submit.click()

        print("Filtered date")

         # ---- First Participant Only ----
        party_only = page.locator("#ln-checkbox-0-input")
        party_only.wait_for(state="visible", timeout=60_000)
        party_only.set_checked(True, timeout=60_000)

        print("First Participant Checked")

        rows = page.locator("table[ln-table] tbody tr:not(.filter-row)")
        rows.first.wait_for(state="visible", timeout=90_000)

        # ---- extract + paginate ----
        all_results = []
        next_btn = page.locator("button.ln-pagination-next[aria-label='Next page']")
        rows = page.locator("table[ln-table] tbody tr:not(.filter-row)")

        while True:
            all_results.extend(extract_results_from_table(page))

            next_btn = page.locator("button.ln-pagination-next[aria-label='Next page']")
            if next_btn.count() == 0 or next_btn.is_disabled():
                break

            rows.first.wait_for(state="visible", timeout=60_000)
            prev_first = rows.nth(0).inner_text()

            if not click_next_page(page):
                break

            page.wait_for_function(
                """(prev) => {
                    const row = document.querySelector("table[ln-table] tbody tr:not(.filter-row)");
                    return row && row.innerText !== prev;
                }""",
                arg=prev_first,
                timeout=60_000
            )

        airtable_rows = []
        for r in all_results:
            airtable_rows.append({
                "Court": r["court"],
                "Docket Number": r["docket_number"],
                "Defendant": r["defendant"],
                "Case Name": r["case_name"],
                "Nature of Suit": r["nature_of_suit"],
                "Cause": r["cause"],
                "Complaint": r["complaint"],
                "Date Hit": r["date_hit"],
                "Date Filed": r["date_filed"],
                # "Key": r["key"],
            })
        created = send_rows_to_airtable(airtable_rows)
        print("Sent rows to Airtable:", created)
        print("Filtered rows exported:", len(all_results))

    finally:
        #time.sleep(90)
        signal.alarm(0)
        try:
            if browser:
                browser.close()
        finally:
            playwright.stop()
        print("Done!")

if __name__ == "__main__":
    main()
