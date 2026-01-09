import os
import re
import json
import time
import requests
from datetime import datetime
from urllib.parse import urlsplit
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

load_dotenv()

HEADLESS = os.getenv("HEADLESS", "true").lower() == "true"

STATE_FILE = "lexis_state.json"
COURT_LINK  = os.getenv("LEXIS_URL")
LEXIS_ALERTS_URL = os.getenv("LEXIS_ALERTS_URL")
LEXIS_USER = os.getenv("LEXIS_USER")
LEXIS_PASS = os.getenv("LEXIS_PASS")

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
AIRTABLE_TABLE = os.environ["AIRTABLE_TABLE"]

AIRTABLE_URL = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE}"

# ---------- helpers ----------
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
    
def main():
    today = datetime.now()
    if ALERT_FROM and ALERT_TO:
        date_from = ALERT_FROM
        date_to   = ALERT_TO
    else:
        date_from = today.strftime("%m/%d/%Y")
        date_to   = today.strftime("%m/%d/%Y")

    playwright = sync_playwright().start()
    browser = None

    try:
        browser = playwright.chromium.launch(headless=HEADLESS)            
        context = (
            browser.new_context(storage_state=STATE_FILE, accept_downloads=True)
            if os.path.exists(STATE_FILE)
            else browser.new_context(accept_downloads=True)
        )

        page = context.new_page()
        page.goto(COURT_LINK, wait_until="domcontentloaded", timeout=60_000)

        if not is_logged_in(page):
            print("Not Logged in");
            if not LEXIS_USER or not LEXIS_PASS:
                raise RuntimeError("Missing LEXIS_USER / LEXIS_PASS (set in .env or environment)")
            login_lexis(page, LEXIS_USER, LEXIS_PASS)
            page.wait_for_url(f"{COURT_LINK}*", timeout=120_000)            
            page.wait_for_function("() => !document.querySelector('#password')", timeout=120_000)
            context.storage_state(path=STATE_FILE)

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

            next_btn.click()

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
        if browser:
            #time.sleep(90)
            browser.close()        
            print("Done!")
            playwright.stop()

if __name__ == "__main__":
    main()
