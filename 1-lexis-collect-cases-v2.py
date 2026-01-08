import os
import re
import json
import time
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
ALLOW_NATURE = {
    x.strip()
    for x in os.getenv("FILTER_BY_CASE_NATURE", "").split(",")
    if x.strip()
}

# ---------- helpers ----------
def fmt_date(d: datetime) -> str:
    return d.strftime("%b %d, %Y")

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
    rows = page.locator("table[ln-table] tbody tr:not(.filter-row)")
    rows.first.wait_for(state="visible", timeout=60_000)

    results = []
    allow_all_nature = len(ALLOW_NATURE) == 0

    for i in range(rows.count()):
        row = rows.nth(i)
        tds = row.locator("td")

        court        = tds.nth(2).inner_text().strip()
        docket_no    = tds.nth(3).inner_text().strip()
        defendant    = tds.nth(4).inner_text().strip()
        case_name    = tds.nth(5).inner_text().strip()
        nature_suit  = " ".join(tds.nth(6).inner_text().split())
        cause        = tds.nth(7).inner_text().strip()
        complaint    = " ".join(tds.nth(8).inner_text().split()).lower()
        date_hit     = tds.nth(9).inner_text().strip()
        date_filed   = tds.nth(10).inner_text().strip()

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

def main():
    today = datetime.now()
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

        # ---- extract + paginate ----
        all_results = []
        next_btn = page.locator("button.ln-pagination-next[aria-label='Next page']")
        rows = page.locator("table[ln-table] tbody tr:not(.filter-row)")

        while True:
            all_results.extend(extract_results_from_table(page))

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

        # dump correct var
        safe_date = re.sub(r"[^0-9A-Za-z_-]", "_", date_to)
        filename = f"filtered_results_{safe_date}.json"
        tmp = filename + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(all_results, f, indent=2, ensure_ascii=False)
        os.replace(tmp, filename)

        print("Filtered rows exported:", len(all_results))
        print("Output file:", filename)

    finally:
        if browser:
            #time.sleep(90)
            browser.close()        
            print("Done!")
            playwright.stop()

if __name__ == "__main__":
    main()
