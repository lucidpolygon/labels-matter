import os
import re
import json
import time
import boto3
import requests
import signal
from typing import Optional, Dict
from urllib.parse import urljoin
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
from botocore.config import Config

load_dotenv()

HEADLESS = os.getenv("HEADLESS", "true").lower() == "true"

LEXIS_USER = os.getenv("LEXIS_USER")
LEXIS_PASS = os.getenv("LEXIS_PASS")
LEXIS_COURT_LINK  = os.getenv("LEXIS_URL")
LEXIS_ALERTS_URL = os.getenv("LEXIS_ALERTS_URL")
LEXIS_SET_CLIENT_ID_LINK = "https://advance.lexis.com/clclientidset"

AIRTABLE_TOKEN = os.environ["AIRTABLE_TOKEN"]
AIRTABLE_BASE_ID = os.environ["AIRTABLE_BASE_ID"]
AIRTABLE_TABLE = os.environ["AIRTABLE_TABLE"]
AIRTABLE_NO_OF_RECORDS_PER_CALL = int(os.getenv("AIRTABLE_NO_OF_RECORDS_PER_CALL", "3"))

R2_ACCOUNT_ID = os.environ["R2_ACCOUNT_ID"]
R2_ACCESS_KEY_ID = os.environ["R2_ACCESS_KEY_ID"]
R2_SECRET_ACCESS_KEY = os.environ["R2_SECRET_ACCESS_KEY"]
R2_BUCKET = os.environ["R2_BUCKET"]
R2_PUBLIC_BASE_URL = os.getenv("R2_PUBLIC_BASE_URL", "").rstrip("/")
_R2 = None

F_ATTEMPTS="Complaint Attempt Count"
F_STATUS="Complaint Status"
F_FILE="Complaint File"
F_DOCKET_NUMBER="Docket Number"
F_DEFENDANT="Defendant"
F_CASE_NAME="Case Name"
F_ERROR_FIELD="Complaint Error"

AIRTABLE_URL = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE}"

def _timeout_handler(signum, frame):
    raise TimeoutError("Global scrape timeout hit")

def airtable_headers():
    return {
        "Authorization": f"Bearer {AIRTABLE_TOKEN}",
        "Content-Type": "application/json",
    }

def fetch_queue(limit, max_attempts=5):
    # Complaint File empty + Status empty or Error + attempts < max_attempts
    # Note: Attachment emptiness checks are a bit awkward; this works well in practice:
    formula = (
        f"AND("
        f"NOT({{{F_FILE}}}),"
        f"OR({{{F_STATUS}}} = '', {{{F_STATUS}}} = 'Error'),"
        f"OR({{{F_ATTEMPTS}}} = '', {{{F_ATTEMPTS}}} < {max_attempts})"
        f")"
    )

    params = {
        "pageSize": limit,
        "filterByFormula": formula,
    }
    r = requests.get(AIRTABLE_URL, headers=airtable_headers(), params=params, timeout=30)
    r.raise_for_status()
    return r.json().get("records", [])

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

def login_lexis(page, username: str, password: str):
    page.wait_for_selector("#userid", timeout=60_000)
    page.fill("#userid", username)
    page.click("#signInSbmtBtn")

    page.wait_for_selector("#password", timeout=60_000)
    page.fill("#password", password)
    page.click("#next")

def is_logged_in(page) -> bool:
    return page.locator("#userid").count() == 0

def norm_title(s: str) -> str:
    s = (s or "").strip().lower()
    # collapse whitespace + remove punctuation that often differs
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"[^a-z0-9 ]+", "", s)
    return s

def run_courtlink_search(page, docket_number: str, litigant_name: str):
    page.goto(LEXIS_COURT_LINK, wait_until="domcontentloaded")

    page.wait_for_selector("#docketNumberInput", timeout=60_000)
    page.fill("#docketNumberInput", docket_number)

    name_input = page.locator("input[placeholder='Enter Name...']").first
    name_input.wait_for(state="visible", timeout=60_000)
    name_input.fill(litigant_name)

    def_cb = page.locator("#litigant-defendant1").first
    def_cb.wait_for(state="visible", timeout=60_000)
    if not def_cb.is_checked():
        def_cb.check()

    for sel in ["#litigant-plaintiff1", "#litigant-other1"]:
        cb = page.locator(sel).first
        if cb.count() and cb.is_checked():
            cb.uncheck()

    page.locator("#triggersearch").click()

    # TODO: replace with a real results selector once you identify it
    page.wait_for_timeout(2000)

def wait_results_loaded(page, timeout=60_000):
    # results container
    page.wait_for_selector("resultslist result-item", timeout=timeout)
    # wait for the loading box to go away (if present)
    loadbox = page.locator("loadbox .loadbox")
    if loadbox.count():
        try:
            page.wait_for_selector("loadbox .loadbox", state="hidden", timeout=timeout)
        except Exception:
            pass  # sometimes it lingers but results are already clickable

def click_first_result_if_title_matches(page, expected_case_name: str) -> bool:
    wait_results_loaded(page)

    first = page.locator("resultslist result-item").first
    title_a = first.locator("a.titleLink").first
    title_a.wait_for(state="visible", timeout=60_000)

    found = title_a.inner_text().strip()

    if norm_title(found) != norm_title(expected_case_name):
        print("Title mismatch")
        print("  expected:", expected_case_name)
        print("  found   :", found)
        return False

    title_a.click()
    # you’ll add the next actions after opening the case
    return True

def click_free_complaint_row(page, timeout=60_000) -> bool:
    page.wait_for_selector("tr[data-proceedingnumber]", timeout=timeout)
    rows = page.locator("tr[data-proceedingnumber]")

    for i in range(rows.count()):
        row = rows.nth(i)

        free_link = row.locator(
            "a.SS_ProceedingLink[data-action='ProceedingFree']:has-text('Free')"
        ).first
        if free_link.count() == 0:
            continue

        text_td = row.locator("td[id^='text_']").first
        if text_td.count() == 0:
            continue

        txt = (text_td.inner_text() or "").strip()
        if not txt.upper().startswith("COMPLAINT"):
            continue

        # The Free link does NOT navigate; it opens a modal.
        # Click and wait for the Get Documents button to appear.
        free_link.scroll_into_view_if_needed()

        get_btn = page.locator("button.button.primary:has-text('Get Documents')").first

        for attempt in range(1, 4):
            try:
                free_link.click(timeout=10_000)
            except Exception:
                # fallback if overlay / weird click interception
                free_link.click(force=True, timeout=10_000)

            try:
                get_btn.wait_for(state="visible", timeout=15_000)
                print("Clicked Free -> Get Documents visible")
                return True
            except Exception:
                print(f"Free clicked but modal not visible (attempt {attempt}/3), retrying...")
                page.wait_for_timeout(800)

        return False

    return False

def _is_pdfish(resp) -> bool:
    ct = (resp.headers.get("content-type") or "").lower()
    cd = (resp.headers.get("content-disposition") or "").lower()
    u = (resp.url or "").lower()
    return ("application/pdf" in ct) or (".pdf" in cd) or ("/downloadfile/" in u)

def click_get_documents_and_fetch_pdf(context, page, timeout=180_000):
    get_btn = page.locator("button.button.primary", has_text=re.compile(r"Get Documents", re.I)).first
    view_link = page.locator("#viewfile a", has_text=re.compile(r"View", re.I)).first

    get_btn.wait_for(state="visible", timeout=timeout)
    print("Modal ready: Get Documents visible")

    # Click Get Documents until View appears
    for _ in range(1, 7):
        try:
            get_btn.click(timeout=10_000)
        except Exception:
            get_btn.click(force=True, timeout=10_000)

        try:
            view_link.wait_for(state="visible", timeout=25_000)
            print("View link visible")
            break
        except Exception:
            page.wait_for_timeout(900)
    else:
        raise TimeoutError("Clicked Get Documents but View never appeared")

    # 1) Capture window.open(url) because View has no href
    page.evaluate("""
        () => {
          if (window.__pw_open_patched) return;
          window.__pw_open_patched = true;
          window.__pw_last_open_url = null;
          const orig = window.open;
          window.open = function(url, name, specs) {
            window.__pw_last_open_url = url;
            return orig.call(window, url, name, specs);
          };
        }
    """)

    # 2) Also capture the PDF response at *context* level (popup or no popup)
    hit = {"resp": None}
    def on_response(resp):
        if hit["resp"] is None:
            try:
                if _is_pdfish(resp):
                    hit["resp"] = resp
            except Exception:
                pass

    context.on("response", on_response)

    # Click View (don’t wait for popup load states)
    view_link.scroll_into_view_if_needed()
    view_link.click(force=True, timeout=10_000)

    # Wait for either: window.open url OR a pdf-ish response
    deadline = time.time() + (timeout / 1000)
    opened_url = None
    while time.time() < deadline:
        if hit["resp"] is not None:
            break
        opened_url = page.evaluate("() => window.__pw_last_open_url")
        if opened_url:
            break
        page.wait_for_timeout(200)

    try:
        context.off("response", on_response)
    except Exception:
        pass

    # If we saw the response, just read bytes from it
    if hit["resp"] is not None:
        r = hit["resp"]
        return r.url, r.body()

    # If we only captured the opened URL, fetch it using the authenticated context
    if opened_url:
        abs_url = urljoin(page.url, opened_url)  # <-- critical
        print("Fetching:", abs_url)

        resp = context.request.get(abs_url, timeout=timeout)
        if not resp.ok:
            raise RuntimeError(f"Failed to fetch PDF. status={resp.status} url={abs_url}")

        body = resp.body()

        # Sanity check: first bytes should look like PDF
        if not body.startswith(b"%PDF"):
            # Lexis sometimes returns HTML if auth expired
            snippet = body[:200].decode("utf-8", errors="replace")
            raise RuntimeError(f"Got non-PDF response from {abs_url}. First 200 chars:\n{snippet}")

        return abs_url, body

    raise TimeoutError("Clicked View but neither popup URL nor PDF response was observed.")

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

def upload_pdf_to_r2_and_get_url(key: str, data: bytes) -> str:
    s3 = r2_client()
    s3.put_object(
        Bucket=R2_BUCKET,
        Key=key,
        Body=data,
        ContentType="application/pdf",
    )

    # Otherwise: signed URL (Airtable only needs it once)
    return s3.generate_presigned_url(
        ClientMethod="get_object",
        Params={"Bucket": R2_BUCKET, "Key": key},
        ExpiresIn=60 * 60 * 24,  # 24h
    )

def patch_airtable(rec_id: str, fields: dict):
    r = requests.patch(
        f"{AIRTABLE_URL}/{rec_id}",
        headers=airtable_headers(),
        json={"fields": fields},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()

def main():

    print("stage: start", flush=True)
    
    signal.signal(signal.SIGALRM, _timeout_handler)
    signal.alarm(15 * 60)

    records = fetch_queue(limit=AIRTABLE_NO_OF_RECORDS_PER_CALL)

    if not records:
        print("No complaints to download.")
        return

    print(f"stage: fetched {len(records)} records", flush=True)
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
        page.goto(LEXIS_COURT_LINK, wait_until="domcontentloaded", timeout=60_000)

        if not is_logged_in(page):
            print("Not Logged in");
            if not LEXIS_USER or not LEXIS_PASS:
                raise RuntimeError("Missing LEXIS_USER / LEXIS_PASS (set in .env or environment)")
            login_lexis(page, LEXIS_USER, LEXIS_PASS)
            page.wait_for_url(f"{LEXIS_COURT_LINK}*", timeout=120_000)            
            page.wait_for_function("() => !document.querySelector('#password')", timeout=120_000)
            save_state_to_r2(context.storage_state())

        print("Login Done")

        page.goto(LEXIS_SET_CLIENT_ID_LINK, wait_until="domcontentloaded", timeout=60_000)

        page.locator("#recent").check()

        client_select = page.locator("select.clientIds")
        client_select.wait_for(state="visible", timeout=30_000)

        current = client_select.input_value()
        if current != "Office":
            client_select.select_option(value="Office")
        
        with page.expect_navigation(wait_until="domcontentloaded", timeout=60_000):
            page.locator("input.submit-client").click()

        print("Client Set")
        
        page.goto(LEXIS_COURT_LINK, wait_until="domcontentloaded", timeout=60_000)

        for i, rec in enumerate(records, start=1):
            rec_id = rec["id"]
            fields = rec.get("fields", {})
            attempts = int(fields.get(F_ATTEMPTS) or 0)

            try:    
                docket = fields.get(F_DOCKET_NUMBER)
                defendant = fields.get(F_DEFENDANT)
                case_name = fields.get(F_CASE_NAME)

                if not docket or not defendant or not case_name:
                    print(f"[{i}] skipped (missing docket/defendant/case_name)")
                    continue

                print(f"[{i}/{len(records)}] {docket} — {defendant}")

                run_courtlink_search(page, docket, defendant)

                if not click_first_result_if_title_matches(page, case_name):
                    raise RuntimeError("Title mismatch / no matching result")

                if not click_free_complaint_row(page):
                    raise RuntimeError("No FREE COMPLAINT row (or modal didn’t open)")

                pdf_url, pdf_bytes = click_get_documents_and_fetch_pdf(context, page)


                safe_docket = re.sub(r"[^A-Za-z0-9._-]+", "_", docket)
                filename = f"{safe_docket}_complaint.pdf"
                key = f"complaints/{safe_docket}/{filename}"

                file_url = upload_pdf_to_r2_and_get_url(key, pdf_bytes)

                patch_airtable(rec_id, {
                    F_FILE: [{"url": file_url, "filename": filename}],
                    F_STATUS: "Done",
                    F_ERROR_FIELD: "", # clear old errors if any
                })

                print(f"Uploaded complaint to R2 ({len(pdf_bytes)} bytes) -> {file_url}")
            except Exception as e:
                print(f"ERROR for record {rec_id}: {e}")
                patch_airtable(rec_id, {
                    F_STATUS: "Error",
                    F_ATTEMPTS: attempts + 1,
                    F_ERROR_FIELD: str(e)[:2000],
                })
                continue

    finally:
        signal.alarm(0)
        try:
            if browser:
                browser.close()
        finally:
            playwright.stop()
        print("Done!")

if __name__ == "__main__":
    main()