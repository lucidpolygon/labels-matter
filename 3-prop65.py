import os
import re
import json
import time
import requests
import signal
from datetime import datetime, timedelta
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

load_dotenv()

HEADLESS = os.getenv("HEADLESS", "true").lower() == "true"

START_URL = "https://oag.ca.gov/prop65/60-day-notice-search-results"
BASE_URL = "https://oag.ca.gov"

AIRTABLE_TOKEN = os.environ["AIRTABLE_TOKEN"]
AIRTABLE_BASE_ID = os.environ["AIRTABLE_BASE_ID"]
AIRTABLE_TABLE = os.environ["AIRTABLE_PROP65_TABLE"]
AIRTABLE_URL = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE}"

# ---------- helpers ----------
def _timeout_handler(signum, frame):
    raise TimeoutError("Global scrape timeout hit")

def chunked(lst, n=10):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

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
        payload = {"records": [{"fields": r} for r in batch]}
        resp = requests.post(AIRTABLE_URL, headers=headers, json=payload, timeout=30)

        if resp.status_code >= 300:
            raise RuntimeError(f"Airtable error {resp.status_code}: {resp.text}")

        created += len(resp.json().get("records", []))
        time.sleep(0.25)

    return created

def clean_ws(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())

def parse_count(text: str, label: str) -> int:
    m = re.search(rf"{label}\s*\((\d+)\)", text or "", flags=re.I)
    return int(m.group(1)) if m else 0

def extract_prop65_rows(page) -> list[dict]:
    # wait for at least one row to show up
    page.wait_for_selector("div.view-prop65 div.view-content div.views-row", timeout=90_000)

    # Pull everything via evaluate for speed/stability (similar spirit to your Lexis table extraction)
    rows_data = page.evaluate(
        """
        () => {
          const rows = Array.from(document.querySelectorAll("div.view-prop65 div.view-content div.views-row"));
          return rows.map(r => {
            const out = {};

            // AG number + detail URL
            const a = r.querySelector("h3.ag-number a");
            const aText = (a?.innerText || "").trim();
            const href = a?.getAttribute("href") || "";
            out.ag_text = aText;
            out.detail_href = href;

            // PDF
            const pdfA =
              r.querySelector('a[href*="/system/files/prop65/notices/"][href$=".pdf"]')
              || r.querySelector('a[href$=".pdf"]');
            out.pdf_url = pdfA?.getAttribute("href") || "";

            // key/value blocks
            // structure: <div class="details-label"><div class="details">Date Filed: </div><div>...</div></div>
            const blocks = Array.from(r.querySelectorAll("div.details-label"));
            for (const b of blocks) {
              const k = (b.querySelector("div.details")?.innerText || "").trim();
              if (!k) continue;

              // value: try the 2nd child div, else whole text minus key
              const children = Array.from(b.children);
              let v = "";
              if (children.length >= 2) {
                v = (children[1].innerText || "").trim();
              } else {
                v = (b.innerText || "").replace(k, "").trim();
              }
              out[k] = v;
            }

            // Plaintiff attorney + chemical (special markup)
            const pa = r.querySelector(".views-field-field-prop65-p-attorney .field-content");
            out["Plaintiff Attorney"] = (pa?.innerText || "").trim();

            const chem = r.querySelector(".views-field-field-prop65-chemical .field-content");
            out["Chemical"] = (chem?.innerText || "").trim();

            // Complaint/Settlement/Judgment counts
            const types = r.querySelector("div.field-name-field-prop65-type");
            out.types_text = (types?.innerText || "").trim();

            return out;
          });
        }
        """
    )

    parsed = []
    for r in rows_data:
        ag_text = clean_ws(r.get("ag_text", ""))
        detail_href = (r.get("detail_href") or "").strip()
        pdf_url = (r.get("pdf_url") or "").strip()

        m = re.search(r"AG Number\s+(\d{4}-\d+)", ag_text)
        if not m:
            # fallback from href
            m = re.search(r"(\d{4}-\d+)", detail_href or "")
        if not m:
            continue

        ag_number = m.group(1)
        detail_url = detail_href if detail_href.startswith("http") else (BASE_URL + detail_href if detail_href.startswith("/") else detail_href)

        date_filed = clean_ws(r.get("Date Filed:", ""))
        noticing_party = clean_ws(r.get("Noticing Party:", ""))
        alleged_violators = clean_ws(r.get("Alleged Violators:", ""))
        source = clean_ws(r.get("Source:", ""))
        comments = clean_ws(r.get("Comments:", ""))

        plaintiff_attorney = clean_ws(r.get("Plaintiff Attorney", ""))
        chemical = clean_ws(r.get("Chemical", ""))

        types_text = clean_ws(r.get("types_text", ""))
        complaint_count = parse_count(types_text, "Complaint")
        settlement_count = parse_count(types_text, "Settlement")
        judgment_count = parse_count(types_text, "Judgment")

        parsed.append({
            "AG Number": ag_number,
            "Detail URL": detail_url,
            "Notice PDF URL": pdf_url,
            "Notice File": [{"url": pdf_url}] if pdf_url else [],
            "Date Filed": date_filed,
            "Noticing Party": noticing_party,
            "Plaintiff Attorney": plaintiff_attorney,
            "Alleged Violators": alleged_violators,
            "Chemical": chemical,
            "Source": source,
            "Comments": comments,
            "Complaint Count": complaint_count,
            "Settlement Count": settlement_count,
            "Judgment Count": settlement_count,
            "Judgment Count": judgment_count,
        })

    return parsed

def click_next_if_exists(page) -> bool:
    # pagination has: <li class="next"><a href="...">Next ›</a></li>
    next_a = page.locator("ul.pagination li.next a").first
    if next_a.count() == 0:
        return False

    href = next_a.get_attribute("href") or ""
    if not href:
        return False

    url = href if href.startswith("http") else (BASE_URL + href if href.startswith("/") else href)

    # Navigate by goto (more reliable than click on slow Render)
    page.goto(url, wait_until="domcontentloaded", timeout=60_000)
    return True

def try_parse_mmddyyyy(s: str):
    try:
        return datetime.strptime((s or "").strip(), "%m/%d/%Y").date()
    except Exception:
        return None

def main():
    print("stage: start", flush=True)
    signal.signal(signal.SIGALRM, _timeout_handler)
    signal.alarm(15 * 60)

    today = datetime.now()
    yesterday = today - timedelta(days=1)
    target_str = yesterday.strftime("%m/%d/%Y")

    print("target Date Filed:", target_str, flush=True)

    playwright = sync_playwright().start()
    browser = None

    try:
        print("stage: launching chromium", flush=True)
        browser = playwright.chromium.launch(
            headless=HEADLESS,
            args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-popup-blocking", "--disable-features=IsolateOrigins,site-per-process"],
        )

        context_kwargs = dict(
            accept_downloads=False,
            viewport={"width": 1280, "height": 720},
        )

        print("stage: chromium launched", flush=True)

        context = browser.new_context(**context_kwargs)
        page = context.new_page()

        # Render is slower. Keep same pattern as your script
        page.set_default_timeout(120_000)
        page.set_default_navigation_timeout(120_000)

        page.goto(START_URL, wait_until="domcontentloaded", timeout=60_000)

        print("stage: on prop65 results page", flush=True)

        all_matches = []
        stop = False

        while True:
            rows = extract_prop65_rows(page)

            # Filter to yesterday, and early-stop once we see older dates (list is newest -> oldest)
            for r in rows:
                df = (r.get("Date Filed") or "").strip()
                if df == target_str:
                    all_matches.append(r)
                else:
                    df_date = try_parse_mmddyyyy(df)
                    target_date = try_parse_mmddyyyy(target_str)
                    if df_date and target_date and df_date < target_date:
                        stop = True
                        break

            if stop:
                break

            if not click_next_if_exists(page):
                break

            # be polite / match your pacing style
            time.sleep(0.35)

        print("stage: scraped matches", len(all_matches), flush=True)

        if all_matches:
            created = send_rows_to_airtable(all_matches)
            print("Sent rows to Airtable:", created, flush=True)
        else:
            print("No rows for yesterday.", flush=True)

    finally:
        signal.alarm(0)
        try:
            if browser:
                browser.close()
        finally:
            playwright.stop()
        print("Done!", flush=True)

if __name__ == "__main__":
    main()
