"""
Cuyahoga County Motivated Seller Lead Scraper — Option B
=========================================================
Scrapes the Cuyahoga County Common Pleas Court docket for foreclosure filings.
URL: https://cpdocket.cp.cuyahogacounty.us

This runs automatically every day via GitHub Actions.
Manual portal PDFs are handled by the dashboard's PDF upload feature.
"""

import asyncio
import csv
import json
import logging
import os
import re
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import urllib3
import requests
from bs4 import BeautifulSoup

# Suppress SSL warnings for government sites with cert issues
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

try:
    from playwright.async_api import async_playwright, TimeoutError as PWTimeout
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False
    logging.warning("Playwright not available")

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("cuyahoga_scraper")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
COURT_URL        = "https://cpdocket.cp.cuyahogacounty.gov"
SHERIFF_SALE_URL = "https://cpdocket.cp.cuyahogacounty.gov/SheriffSearch/search.aspx"
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "7"))
REPO_ROOT     = Path(__file__).resolve().parent.parent
DASHBOARD_JSON = REPO_ROOT / "dashboard" / "records.json"
DATA_JSON      = REPO_ROOT / "data" / "records.json"
GHL_CSV        = REPO_ROOT / "data" / "ghl_export.csv"
RETRY_ATTEMPTS = 3

# Case types that indicate motivated sellers
CASE_TYPES = [
    ("Foreclosure",          "NOFC",    "Notice of Foreclosure",   "NOFC"),
    ("Foreclosure - Tax",    "TAXDEED", "Tax Foreclosure",         "TAXDEED"),
    ("Partition",            "JUD",     "Partition / Judgment",    "JUD"),
    ("Probate",              "PRO",     "Probate",                 "PRO"),
]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def parse_amount(text) -> Optional[float]:
    if not text:
        return None
    clean = re.sub(r"[^\d.]", "", str(text))
    try:
        return float(clean) if clean else None
    except ValueError:
        return None

def normalize_name(name: str) -> str:
    return " ".join(name.upper().split()) if name else ""

def name_variants(full_name: str) -> list:
    full_name = normalize_name(full_name)
    variants = {full_name}
    if "," in full_name:
        parts = [p.strip() for p in full_name.split(",", 1)]
        if len(parts) == 2:
            variants.add(f"{parts[1]} {parts[0]}")
    else:
        parts = full_name.split()
        if len(parts) >= 2:
            variants.add(f"{parts[-1]}, {' '.join(parts[:-1])}")
    return [v for v in variants if v]

def fmt_date(raw: str) -> str:
    if not raw:
        return ""
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y", "%m/%d/%y"):
        try:
            return datetime.strptime(str(raw).strip()[:10], fmt).strftime("%m/%d/%Y")
        except Exception:
            continue
    return str(raw).strip()[:10]


# ===========================================================================
# Court Docket Scraper
# ===========================================================================

class CourtDocketScraper:
    """
    Scrapes Cuyahoga County Common Pleas Court docket for foreclosure cases.
    The docket is a public ASP.NET site — we use requests + BeautifulSoup.
    """

    def __init__(self):
        self.date_to   = datetime.now()
        self.date_from = self.date_to - timedelta(days=LOOKBACK_DAYS)
        self.df_str    = self.date_from.strftime("%m/%d/%Y")
        self.dt_str    = self.date_to.strftime("%m/%d/%Y")
        self.session   = requests.Session()
        self.session.verify = False   # govt site has SSL cert issues from GitHub runners
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                          "AppleWebKit/537.36 Chrome/122.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,*/*",
        })
        self.raw_records: list[dict] = []

    # ------------------------------------------------------------------
    async def run(self):
        log.info("Scraping Cuyahoga Common Pleas Court docket ...")
        log.info("Date range: %s - %s", self.df_str, self.dt_str)

        # Strategy 1: Sheriff Sale search (direct foreclosure endpoint)
        self._scrape_sheriff_sales()

        # Strategy 2: General case search by case type
        if not self.raw_records:
            self._scrape_requests()

        # Strategy 3: Playwright fallback
        if not self.raw_records and PLAYWRIGHT_AVAILABLE:
            log.info("Trying Playwright for court docket ...")
            await self._scrape_playwright()

        log.info("Court docket: %d records collected", len(self.raw_records))

    # ------------------------------------------------------------------
    def _scrape_sheriff_sales(self):
        """
        Scrape the Sheriff Sale search — direct foreclosure endpoint.
        URL: /SheriffSearch/search.aspx with dateFrom / dateTo params.
        """
        log.info("Trying Sheriff Sale search ...")
        try:
            # GET the page first for hidden fields
            r = self.session.get(SHERIFF_SALE_URL, timeout=30)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "lxml")

            form_data = {}
            for inp in soup.find_all("input"):
                n = inp.get("name","")
                v = inp.get("value","")
                if n:
                    form_data[n] = v

            # Fill date fields — try common field name patterns
            for k in list(form_data.keys()):
                kl = k.lower()
                if "datefrom" in kl or "startdate" in kl or "from" in kl:
                    form_data[k] = self.df_str
                elif "dateto" in kl or "enddate" in kl or ("to" in kl and "date" in kl):
                    form_data[k] = self.dt_str

            # Also set by common names directly
            for fname in ["dateFrom","dateTo","txtDateFrom","txtDateTo",
                          "StartDate","EndDate","start_date","end_date"]:
                if fname not in form_data:
                    if "from" in fname.lower() or "start" in fname.lower():
                        form_data[fname] = self.df_str
                    else:
                        form_data[fname] = self.dt_str

            form_data["__EVENTTARGET"]   = ""
            form_data["__EVENTARGUMENT"] = ""

            # Find and set submit button
            for inp in soup.find_all("input", type="submit"):
                form_data[inp.get("name","btnSearch")] = inp.get("value","Search")
                break

            resp = self.session.post(SHERIFF_SALE_URL, data=form_data, timeout=30)
            resp.raise_for_status()

            recs = self._parse_sheriff_html(resp.text)
            self.raw_records.extend(recs)
            log.info("Sheriff sales: %d records", len(recs))

        except Exception as e:
            log.warning("Sheriff sale search failed: %s", e)

    # ------------------------------------------------------------------
    def _parse_sheriff_html(self, html: str) -> list[dict]:
        """
        Parse the Cuyahoga County Sheriff Sale results page.
        """
        records = []
        soup = BeautifulSoup(html, "lxml")

        for table in soup.find_all("table"):
            rows = table.find_all("tr")
            if len(rows) < 2:
                continue

            header_row = rows[0]
            headers = [th.get_text(strip=True).lower()
                       for th in header_row.find_all(["th","td"])]
            header_str = " ".join(headers)

            if not any(k in header_str for k in
                       ["case","sale","parcel","address","plaintiff","defendant"]):
                continue

            # Log actual headers and first data row for debugging
            log.info("Sheriff table headers: %s", headers)
            if len(rows) > 1:
                first = [c.get_text(" ", strip=True)
                         for c in rows[1].find_all(["td","th"])]
                log.info("Sheriff first row: %s", first)

            # Map column indices from actual headers
            col = {}
            for i, h in enumerate(headers):
                h = h.strip()
                if any(k in h for k in ["case #","case no","cv #","case number","caseno"]):
                    col.setdefault("case_num", i)
                elif h in ["case", "no", "#"] or "case" in h:
                    col.setdefault("case_num", i)
                if any(k in h for k in ["sale date","date of sale","sale dt","scheduled date","saledate"]):
                    col.setdefault("date", i)
                elif "date" in h and "case" not in h:
                    col.setdefault("date", i)
                if "plaintiff" in h:
                    col.setdefault("plaintiff", i)
                if "defendant" in h or "owner" in h:
                    col.setdefault("defendant", i)
                if "address" in h or "property addr" in h:
                    col.setdefault("address", i)
                if "parcel" in h and "address" not in h:
                    col.setdefault("parcel", i)
                if any(k in h for k in ["appraised","appraisal","value","amount","judgment"]):
                    col.setdefault("amount", i)

            log.info("Sheriff column map: %s", col)

            for row in rows[1:]:
                cells = [c.get_text(" ", strip=True) for c in row.find_all(["td","th"])]
                if len(cells) < 3:
                    continue

                def get(key, default=""):
                    idx = col.get(key)
                    if idx is not None and idx < len(cells):
                        return cells[idx].strip()
                    return default

                # Use positional if no column map
                n = len(cells)
                if not col:
                    # Typical Cuyahoga order: CaseNum SaleDate Plaintiff Defendant Address Parcel AppValue Status
                    case_num  = cells[0] if n > 0 else ""
                    sale_date = cells[1] if n > 1 else ""
                    plaintiff = cells[2] if n > 2 else ""
                    defendant = cells[3] if n > 3 else ""
                    address   = cells[4] if n > 4 else ""
                    parcel    = cells[5] if n > 5 else ""
                    amount    = cells[6] if n > 6 else ""
                else:
                    case_num  = get("case_num")
                    sale_date = get("date")
                    plaintiff = get("plaintiff")
                    defendant = get("defendant")
                    address   = get("address")
                    parcel    = get("parcel")
                    amount    = get("amount")

                # Clean case number
                case_match = re.search(r'(CV\s*\d{4,}|\d{2,4}\s*CV\s*\d+)', case_num, re.I)
                clean_case = case_match.group(1).replace(" ","") if case_match else case_num[:20]
                if not clean_case:
                    continue

                # Clean date
                date_match = re.search(r'\d{1,2}/\d{1,2}/\d{4}', sale_date)
                clean_date = date_match.group(0) if date_match else ""

                # Clean address
                addr_match = re.search(
                    r'\d+\s+[A-Za-z][A-Za-z0-9\s]{3,40?}'
                    r'(?:ST|AVE|DR|RD|BLVD|LN|CT|PL|WAY|CIR|PKWY|STREET|AVENUE|DRIVE|ROAD|LANE|COURT)\b',
                    address, re.I
                )
                clean_addr = addr_match.group(0).strip() if addr_match else ""
                if not clean_addr and address and not re.match(r'^(CV|Case|IF NOT|\d{3}-\d{2})', address, re.I):
                    clean_addr = address[:80]

                zip_match = re.search(r'\b(\d{5})\b', address)
                clean_zip = zip_match.group(1) if zip_match else ""

                # Clean amount
                raw_amount = parse_amount(amount)
                if raw_amount and raw_amount > 50_000_000:
                    raw_amount = None

                # Clean owner — reject values that look like non-names
                clean_owner = defendant.strip()
                if re.match(r'^(CV|IF NOT|PARCEL|CASE #|\d{3}-\d{2})', clean_owner, re.I):
                    clean_owner = ""

                clean_plaintiff = plaintiff.strip()
                if re.match(r'^(CV|IF NOT|PARCEL|CASE #|\d{3}-\d{2})', clean_plaintiff, re.I):
                    clean_plaintiff = ""

                # Build URL
                link_tag = row.find("a", href=True)
                href = link_tag["href"] if link_tag else ""
                if href and not href.startswith("http"):
                    href = f"{COURT_URL}/{href.lstrip('/')}"

                # Date filter
                if clean_date:
                    try:
                        dt = datetime.strptime(clean_date, "%m/%d/%Y")
                        if dt < self.date_from:
                            continue
                    except Exception:
                        pass

                records.append({
                    "doc_num":      clean_case,
                    "doc_type":     "NOFC",
                    "cat":          "NOFC",
                    "cat_label":    "Notice of Foreclosure",
                    "filed":        clean_date,
                    "owner":        clean_owner,
                    "grantee":      clean_plaintiff,
                    "amount":       raw_amount,
                    "legal":        parcel,
                    "clerk_url":    href,
                    "prop_address": clean_addr,
                    "prop_city":    "Cleveland",
                    "prop_state":   "OH",
                    "prop_zip":     clean_zip,
                    "mail_address": "",
                    "mail_city":    "",
                    "mail_state":   "OH",
                    "mail_zip":     "",
                    "source":       "Court Docket",
                })

        return records
        records = []
        soup = BeautifulSoup(html, "lxml")

        for table in soup.find_all("table"):
            rows = table.find_all("tr")
            if len(rows) < 2:
                continue

            # Get headers
            header_row = rows[0]
            headers = [th.get_text(strip=True).lower()
                       for th in header_row.find_all(["th","td"])]
            header_str = " ".join(headers)

            # Must look like a sheriff sale table
            if not any(k in header_str for k in
                       ["case","sale","parcel","address","plaintiff","defendant"]):
                continue

            # Build column index map from headers
            col = {}
            for i, h in enumerate(headers):
                if any(k in h for k in ["case","cv #","case #","case no"]):
                    col.setdefault("case_num", i)
                if any(k in h for k in ["sale date","date of sale","scheduled","filed"]):
                    col.setdefault("date", i)
                if "plaintiff" in h or "lender" in h or "bank" in h:
                    col.setdefault("plaintiff", i)
                if "defendant" in h or "owner" in h or "debtor" in h:
                    col.setdefault("defendant", i)
                if "address" in h or "property" in h or "location" in h:
                    col.setdefault("address", i)
                if "parcel" in h and "address" not in h:
                    col.setdefault("parcel", i)
                if any(k in h for k in ["appraised","value","amount","judgment"]):
                    col.setdefault("amount", i)

            for row in rows[1:]:
                cells = [c.get_text(" ", strip=True) for c in row.find_all(["td","th"])]
                if len(cells) < 3:
                    continue

                def get(key, default=""):
                    idx = col.get(key)
                    if idx is not None and idx < len(cells):
                        return cells[idx].strip()
                    return default

                # Positional fallback if headers didn't map well
                # Cuyahoga sheriff sale typical order:
                # 0=CaseNum, 1=SaleDate, 2=Plaintiff, 3=Defendant,
                # 4=Address, 5=Parcel, 6=AppraisedValue, 7=Status
                if not col:
                    case_num  = cells[0] if len(cells) > 0 else ""
                    sale_date = cells[1] if len(cells) > 1 else ""
                    plaintiff = cells[2] if len(cells) > 2 else ""
                    defendant = cells[3] if len(cells) > 3 else ""
                    address   = cells[4] if len(cells) > 4 else ""
                    parcel    = cells[5] if len(cells) > 5 else ""
                    amount    = cells[6] if len(cells) > 6 else ""
                else:
                    case_num  = get("case_num")
                    sale_date = get("date")
                    plaintiff = get("plaintiff")
                    defendant = get("defendant")
                    address   = get("address")
                    parcel    = get("parcel")
                    amount    = get("amount")

                # Clean case number — must look like CV + digits
                case_match = re.search(r'(CV\s*\d+|\d{2}CV\d+)', case_num, re.I)
                if case_match:
                    case_num = case_match.group(1).replace(" ","")
                elif not case_num:
                    continue

                # Clean date — extract MM/DD/YYYY pattern
                date_match = re.search(r'\d{1,2}/\d{1,2}/\d{4}', sale_date)
                clean_date = date_match.group(0) if date_match else ""

                # Clean address — must have a street number
                addr_match = re.search(
                    r'\d+\s+[A-Za-z][A-Za-z0-9\s]{3,40}'
                    r'(?:ST|AVE|DR|RD|BLVD|LN|CT|PL|WAY|CIR|PKWY|STREET|AVENUE|DRIVE|ROAD|LANE|COURT)',
                    address, re.I
                )
                clean_addr = addr_match.group(0).strip() if addr_match else address[:80] if address else ""

                # Clean zip from address if present
                zip_match = re.search(r'\b(\d{5})\b', address)
                clean_zip = zip_match.group(1) if zip_match else ""

                # Clean amount — reject absurd values (parcel numbers mistaken for money)
                raw_amount = parse_amount(amount)
                if raw_amount and raw_amount > 50_000_000:
                    raw_amount = None  # parcel number, not a dollar amount

                # Clean owner name — remove junk
                clean_owner = defendant.strip()
                # If it looks like a case number or address, discard
                if re.match(r'^(CV|IF NOT|PARCEL|CASE|703|812|\d{3}-)', clean_owner, re.I):
                    clean_owner = ""

                # Clean plaintiff
                clean_plaintiff = plaintiff.strip()
                if re.match(r'^(CV|IF NOT|PARCEL|CASE|\d{3}-)', clean_plaintiff, re.I):
                    clean_plaintiff = ""

                # Build clerk URL
                link_tag = row.find("a", href=True)
                href = link_tag["href"] if link_tag else ""
                if href and not href.startswith("http"):
                    href = f"{COURT_URL}/{href.lstrip('/')}"

                # Filter by date range
                if clean_date:
                    try:
                        dt = datetime.strptime(clean_date, "%m/%d/%Y")
                        if dt < self.date_from:
                            continue
                    except Exception:
                        pass

                records.append({
                    "doc_num":      case_num,
                    "doc_type":     "NOFC",
                    "cat":          "NOFC",
                    "cat_label":    "Notice of Foreclosure",
                    "filed":        clean_date,
                    "owner":        clean_owner,
                    "grantee":      clean_plaintiff,
                    "amount":       raw_amount,
                    "legal":        parcel,
                    "clerk_url":    href,
                    "prop_address": clean_addr,
                    "prop_city":    "Cleveland",
                    "prop_state":   "OH",
                    "prop_zip":     clean_zip,
                    "mail_address": "",
                    "mail_city":    "",
                    "mail_state":   "OH",
                    "mail_zip":     "",
                    "source":       "Court Docket",
                })

        return records

    # ------------------------------------------------------------------
    def _scrape_requests(self):
        """Scrape the court docket search using requests."""
        for case_label, code, label, cat in CASE_TYPES:
            try:
                records = self._search_case_type(case_label, code, label, cat)
                if records:
                    self.raw_records.extend(records)
                    log.info("  %s -> %d records", case_label, len(records))
                else:
                    log.info("  %s -> 0 records", case_label)
            except Exception as e:
                log.warning("Failed %s: %s", case_label, e)
            time.sleep(1)

    # ------------------------------------------------------------------
    def _search_case_type(self, case_type: str, code: str,
                          label: str, cat: str) -> list[dict]:
        records = []

        # Step 1: GET the search page to grab hidden fields
        search_url = f"{COURT_URL}/Search.aspx"
        for attempt in range(1, RETRY_ATTEMPTS + 1):
            try:
                r = self.session.get(search_url, timeout=30)
                r.raise_for_status()
                break
            except Exception as e:
                log.warning("GET attempt %d/%d: %s", attempt, RETRY_ATTEMPTS, e)
                if attempt == RETRY_ATTEMPTS:
                    return records
                time.sleep(3 * attempt)

        soup = BeautifulSoup(r.text, "lxml")

        # Extract hidden ASP.NET fields
        form_data = {}
        for inp in soup.find_all("input"):
            name  = inp.get("name","")
            value = inp.get("value","")
            if name:
                form_data[name] = value

        # Fill in search parameters
        # Common field names on Cuyahoga court docket
        field_map = {
            "case_type":  ["ddlCaseType","DropDownList1","caseType","CaseType"],
            "date_from":  ["txtFiledFrom","txtDateFrom","Filed_From","dtFrom"],
            "date_to":    ["txtFiledTo","txtDateTo","Filed_To","dtTo"],
            "search_btn": ["btnSearch","Button1","cmdSearch","Search"],
        }

        for field_key, field_names in field_map.items():
            for fname in field_names:
                if fname in form_data or soup.find(attrs={"name": fname}):
                    if field_key == "case_type":
                        form_data[fname] = case_type
                    elif field_key == "date_from":
                        form_data[fname] = self.df_str
                    elif field_key == "date_to":
                        form_data[fname] = self.dt_str
                    elif field_key == "search_btn":
                        form_data[fname] = "Search"
                    break

        # __doPostBack
        form_data["__EVENTTARGET"]   = form_data.get("__EVENTTARGET","btnSearch")
        form_data["__EVENTARGUMENT"] = ""

        # Step 2: POST the search
        for attempt in range(1, RETRY_ATTEMPTS + 1):
            try:
                resp = self.session.post(
                    search_url, data=form_data, timeout=30
                )
                resp.raise_for_status()
                break
            except Exception as e:
                log.warning("POST attempt %d/%d: %s", attempt, RETRY_ATTEMPTS, e)
                if attempt == RETRY_ATTEMPTS:
                    return records
                time.sleep(3 * attempt)

        # Step 3: Parse results
        page_records = self._parse_docket_html(resp.text, code, label, cat)
        records.extend(page_records)

        # Step 4: Handle pagination
        page_num = 1
        while page_num < 30:
            next_soup = BeautifulSoup(resp.text, "lxml")
            next_link = next_soup.find("a", string=re.compile(r"Next|>|>>", re.I))
            if not next_link:
                break

            # Extract postback args for next page
            href = next_link.get("href","")
            postback_match = re.search(r"__doPostBack\('([^']+)','([^']*)'\)", href)
            if not postback_match:
                break

            form_data2 = {}
            for inp in next_soup.find_all("input"):
                n = inp.get("name","")
                v = inp.get("value","")
                if n:
                    form_data2[n] = v
            form_data2["__EVENTTARGET"]   = postback_match.group(1)
            form_data2["__EVENTARGUMENT"] = postback_match.group(2)

            try:
                resp = self.session.post(search_url, data=form_data2, timeout=30)
                resp.raise_for_status()
                new_recs = self._parse_docket_html(resp.text, code, label, cat)
                if not new_recs:
                    break
                records.extend(new_recs)
                page_num += 1
                time.sleep(0.5)
            except Exception as e:
                log.warning("Pagination error: %s", e)
                break

        return records

    # ------------------------------------------------------------------
    def _parse_docket_html(self, html: str, code: str,
                            label: str, cat: str) -> list[dict]:
        records = []
        soup = BeautifulSoup(html, "lxml")

        for table in soup.find_all("table"):
            rows = table.find_all("tr")
            if len(rows) < 2:
                continue

            headers = [th.get_text(strip=True).lower()
                       for th in rows[0].find_all(["th","td"])]

            # Must look like a case results table
            if not any(k in " ".join(headers)
                       for k in ["case","filed","party","plaintiff","defendant"]):
                continue

            for row in rows[1:]:
                cells = [c.get_text(strip=True) for c in row.find_all(["td","th"])]
                if len(cells) < 3:
                    continue

                def f(*keys):
                    for k in keys:
                        for i, h in enumerate(headers):
                            if k in h and i < len(cells):
                                return cells[i]
                    return ""

                link_tag = row.find("a", href=True)
                case_url = ""
                if link_tag:
                    href = link_tag.get("href","")
                    case_url = href if href.startswith("http") else f"{COURT_URL}/{href.lstrip('/')}"

                case_num   = f("case","number","no","cv")
                filed      = fmt_date(f("filed","date","file"))
                plaintiff  = f("plaintiff","lender","bank","party")
                defendant  = f("defendant","owner","debtor")
                address    = f("address","property","parcel")
                amount_str = f("amount","debt","balance","judgment")

                # Filter by date
                if filed:
                    try:
                        dt = datetime.strptime(filed, "%m/%d/%Y")
                        if dt < self.date_from:
                            continue
                    except Exception:
                        pass

                if not case_num and not filed:
                    continue

                records.append({
                    "doc_num":      case_num,
                    "doc_type":     code,
                    "cat":          cat,
                    "cat_label":    label,
                    "filed":        filed,
                    "owner":        defendant or "",
                    "grantee":      plaintiff or "",
                    "amount":       parse_amount(amount_str),
                    "legal":        "",
                    "clerk_url":    case_url,
                    "prop_address": address,
                    "prop_city":    "Cleveland",
                    "prop_state":   "OH",
                    "prop_zip":     "",
                    "mail_address": "",
                    "mail_city":    "",
                    "mail_state":   "OH",
                    "mail_zip":     "",
                    "source":       "Court Docket",
                })

        return records

    # ------------------------------------------------------------------
    async def _scrape_playwright(self):
        """Playwright fallback for the court docket."""
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(
                headless=True,
                args=["--no-sandbox","--disable-setuid-sandbox",
                      "--disable-dev-shm-usage","--disable-gpu"],
            )
            page = await (await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                           "AppleWebKit/537.36 Chrome/122.0 Safari/537.36",
                ignore_https_errors=True,
            )).new_page()

            for case_label, code, label, cat in CASE_TYPES:
                try:
                    await page.goto(
                        f"{COURT_URL}/Search.aspx",
                        wait_until="networkidle",
                        timeout=40_000,
                    )
                    await asyncio.sleep(2)

                    # Fill case type
                    for sel in ["#ddlCaseType","select[name*='CaseType']",
                                "select[name*='caseType']"]:
                        try:
                            await page.select_option(sel, label=case_label)
                            break
                        except Exception:
                            continue

                    # Fill date from
                    for sel in ["#txtFiledFrom","input[name*='FiledFrom']",
                                "input[name*='dateFrom']"]:
                        try:
                            await page.fill(sel, self.df_str)
                            break
                        except Exception:
                            continue

                    # Fill date to
                    for sel in ["#txtFiledTo","input[name*='FiledTo']",
                                "input[name*='dateTo']"]:
                        try:
                            await page.fill(sel, self.dt_str)
                            break
                        except Exception:
                            continue

                    # Click search
                    for sel in ["#btnSearch","input[value*='Search']",
                                "button:has-text('Search')"]:
                        try:
                            await page.click(sel)
                            break
                        except Exception:
                            continue

                    await page.wait_for_load_state("networkidle", timeout=25_000)
                    await asyncio.sleep(2)

                    content = await page.content()
                    recs = self._parse_docket_html(content, code, label, cat)
                    self.raw_records.extend(recs)
                    log.info("  PW %s -> %d", case_label, len(recs))

                except Exception as e:
                    log.warning("PW error for %s: %s", case_label, e)

            await browser.close()


# ===========================================================================
# Parcel Lookup
# ===========================================================================

class ParcelLookup:
    def __init__(self):
        self._by_owner = defaultdict(list)

    def load(self):
        session = requests.Session()
        session.headers.update({"User-Agent": "CuyahogaLeadScraper/1.0"})
        total = self._load_gis(session)
        if total > 0:
            log.info("Parcel index: %d owner entries", len(self._by_owner))
        else:
            log.warning("Parcel data unavailable — addresses will be skipped")

    def _load_gis(self, session) -> int:
        # Try multiple known Cuyahoga County GIS endpoints
        urls = [
            "https://gis.cuyahogacounty.gov/arcgis/rest/services/OpenData/Parcels/FeatureServer/0/query",
            "https://gis.cuyahogacounty.us/arcgis/rest/services/OpenData/Parcels/FeatureServer/0/query",
            "https://gis.cuyahogacounty.gov/arcgis/rest/services/Parcels/FeatureServer/0/query",
        ]
        offset, size, total = 0, 1000, 0
        working_url = None

        # Find which URL works
        for candidate in urls:
            try:
                test = session.get(candidate, params={"where":"1=1","outFields":"OWNER","f":"json","resultRecordCount":1,"returnGeometry":"false"}, timeout=15)
                if test.status_code == 200:
                    working_url = candidate
                    log.info("GIS URL found: %s", candidate)
                    break
            except Exception:
                continue

        if not working_url:
            log.warning("No working GIS URL found — skipping parcel enrichment")
            return 0

        url = working_url
        try:
            while True:
                params = {
                    "where":             "1=1",
                    "outFields":         ("OWNER,OWN1,SITEADDR,SITE_ADDR,SITE_CITY,"
                                          "SITE_ZIP,MAILADR1,ADDR_1,MAILCITY,CITY,"
                                          "STATE,MAILZIP,ZIP"),
                    "f":                 "json",
                    "resultOffset":      offset,
                    "resultRecordCount": size,
                    "returnGeometry":    "false",
                }
                r = session.get(url, params=params, timeout=60)
                r.raise_for_status()
                features = r.json().get("features", [])
                if not features:
                    break
                for feat in features:
                    self._ingest(feat.get("attributes", {}))
                    total += 1
                if len(features) < size:
                    break
                offset += size
                time.sleep(0.2)
        except Exception as e:
            log.warning("GIS API: %s", e)
        log.info("GIS parcels loaded: %d", total)
        return total

    def _ingest(self, r: dict):
        def g(*keys):
            for k in keys:
                for key in [k, k.upper(), k.lower()]:
                    v = r.get(key)
                    if v and str(v).strip() not in ("","None","null"):
                        return str(v).strip()
            return ""
        owner = g("OWNER","OWN1")
        if not owner:
            return
        rec = {
            "site_addr":  g("SITEADDR","SITE_ADDR"),
            "site_city":  g("SITE_CITY"),
            "site_zip":   g("SITE_ZIP"),
            "mail_addr":  g("MAILADR1","ADDR_1"),
            "mail_city":  g("MAILCITY","CITY"),
            "mail_state": g("STATE") or "OH",
            "mail_zip":   g("MAILZIP","ZIP"),
        }
        for v in name_variants(owner):
            self._by_owner[v].append(rec)

    def lookup(self, name: str) -> Optional[dict]:
        if not name:
            return None
        for v in name_variants(name):
            hits = self._by_owner.get(v)
            if hits:
                return hits[0]
        return None


# ===========================================================================
# Scoring
# ===========================================================================

class LeadScorer:
    WEEK_AGO = datetime.now() - timedelta(days=7)

    @staticmethod
    def score(rec: dict, all_recs: list) -> tuple:
        flags, points = [], 30
        cat   = rec.get("cat","")
        dtype = rec.get("doc_type","")
        owner = rec.get("owner","")
        amt   = rec.get("amount")

        if cat == "LP":      flags.append("Lis pendens");      points += 10
        if cat == "NOFC":    flags.append("Pre-foreclosure");  points += 10
        if cat == "TAXDEED": flags.append("Tax deed");         points += 10
        if cat == "JUD":     flags.append("Judgment lien");    points += 10
        if cat == "LIEN":
            if dtype in ("LNIRS","LNFED","LNCORPTX"):
                flags.append("Tax lien")
            elif dtype == "LNMECH":
                flags.append("Mechanic lien")
            elif dtype == "LNHOA":
                flags.append("HOA lien")
            else:
                flags.append("Lien")
            points += 10
        if cat == "PRO":     flags.append("Probate / estate"); points += 10
        if cat == "RELLP":   flags.append("Lis pendens");      points += 5

        norm = normalize_name(owner)
        if norm:
            owner_cats = {r["cat"] for r in all_recs
                          if normalize_name(r.get("owner","")) == norm}
            if "LP" in owner_cats and owner_cats & {"NOFC","TAXDEED"}:
                points += 20

        if amt:
            if amt > 100_000:
                flags.append("High debt (>$100k)"); points += 15
            elif amt > 50_000:
                points += 10

        try:
            dt = datetime.strptime(rec.get("filed","").strip(), "%m/%d/%Y")
            if dt >= LeadScorer.WEEK_AGO:
                flags.append("New this week"); points += 5
        except Exception:
            pass

        if rec.get("prop_address") or rec.get("mail_address"):
            flags.append("Address found"); points += 5

        if owner and re.search(r"\b(LLC|INC|CORP|LTD|TRUST|ESTATE)\b", owner, re.I):
            flags.append("LLC / corp owner"); points += 5

        src = rec.get("source","")
        if src == "Manual Upload":
            flags.append("Recorder filing"); points += 5

        return min(100, max(0, points)), list(dict.fromkeys(flags))


# ===========================================================================
# GHL CSV
# ===========================================================================

GHL_COLS = [
    "First Name","Last Name","Mailing Address","Mailing City","Mailing State","Mailing Zip",
    "Property Address","Property City","Property State","Property Zip",
    "Lead Type","Document Type","Date Filed","Document Number","Amount/Debt Owed",
    "Seller Score","Motivated Seller Flags","Source","Public Records URL",
]

def split_name(n):
    if not n: return "", ""
    if "," in n:
        p = [x.strip() for x in n.split(",",1)]
        return p[1].title(), p[0].title()
    p = n.split()
    return (p[0].title(), " ".join(p[1:]).title()) if len(p) > 1 else ("", p[0].title())

def export_csv(records: list, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=GHL_COLS)
        w.writeheader()
        for r in records:
            fn, ln = split_name(r.get("owner",""))
            amt = r.get("amount")
            w.writerow({
                "First Name":        fn,
                "Last Name":         ln,
                "Mailing Address":   r.get("mail_address",""),
                "Mailing City":      r.get("mail_city",""),
                "Mailing State":     r.get("mail_state","OH"),
                "Mailing Zip":       r.get("mail_zip",""),
                "Property Address":  r.get("prop_address",""),
                "Property City":     r.get("prop_city","Cleveland"),
                "Property State":    r.get("prop_state","OH"),
                "Property Zip":      r.get("prop_zip",""),
                "Lead Type":         r.get("cat_label",""),
                "Document Type":     r.get("doc_type",""),
                "Date Filed":        r.get("filed",""),
                "Document Number":   r.get("doc_num",""),
                "Amount/Debt Owed":  f"${amt:,.2f}" if amt else "",
                "Seller Score":      r.get("score",0),
                "Motivated Seller Flags": "; ".join(r.get("flags",[])),
                "Source":            r.get("source","Court Docket"),
                "Public Records URL":r.get("clerk_url",""),
            })
    log.info("GHL CSV: %d rows -> %s", len(records), path)


# ===========================================================================
# Main
# ===========================================================================

def save_json(data, *paths):
    for path in paths:
        path.parent.mkdir(parents=True, exist_ok=True)
        # Merge with any existing manually-uploaded records
        existing = []
        if path.exists():
            try:
                with open(path, "r", encoding="utf-8") as f:
                    old = json.load(f)
                    existing = [r for r in old.get("records", [])
                                if r.get("source") == "Manual Upload"]
                    if existing:
                        log.info("Preserving %d manually uploaded records from %s",
                                 len(existing), path.name)
            except Exception:
                pass

        # Merge: auto records + manual records
        all_records = data["records"] + existing

        # Deduplicate across both sources
        seen = {}
        for rec in all_records:
            key = rec.get("doc_num") or rec.get("clerk_url") or str(id(rec))
            if key not in seen or rec.get("score",0) > seen[key].get("score",0):
                seen[key] = rec
        merged = sorted(seen.values(), key=lambda r: r.get("score",0), reverse=True)

        output = {**data, "records": merged, "total": len(merged),
                  "with_address": sum(1 for r in merged
                                      if r.get("prop_address") or r.get("mail_address"))}
        with open(path, "w", encoding="utf-8") as f:
            json.dump(output, f, indent=2, default=str)
        log.info("Saved %s (%d records)", path, len(merged))


async def main():
    log.info("=" * 60)
    log.info("Cuyahoga County Lead Scraper — Court Docket")
    log.info("Range: %s -> %s",
             (datetime.now()-timedelta(days=LOOKBACK_DAYS)).strftime("%m/%d/%Y"),
             datetime.now().strftime("%m/%d/%Y"))
    log.info("=" * 60)

    # 1. Parcel data
    parcel = ParcelLookup()
    log.info("Loading parcel data ...")
    parcel.load()

    # 2. Scrape court docket
    scraper = CourtDocketScraper()
    await scraper.run()
    records = scraper.raw_records

    # 3. Enrich addresses from parcel data
    for rec in records:
        if not rec.get("prop_address"):
            match = parcel.lookup(rec.get("owner",""))
            if match:
                rec.update({
                    "prop_address": match["site_addr"],
                    "prop_city":    match["site_city"],
                    "prop_zip":     match["site_zip"],
                    "mail_address": match["mail_addr"],
                    "mail_city":    match["mail_city"],
                    "mail_state":   match["mail_state"],
                    "mail_zip":     match["mail_zip"],
                })

    # 4. Score
    for rec in records:
        rec["score"], rec["flags"] = LeadScorer.score(rec, records)

    # 5. Deduplicate
    seen = {}
    for rec in records:
        key = rec.get("doc_num") or rec.get("clerk_url") or str(id(rec))
        if key not in seen or rec["score"] > seen[key]["score"]:
            seen[key] = rec
    records = sorted(seen.values(), key=lambda r: r["score"], reverse=True)
    log.info("Unique auto records: %d", len(records))

    # 6. Save (merges with any existing manual uploads)
    with_addr = sum(1 for r in records if r.get("prop_address") or r.get("mail_address"))
    output = {
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "source":     "Cuyahoga County Court Docket + Manual Uploads",
        "date_range": {
            "from": (datetime.now()-timedelta(days=LOOKBACK_DAYS)).strftime("%m/%d/%Y"),
            "to":   datetime.now().strftime("%m/%d/%Y"),
        },
        "total":        len(records),
        "with_address": with_addr,
        "records":      records,
    }
    save_json(output, DASHBOARD_JSON, DATA_JSON)
    export_csv(records, GHL_CSV)

    log.info("DONE: %d leads | %d with address | top score: %s",
             len(records), with_addr,
             records[0]["score"] if records else "N/A")


if __name__ == "__main__":
    asyncio.run(main())
