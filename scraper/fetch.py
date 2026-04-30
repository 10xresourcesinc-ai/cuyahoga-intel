"""
Cuyahoga County Motivated Seller Lead Scraper
Sources:
  NOFC    — Cuyahoga Common Pleas Court sheriff sale docket
  CODE    — Cleveland ArcGIS code violations
  CONDEMN — Cleveland ArcGIS condemnations
  PRO     — Cuyahoga County Probate Court estate filings
  BK      — Northern District of Ohio bankruptcy filings (CourtListener API)
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
from datetime import datetime, timedelta, timezone, date
from pathlib import Path
from typing import Optional

import requests
from bs4 import BeautifulSoup

try:
    from playwright.async_api import async_playwright
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

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
SHERIFF_SALE_URL = f"{COURT_URL}/SheriffSearch/search.aspx"
LOOKBACK_DAYS    = int(os.getenv("LOOKBACK_DAYS", "30"))
REPO_ROOT        = Path(__file__).resolve().parent.parent
DASHBOARD_JSON   = REPO_ROOT / "dashboard" / "records.json"
DATA_JSON        = REPO_ROOT / "data" / "records.json"
GHL_CSV          = REPO_ROOT / "data" / "ghl_export.csv"
ACCELA_CACHE_FILE = REPO_ROOT / "data" / "accela_cache.json"

CLEVELAND_ORG     = "https://services3.arcgis.com/dty2kHktVXHrqO8i/ArcGIS/rest/services"
VIOLATIONS_URL    = f"{CLEVELAND_ORG}/Complaint_Violation_Notices/FeatureServer/0/query"
CONDEMNATIONS_URL = f"{CLEVELAND_ORG}/Current_Condemnations/FeatureServer/0/query"
VIOLATIONS_META   = f"{CLEVELAND_ORG}/Complaint_Violation_Notices/FeatureServer/0"

# Cuyahoga County Treasurer — delinquent tax parcel layer (public ArcGIS)
TREASURER_DELINQ_URL = (
    "https://gis.cuyahogacounty.us/server/rest/services/"
    "Treasurer/DelinquentTaxParcels/MapServer/0/query"
)

PROBATE_BASE   = "https://probate.cuyahogacounty.gov/pa"
PROBATE_TOS    = f"{PROBATE_BASE}/TOS.aspx"
PROBATE_SEARCH = f"{PROBATE_BASE}/CaseSearch.aspx"


# ===========================================================================
# Shared helpers
# ===========================================================================

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

def format_parcel(parcel: str) -> str:
    if not parcel:
        return parcel
    p = parcel.replace("-", "").strip()
    if len(p) in (8, 9):
        return f"{p[:3]}-{p[3:5]}-{p[5:]}"
    return parcel


# ===========================================================================
# Sheriff Sale Scraper  (NOFC)
# ===========================================================================

class SheriffScraper:

    def __init__(self):
        self.date_to   = datetime.now()
        self.date_from = self.date_to - timedelta(days=LOOKBACK_DAYS)
        self.df_str    = self.date_from.strftime("%m/%d/%Y")
        self.dt_str    = self.date_to.strftime("%m/%d/%Y")
        self.session   = requests.Session()
        self.session.verify = False
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                          "AppleWebKit/537.36 Chrome/122.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,*/*",
        })
        self.raw_records: list[dict] = []

    async def run(self):
        log.info("Scraping Cuyahoga Common Pleas Court docket ...")
        log.info("Date range: %s - %s", self.df_str, self.dt_str)
        self._scrape_sheriff()
        if not self.raw_records and PLAYWRIGHT_AVAILABLE:
            log.info("Trying Playwright fallback ...")
            await self._playwright_scrape()
        log.info("Court docket: %d records collected", len(self.raw_records))

    def _scrape_sheriff(self):
        log.info("Trying Sheriff Sale search ...")
        try:
            r = self.session.get(SHERIFF_SALE_URL, timeout=30)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "lxml")

            form_data = {}
            for inp in soup.find_all("input"):
                n = inp.get("name", "")
                v = inp.get("value", "")
                if n:
                    form_data[n] = v

            for k in list(form_data.keys()):
                kl = k.lower()
                if any(x in kl for x in ["datefrom", "startdate", "fromdate"]):
                    form_data[k] = self.df_str
                elif any(x in kl for x in ["dateto", "enddate", "todate"]):
                    form_data[k] = self.dt_str

            for fname in ["dateFrom", "dateTo", "txtDateFrom", "txtDateTo",
                          "StartDate", "EndDate", "dfrom", "dto"]:
                kl = fname.lower()
                if any(x in kl for x in ["from", "start"]):
                    form_data[fname] = self.df_str
                else:
                    form_data[fname] = self.dt_str

            form_data["__EVENTTARGET"]   = ""
            form_data["__EVENTARGUMENT"] = ""

            for inp in soup.find_all("input", type="submit"):
                form_data[inp.get("name", "btnSearch")] = inp.get("value", "Search")
                break

            resp = self.session.post(SHERIFF_SALE_URL, data=form_data, timeout=30)
            resp.raise_for_status()
            recs = self._parse_html(resp.text)
            self.raw_records.extend(recs)
            log.info("Sheriff sales: %d records", len(recs))

        except Exception as e:
            log.warning("Sheriff sale search failed: %s", e)

    def _parse_html(self, html: str) -> list[dict]:
        records = []
        soup = BeautifulSoup(html, "lxml")
        blocks = []
        for table in soup.find_all("table"):
            for row in table.find_all("tr"):
                text = row.get_text(" ", strip=True)
                if re.search(r'CV\s*\d{4,}', text, re.I) and len(text) > 50:
                    blocks.append((text, row))

        log.info("Sheriff: found %d property blocks", len(blocks))
        if blocks:
            log.info("SAMPLE BLOCK: %s", blocks[0][0][:500])

        for text, row in blocks:
            try:
                rec = self._extract(text, row)
                if rec:
                    records.append(rec)
            except Exception as e:
                log.debug("Extract error: %s", e)
        return records

    def _extract(self, text: str, row) -> Optional[dict]:
        case_m = re.search(r'Case\s*#\s*:?\s*(CV\s*\d+)', text, re.I)
        if not case_m:
            case_m = re.search(r'\b(CV\d{4,})\b', text, re.I)
        if not case_m:
            return None
        case_num = case_m.group(1).replace(" ", "").upper()

        att_m = re.search(
            r'Attorney\s*:\s*([A-Z][A-Z\s,\.]+?)(?=\s+Appraised|\s+Case\s*#|\s+\$)',
            text, re.I
        )
        plaintiff = att_m.group(1).strip().title() if att_m else ""

        date_m = re.search(r'Sale\s+Date\s+(\d{1,2}/\d{1,2}/\d{4})', text, re.I)
        if not date_m:
            date_m = re.search(r'\b(\d{1,2}/\d{1,2}/\d{4})\b', text)
        sale_date = date_m.group(1) if date_m else ""

        owner_m = re.search(
            r'VS\s+Sale\s+Date\s+\d{1,2}/\d{1,2}/\d{4}\s+([A-Z][A-Z/\s,\.\-]{2,60}?)\s+Status',
            text, re.I
        )
        if not owner_m:
            owner_m = re.search(r'\bVS\b\s+([A-Z][A-Z/\s,\.\-]{2,60}?)\s+Status', text, re.I)
        owner = ""
        if owner_m:
            raw = owner_m.group(1).strip()
            parts = [p.strip() for p in re.split(r'[/,]', raw) if p.strip()]
            owner = " ".join(p.title() for p in parts)

        parcel_m = re.search(r'\b(\d{3}-\d{2}-\d{3})\b', text)
        parcel = parcel_m.group(1) if parcel_m else ""

        prop_address = ""
        if parcel:
            addr_m = re.search(
                re.escape(parcel) + r'\s+(\d{1,5}\s+[A-Z][A-Z0-9\s]{3,50})',
                text, re.I
            )
            if addr_m:
                addr_raw = addr_m.group(1)
                addr_raw = re.sub(
                    r'\s+(A\s+SINGLE|DWELLING|COMMERCIAL|VACANT|LOT|WITH\s+).*$',
                    '', addr_raw, flags=re.I
                )
                prop_address = addr_raw.strip().title()

        if not prop_address:
            addr_m = re.search(
                r'\b(\d{3,5}\s+[A-Z][A-Z\s]{3,40}'
                r'(?:STREET|ST|AVENUE|AVE|DRIVE|DR|ROAD|RD|BOULEVARD|BLVD|'
                r'LANE|LN|COURT|CT|PLACE|PL|WAY|CIRCLE|CIR))\b',
                text, re.I
            )
            if addr_m:
                prop_address = addr_m.group(1).strip().title()

        zip_m = re.search(r'\b(44\d{3})\b', text)
        prop_zip = zip_m.group(1) if zip_m else ""

        amt_m = re.search(r'Appraised\s+\$?([\d,]+(?:\.\d{2})?)', text, re.I)
        if not amt_m:
            amt_m = re.search(r'Minimum\s+Bid\s+\$?([\d,]+)', text, re.I)
        amount = parse_amount(amt_m.group(1)) if amt_m else None

        link_tag = row.find("a", href=True)
        href = link_tag["href"] if link_tag else ""
        if href and not href.startswith("http"):
            href = f"{COURT_URL}/{href.lstrip('/')}"
        if not href:
            href = (f"{COURT_URL}/Search.aspx?"
                    f"q=searchType%3DCase+Number%26searchString%3D{case_num}")

        return {
            "doc_num":      case_num,
            "doc_type":     "NOFC",
            "cat":          "NOFC",
            "cat_label":    "Notice of Foreclosure",
            "filed":        sale_date,
            "owner":        owner,
            "grantee":      plaintiff,
            "amount":       amount,
            "legal":        parcel,
            "clerk_url":    href,
            "prop_address": prop_address,
            "prop_city":    "Cleveland",
            "prop_state":   "OH",
            "prop_zip":     prop_zip,
            "mail_address": "",
            "mail_city":    "",
            "mail_state":   "OH",
            "mail_zip":     "",
            "source":       "Court Docket",
            "neighborhood": "",
            "viol_status":  "",
        }

    async def _playwright_scrape(self):
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-setuid-sandbox",
                      "--disable-dev-shm-usage", "--disable-gpu"],
            )
            page = await (await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                           "AppleWebKit/537.36 Chrome/122.0 Safari/537.36",
                ignore_https_errors=True,
            )).new_page()
            try:
                await page.goto(SHERIFF_SALE_URL, wait_until="networkidle", timeout=40_000)
                await asyncio.sleep(2)
                for sel in ["input[name*='dateFrom']", "input[name*='StartDate']", "#dateFrom"]:
                    try:
                        await page.fill(sel, self.df_str); break
                    except Exception:
                        continue
                for sel in ["input[name*='dateTo']", "input[name*='EndDate']", "#dateTo"]:
                    try:
                        await page.fill(sel, self.dt_str); break
                    except Exception:
                        continue
                for sel in ["input[type='submit']", "button:has-text('Search')"]:
                    try:
                        await page.click(sel)
                        await page.wait_for_load_state("networkidle", timeout=25_000)
                        break
                    except Exception:
                        continue
                await asyncio.sleep(2)
                content = await page.content()
                recs = self._parse_html(content)
                self.raw_records.extend(recs)
                log.info("Playwright sheriff: %d records", len(recs))
            except Exception as e:
                log.warning("Playwright error: %s", e)
            finally:
                await browser.close()


# ===========================================================================
# Code Violation + Condemnation Scraper  (CODE / CONDEMN)
# ===========================================================================

VIOL_TYPE_MAP = {
    "TALL GRASS":      ("CODE_GRASS",   "Tall Grass / Weeds"),
    "WEED":            ("CODE_GRASS",   "Tall Grass / Weeds"),
    "OVERGROWN":       ("CODE_GRASS",   "Tall Grass / Weeds"),
    "VEGETATION":      ("CODE_GRASS",   "Tall Grass / Weeds"),
    "GARBAGE":         ("CODE_DEBRIS",  "Garbage / Debris"),
    "DEBRIS":          ("CODE_DEBRIS",  "Garbage / Debris"),
    "DUMP":            ("CODE_DEBRIS",  "Garbage / Debris"),
    "TRASH":           ("CODE_DEBRIS",  "Garbage / Debris"),
    "LITTER":          ("CODE_DEBRIS",  "Garbage / Debris"),
    "JUNK":            ("CODE_DEBRIS",  "Garbage / Debris"),
    "STRUCTURAL":      ("CODE_STRUCT",  "Structural Issue"),
    "EXTERIOR":        ("CODE_STRUCT",  "Structural Issue"),
    "FOUNDATION":      ("CODE_STRUCT",  "Structural Issue"),
    "ROOF":            ("CODE_STRUCT",  "Structural Issue"),
    "WALL":            ("CODE_STRUCT",  "Structural Issue"),
    "WINDOW":          ("CODE_STRUCT",  "Structural Issue"),
    "DOOR":            ("CODE_STRUCT",  "Structural Issue"),
    "VACANT":          ("CODE_STRUCT",  "Vacant / Unsecured"),
    "UNSECURED":       ("CODE_STRUCT",  "Vacant / Unsecured"),
    "OPEN AND VACANT": ("CODE_STRUCT",  "Vacant / Unsecured"),
    "NUISANCE":        ("CODE_NUISANCE","Property Nuisance"),
    "INOPERABLE":      ("CODE_NUISANCE","Inoperable Vehicle"),
    "VEHICLE":         ("CODE_NUISANCE","Inoperable Vehicle"),
    "ELECTRICAL":      ("CODE_MECH",    "Electrical Issue"),
    "PLUMBING":        ("CODE_MECH",    "Plumbing Issue"),
    "MECHANICAL":      ("CODE_MECH",    "Mechanical Issue"),
    "HVAC":            ("CODE_MECH",    "Mechanical Issue"),
    "RODENT":          ("CODE_PEST",    "Rodent / Pest"),
    "PEST":            ("CODE_PEST",    "Rodent / Pest"),
    "RAT":             ("CODE_PEST",    "Rodent / Pest"),
    "INSECT":          ("CODE_PEST",    "Rodent / Pest"),
}

def classify_violation(description: str) -> tuple:
    upper = (description or "").upper()
    for keyword, (cat, label) in VIOL_TYPE_MAP.items():
        if keyword in upper:
            return cat, label
    return "CODE", "Code Violation"


class CodeViolationScraper:

    def __init__(self):
        self.session = requests.Session()
        self.session.verify = False
        self.session.headers.update({"User-Agent": "CuyahogaLeadScraper/1.0"})
        self.raw_records: list[dict] = []
        self._date_field:   Optional[str] = None
        self._type_field:   Optional[str] = None
        self._avail_fields: list[str]     = []

    def run(self):
        log.info("Scraping Cleveland code violations ...")
        self._discover_schema()
        self._scrape_violations()
        self._scrape_condemnations()
        log.info("Code violations + condemnations: %d records", len(self.raw_records))

    def _discover_schema(self):
        try:
            r = self.session.get(VIOLATIONS_META, params={"f": "json"}, timeout=15)
            r.raise_for_status()
            fields = r.json().get("fields", [])
            self._avail_fields = [f["name"] for f in fields]
            log.info("ArcGIS violation layer fields: %s", self._avail_fields)
            for candidate in ["FILE_DATE", "COMPLAINT_DATE", "DATE_FILED",
                               "OPEN_DATE", "CREATEDATE"]:
                if candidate in self._avail_fields:
                    self._date_field = candidate
                    log.info("Using date field: %s", self._date_field)
                    break
            for candidate in ["COMPLAINT_TYPE", "VIOLATION_TYPE", "VIOLATION_DESC",
                               "DESCRIPTION", "RECORD_TYPE", "TYPE_DESC", "CASE_TYPE",
                               "COMPLAINT_DESC", "WORK_DESCRIPTION", "NOTICE_TYPE"]:
                if candidate in self._avail_fields:
                    self._type_field = candidate
                    log.info("Using violation type field: %s", self._type_field)
                    break
        except Exception as e:
            log.warning("Schema discovery failed: %s — using defaults", e)
        if not self._date_field:
            self._date_field = "FILE_DATE"
        if not self._type_field:
            self._type_field = "COMPLAINT_TYPE"

    def _scrape_violations(self):
        cutoff_dt  = datetime.now() - timedelta(days=LOOKBACK_DAYS)
        cutoff_sql = cutoff_dt.strftime("%Y-%m-%d")
        desired = [
            "RECORD_ID", "VIOLATION_NUMBER", "FILE_DATE",
            "PARCEL_NUMBER", "PRIMARY_ADDRESS",
            "VIOLATION_APP_STATUS",
            "DW_Neighborhood",
            "COMPLAINT_ACCELA_CITIZEN_ACCESS_URL",
            "VIOLATION_ACCELA_CITIZEN_ACCESS_URL",
        ]
        features = []
        for where_clause in [
            f"FILE_DATE >= DATE '{cutoff_sql}'",
            f"FILE_DATE >= '{cutoff_sql}'",
            "1=1",
        ]:
            try:
                params = {
                    "where":             where_clause,
                    "outFields":         ",".join(desired),
                    "f":                 "json",
                    "resultRecordCount": 2000,
                    "returnGeometry":    "false",
                    "orderByFields":     f"{self._date_field} DESC",
                }
                r = self.session.get(VIOLATIONS_URL, params=params, timeout=30)
                r.raise_for_status()
                data = r.json()
                if "error" in data:
                    log.warning("ArcGIS error with where='%s': %s", where_clause, data["error"])
                    continue
                features = data.get("features", [])
                log.info("WHERE '%s' → %d features", where_clause, len(features))
                if features:
                    log.info("SAMPLE violation record: %s", features[0].get("attributes", {}))
                    break
            except Exception as e:
                log.warning("Violation query failed (where='%s'): %s", where_clause, e)
                continue

        log.info("Code violations fetched: %d records", len(features))
        for feat in features:
            rec = self._violation_to_record(feat.get("attributes", {}))
            if rec:
                self.raw_records.append(rec)

    def _scrape_condemnations(self):
        try:
            params = {
                "where":             "Active_Condemnation='Yes'",
                "outFields":         "Parcel_Number,Address,Condemnation_Date,DW_Neighborhood",
                "f":                 "json",
                "resultRecordCount": 2000,
                "returnGeometry":    "false",
                "orderByFields":     "Condemnation_Date DESC",
            }
            r = self.session.get(CONDEMNATIONS_URL, params=params, timeout=30)
            r.raise_for_status()
            features = r.json().get("features", [])
            log.info("Condemnations fetched: %d records", len(features))
            for feat in features:
                rec = self._condemnation_to_record(feat.get("attributes", {}))
                if rec:
                    self.raw_records.append(rec)
        except Exception as e:
            log.warning("Condemnation scrape failed: %s", e)

    def _violation_to_record(self, attrs: dict) -> Optional[dict]:
        parcel  = format_parcel((attrs.get("PARCEL_NUMBER") or "").strip())
        address = (attrs.get("PRIMARY_ADDRESS") or "").strip().title()
        rec_id  = attrs.get("RECORD_ID") or attrs.get("VIOLATION_NUMBER") or ""
        status  = attrs.get("VIOLATION_APP_STATUS") or ""
        nbhd    = attrs.get("DW_Neighborhood") or ""
        url     = (attrs.get("VIOLATION_ACCELA_CITIZEN_ACCESS_URL")
                   or attrs.get("COMPLAINT_ACCELA_CITIZEN_ACCESS_URL") or "")

        SKIP_STATUSES = {
            "closed", "void", "closed/non-compliant",
            "closed-referred to aging", "structure razed",
            "demolition approved", "approved for demo",
        }
        if status.lower().strip() in SKIP_STATUSES:
            return None

        STATUS_MAP = {
            "chief approved":                ("CODE_STRUCT", "Substandard Structure",       "high"),
            "placard post/photo":            ("CODE_STRUCT", "Substandard Structure",       "high"),
            "court judgment":                ("CODE_STRUCT", "Substandard Structure",       "high"),
            "court stat":                    ("CODE_STRUCT", "Substandard Structure",       "high"),
            "court date set":                ("CODE_STRUCT", "Substandard Structure",       "high"),
            "search warrant executed":       ("CODE_STRUCT", "Substandard Structure",       "high"),
            "summons approved":              ("CODE_STRUCT", "Substandard Structure",       "high"),
            "prosecution packet to chief":   ("CODE_STRUCT", "Substandard Structure",       "high"),
            "prosecution pkt created":       ("CODE_STRUCT", "Substandard Structure",       "high"),
            "s/w packet sent to chief":      ("CODE_STRUCT", "Substandard Structure",       "high"),
            "s/w packet sent to legal":      ("CODE_STRUCT", "Substandard Structure",       "high"),
            "paralegal to law":              ("CODE_STRUCT", "Substandard Structure",       "high"),
            "packet sent to legal":          ("CODE_STRUCT", "Substandard Structure",       "high"),
            "filed w/clerk of courts":       ("CODE_STRUCT", "Substandard Structure",       "high"),
            "lockbox processed":             ("CODE_STRUCT", "Substandard Structure",       "high"),
            "boza denied":                   ("CODE_STRUCT", "Substandard Structure",       "high"),
            "vn created & mailed":           ("CODE",        "Code Violation",              "medium"),
            "open":                          ("CODE",        "Code Violation",              "medium"),
            "pending":                       ("CODE",        "Code Violation",              "medium"),
            "awaiting reinspection":         ("CODE",        "Code Violation",              "medium"),
            "post&photo (p)":                ("CODE",        "Code Violation",              "medium"),
            "demo file to chief":            ("CODE_STRUCT", "Substandard Structure",       "medium"),
            "demo packet to legal":          ("CODE_STRUCT", "Substandard Structure",       "medium"),
            "packet requested from chief":   ("CODE_STRUCT", "Substandard Structure",       "medium"),
            "packet to chief for revision":  ("CODE_STRUCT", "Substandard Structure",       "medium"),
            "packet to paralegal from chief":("CODE_STRUCT", "Substandard Structure",       "medium"),
            "packet to chief for inspector": ("CODE_STRUCT", "Substandard Structure",       "medium"),
            "to inspector for revision":     ("CODE_STRUCT", "Substandard Structure",       "medium"),
            "post&photo (nc)":               ("CODE_STRUCT", "Substandard Structure",       "medium"),
            "post&photo (c)":                ("CODE_STRUCT", "Substandard Structure",       "medium"),
            "bbs appeal":                    ("CODE",        "Code Violation",              "medium"),
            "bbs/boza appeal":               ("CODE",        "Code Violation",              "medium"),
            "boza dismissed":                ("CODE",        "Code Violation",              "medium"),
        }
        s_lower = status.lower().strip()
        if s_lower in STATUS_MAP:
            cat, cat_label, viol_severity = STATUS_MAP[s_lower]
        else:
            cat, cat_label, viol_severity = "CODE", "Code Violation", "low"
        viol_desc = status

        filed = ""
        raw_date = attrs.get("FILE_DATE")
        if raw_date:
            try:
                val = int(raw_date)
                if val > 1_000_000_000_000:
                    filed = datetime.fromtimestamp(val / 1000).strftime("%m/%d/%Y")
                elif val > 10_000:
                    filed = datetime.fromtimestamp(val * 86400).strftime("%m/%d/%Y")
            except Exception:
                pass

        if not address and not parcel:
            return None

        zip_m = re.search(r'\b(44\d{3})\b', address)
        prop_zip = zip_m.group(1) if zip_m else ""

        return {
            "doc_num":      rec_id,
            "doc_type":     cat,
            "cat":          "CODE",
            "cat_label":    cat_label,
            "filed":        filed,
            "owner":        "",
            "grantee":      "",
            "amount":       None,
            "legal":        parcel,
            "clerk_url":    url,
            "prop_address": address,
            "prop_city":    "Cleveland",
            "prop_state":   "OH",
            "prop_zip":     prop_zip,
            "mail_address": "",
            "mail_city":    "",
            "mail_state":   "OH",
            "mail_zip":     "",
            "source":       "Cleveland Code Enforcement",
            "neighborhood": nbhd,
            "viol_status":  status,
            "viol_desc":    viol_desc,
            "viol_severity": viol_severity,
        }

    def _condemnation_to_record(self, attrs: dict) -> Optional[dict]:
        parcel  = format_parcel((attrs.get("Parcel_Number") or "").strip())
        address = (attrs.get("Address") or "").strip().title()
        nbhd    = attrs.get("DW_Neighborhood") or ""

        filed = ""
        ms = attrs.get("Condemnation_Date")
        if ms:
            try:
                filed = datetime.fromtimestamp(ms / 1000).strftime("%m/%d/%Y")
            except Exception:
                pass

        if not address and not parcel:
            return None

        prop_addr_clean = address
        prop_city = "Cleveland"
        if "," in address:
            parts = [p.strip() for p in address.split(",")]
            prop_addr_clean = parts[0]
            if len(parts) > 1:
                prop_city = parts[1].title()

        zip_m = re.search(r'\b(44\d{3})\b', address)
        prop_zip = zip_m.group(1) if zip_m else ""

        return {
            "doc_num":      f"CONDEMN-{parcel or address[:20].replace(' ', '-')}",
            "doc_type":     "CONDEMN",
            "cat":          "CONDEMN",
            "cat_label":    "Condemned Property",
            "filed":        filed,
            "owner":        "",
            "grantee":      "",
            "amount":       None,
            "legal":        parcel,
            "clerk_url":    "https://aca-prod.accela.com/COC/",
            "prop_address": prop_addr_clean,
            "prop_city":    prop_city,
            "prop_state":   "OH",
            "prop_zip":     prop_zip,
            "mail_address": "",
            "mail_city":    "",
            "mail_state":   "OH",
            "mail_zip":     "",
            "source":       "Cleveland Code Enforcement",
            "neighborhood": nbhd,
            "viol_status":  "Condemned",
            "viol_desc":    "",
            "viol_severity": "high",
        }


# ===========================================================================
# Probate Court Scraper  (PRO)  — Phase 3
# ===========================================================================

class ProbateScraper:
    """
    Scrapes estate filings from the Cuyahoga County Probate Court public portal.
    Platform: PROWARE 2.6 (ASP.NET WebForms)

    Strategy:
      1. GET /pa/TOS.aspx  → accept terms → get session cookie
      2. GET /pa/CaseSearch.aspx → scrape ViewState tokens
      3. POST party-name search with category=ES (Estate) + filing date window
      4. Parse results table → yield one PRO record per estate filing
    """

    def __init__(self):
        self.raw_records: list[dict] = []
        self._session: Optional[requests.Session] = None

    def run(self):
        log.info("Scraping Cuyahoga Probate Court estate filings ...")
        try:
            self._session = self._make_session()
            self._search_estates()
        except Exception as e:
            log.warning("Probate scraper failed: %s", e)
        log.info("Probate estates: %d records", len(self.raw_records))

    # ------------------------------------------------------------------
    # Session / ToS
    # ------------------------------------------------------------------

    def _make_session(self) -> requests.Session:
        session = requests.Session()
        session.verify = True
        session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 Chrome/124.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,*/*",
        })

        # Step 1 — GET TOS page for ViewState tokens
        resp = session.get(PROBATE_TOS, timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        vs = self._get_viewstate(soup)

        if not vs.get("__VIEWSTATE"):
            raise RuntimeError("No ViewState on TOS page — site layout may have changed.")

        # Step 2 — POST to accept TOS
        accept_btn = soup.find("input", {"type": "submit"})
        btn_name   = accept_btn["name"]  if accept_btn else "btnAccept"
        btn_value  = accept_btn["value"] if accept_btn else "I Accept"

        payload = {
            **vs,
            "__EVENTTARGET":   "",
            "__EVENTARGUMENT": "",
            btn_name:          btn_value,
        }
        resp = session.post(PROBATE_TOS, data=payload, timeout=30)
        resp.raise_for_status()
        log.info("Probate TOS accepted.")
        return session

    # ------------------------------------------------------------------
    # Search
    # ------------------------------------------------------------------

    def _search_estates(self):
        date_to   = date.today()
        date_from = date_to - timedelta(days=LOOKBACK_DAYS)
        log.info("Probate search window: %s → %s", date_from, date_to)

        # GET CaseSearch page for fresh ViewState
        resp = self._session.get(PROBATE_SEARCH, timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        payload = self._build_payload(soup, date_from, date_to)
        time.sleep(1)

        resp = self._session.post(PROBATE_SEARCH, data=payload, timeout=45)
        resp.raise_for_status()
        result_soup = BeautifulSoup(resp.text, "html.parser")

        # Debug: log page title and first 300 chars so we can verify it's a results page
        title = result_soup.find("title")
        log.info("Probate POST response — title: %s | preview: %s",
                 title.get_text(strip=True) if title else "none",
                 result_soup.get_text(" ", strip=True)[:300])

        rows = self._parse_results(result_soup)
        log.info("Probate results: %d estate filings", len(rows))

        for row in rows:
            rec = self._row_to_record(row)
            if rec:
                self.raw_records.append(rec)

    def _build_payload(self, soup: BeautifulSoup,
                       date_from: date, date_to: date) -> dict:
        vs = self._get_viewstate(soup)

        # Confirmed prefix from DevTools: input#mpContentPH_btnSearchByCase
        # → name attribute uses "mpContentPH$" prefix
        prefix = "mpContentPH$"

        fmt = lambda d: d.strftime("%m/%d/%Y")

        # The form has two sections:
        #   "Search by Case"  → btnSearchByCase
        #   "Search by Party" → btnSearchByParty
        # We use Party search with blank names + date range to get all estate filings.
        # Case Category dropdown for Party section is ddlCaseCategoryParty (separate from
        # the Case section's ddlCaseCategory).
        # PROWARE requires __EVENTTARGET set to the button's full dotted control path.
        # The button name is "mpContentPH$btnSearchByParty" so the event target
        # uses dollar-sign → dot notation: "mpContentPH.btnSearchByParty"
        # We also need to include ALL form fields (Case section too) to avoid
        # the server ignoring the POST and re-rendering the default page.
        btn_event_target = f"{prefix}btnSearchByParty".replace("$", "$")

        return {
            **vs,
            "__EVENTTARGET":                            f"{prefix}btnSearchByParty",
            "__EVENTARGUMENT":                          "",
            # Case search section (must be present even though we use Party search)
            f"{prefix}txtCaseYear":                     "",
            f"{prefix}ddlCaseCategory":                 "",
            f"{prefix}txtCaseNumber":                   "",
            # Party search fields
            f"{prefix}rblPartyType":                    "P",      # P=Person, C=Company
            f"{prefix}txtFirstName":                    "",       # blank = wildcard
            f"{prefix}txtMiddleName":                   "",
            f"{prefix}txtLastName":                     "",
            f"{prefix}ddlSuffix":                       "",
            f"{prefix}ddlPartyRole":                    "",
            f"{prefix}txtCaseYearParty":                "",
            f"{prefix}ddlCaseCategoryParty":            "ES",     # ES = Estate
            f"{prefix}txtFilingDateFrom":               fmt(date_from),
            f"{prefix}txtFilingDateTo":                 fmt(date_to),
            # Do NOT include btnSearchByParty as a form value when using __EVENTTARGET
        }

    # ------------------------------------------------------------------
    # Parsing
    # ------------------------------------------------------------------

    def _parse_results(self, soup: BeautifulSoup) -> list[dict]:
        rows = []

        # Try specific IDs first, then class patterns, then any data table
        table = (
            soup.find("table", id=re.compile(r"gvResults|GridView|grdResults|mpContentPH|SearchResult", re.I))
            or soup.find("table", class_=re.compile(r"grid|result|case|SearchResult", re.I))
        )
        if not table:
            # Last resort: pick the largest table with a header-like first row
            best, best_rows = None, 0
            for t in soup.find_all("table"):
                trs = t.find_all("tr")
                if len(trs) > best_rows and t.find("td"):
                    best, best_rows = t, len(trs)
            table = best

        if not table or len(table.find_all("tr")) < 2:
            preview = soup.get_text(" ", strip=True)[:400]
            log.warning("Probate: no results table found. Page preview: %s", preview)
            return rows

        tr_list = table.find_all("tr")
        if len(tr_list) < 2:
            return rows

        headers = [th.get_text(strip=True).lower()
                   for th in tr_list[0].find_all(["th", "td"])]
        idx = {
            "case_no":   self._col(headers, ["case number", "case no", "casenumber"]),
            "name":      self._col(headers, ["name", "decedent", "party"]),
            "case_type": self._col(headers, ["case type", "type"]),
            "filed":     self._col(headers, ["filed", "filing date", "date"]),
            "status":    self._col(headers, ["status"]),
        }

        for tr in tr_list[1:]:
            cells = tr.find_all("td")
            if not cells:
                continue

            def cell(key):
                i = idx.get(key)
                return cells[i].get_text(strip=True) if i is not None and i < len(cells) else ""

            link_tag = tr.find("a", href=True)
            detail_href = link_tag["href"] if link_tag else ""
            if detail_href and not detail_href.startswith("http"):
                detail_href = f"{PROBATE_BASE}/{detail_href.lstrip('/')}"

            rows.append({
                "case_no":    cell("case_no"),
                "name":       cell("name"),
                "case_type":  cell("case_type"),
                "filed":      cell("filed"),
                "status":     cell("status"),
                "detail_url": detail_href,
            })
        return rows

    def _row_to_record(self, row: dict) -> Optional[dict]:
        raw_name = row["name"].strip()
        if not raw_name:
            return None

        # Normalise "SMITH, JOHN WILLIAM" → last / first
        if "," in raw_name:
            last, _, first = raw_name.partition(",")
        else:
            parts = raw_name.split()
            last  = parts[-1] if parts else raw_name
            first = " ".join(parts[:-1])

        last  = last.strip().upper()
        first = first.strip().upper()

        # Use decedent name as owner proxy for parcel enrichment
        owner = f"{last}, {first}" if first else last

        return {
            "doc_num":      row["case_no"] or f"PRO-{raw_name[:20]}",
            "doc_type":     "PRO",
            "cat":          "PRO",
            "cat_label":    "Probate / Estate",
            "filed":        row["filed"],
            "owner":        owner,          # decedent name → MyPlace owner search
            "grantee":      "",
            "amount":       None,
            "legal":        "",             # filled by parcel enrichment
            "clerk_url":    row["detail_url"] or PROBATE_SEARCH,
            "prop_address": "",
            "prop_city":    "Cleveland",
            "prop_state":   "OH",
            "prop_zip":     "",
            "mail_address": "",
            "mail_city":    "",
            "mail_state":   "OH",
            "mail_zip":     "",
            "source":       "Cuyahoga Probate Court",
            "neighborhood": "",
            "viol_status":  row["status"],
            "viol_desc":    "",
            "viol_severity": "",
            # Extra PRO fields (visible in dashboard / CSV)
            "pro_case_no":  row["case_no"],
            "pro_decedent": raw_name,
            "pro_status":   row["status"],
        }

    # ------------------------------------------------------------------
    # Utilities
    # ------------------------------------------------------------------

    @staticmethod
    def _get_viewstate(soup: BeautifulSoup) -> dict:
        fields = {}
        for name in ("__VIEWSTATE", "__VIEWSTATEGENERATOR", "__EVENTVALIDATION"):
            tag = soup.find("input", {"name": name})
            if tag:
                fields[name] = tag.get("value", "")
        return fields

    @staticmethod
    def _col(headers: list, candidates: list) -> Optional[int]:
        for c in candidates:
            for i, h in enumerate(headers):
                if c in h:
                    return i
        return None

# ===========================================================================
# Probate Court Scraper  (PRO)  — Phase 3
# ===========================================================================

PROBATE_BASE   = "https://probate.cuyahogacounty.gov/pa"
PROBATE_TOS    = f"{PROBATE_BASE}/TOS.aspx"
PROBATE_SEARCH = f"{PROBATE_BASE}/CaseSearch.aspx"

from datetime import date as _date

class ProbateScraper:

    def __init__(self):
        self.raw_records: list[dict] = []
        self._session = None

    def run(self):
        log.info("Scraping Cuyahoga Probate Court estate filings ...")
        try:
            self._session = self._make_session()
            if self._session:
                self._search_estates()
        except Exception as e:
            log.warning("Probate scraper failed: %s", e)
        log.info("Probate estates: %d records", len(self.raw_records))

    def _make_session(self):
        import requests as _req
        session = _req.Session()
        session.verify = True
        session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,*/*",
        })
        resp = session.get(PROBATE_TOS, timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        vs = {n: (soup.find("input", {"name": n}) or {}).get("value", "")
              for n in ("__VIEWSTATE", "__VIEWSTATEGENERATOR", "__EVENTVALIDATION")}
        log.info("Probate TOS cookies: %s | ViewState present: %s",
                 dict(session.cookies), bool(vs.get("__VIEWSTATE")))
        if not vs.get("__VIEWSTATE"):
            log.warning("Probate: no ViewState on TOS page — skipping")
            return None
        btn = soup.find("input", {"type": "submit"})
        btn_name  = btn["name"]  if btn else "btnAccept"
        btn_value = btn["value"] if btn else "I Accept"
        session.cookies.set("CUPR_WEBDOCKET", "1",
                            domain="probate.cuyahogacounty.gov", path="/pa")
        resp = session.post(PROBATE_TOS, timeout=30, data={
            **vs,
            "__EVENTTARGET": "", "__EVENTARGUMENT": "",
            btn_name: btn_value,
        }, headers={"Referer": PROBATE_TOS})
        resp.raise_for_status()
        log.info("Probate TOS POST done. Final URL: %s | cookies: %s",
                 resp.url, dict(session.cookies))
        return session

    def _search_estates(self):
        date_to   = _date.today()
        date_from = date_to - timedelta(days=LOOKBACK_DAYS)
        log.info("Probate search window: %s -> %s", date_from, date_to)
        fmt = lambda d: d.strftime("%m/%d/%Y")

        resp = self._session.get(PROBATE_SEARCH, timeout=30,
                                 headers={"Referer": PROBATE_TOS})
        resp.raise_for_status()
        log.info("Probate CaseSearch GET — final URL: %s", resp.url)

        soup = BeautifulSoup(resp.text, "html.parser")
        vs = {n: (soup.find("input", {"name": n}) or {}).get("value", "")
              for n in ("__VIEWSTATE", "__VIEWSTATEGENERATOR", "__EVENTVALIDATION")}

        # Detect prefix by finding any input whose name ends with btnSearchByParty
        prefix = "mpContentPH$"
        for inp in soup.find_all("input", {"type": "submit"}):
            n = inp.get("name", "")
            if "btnSearchByParty" in n:
                prefix = n.replace("btnSearchByParty", "")
                break
            if "btnSearchByCase" in n:
                prefix = n.replace("btnSearchByCase", "")
                break
        log.info("Probate prefix detected: '%s'", prefix)

        payload = {
            **vs,
            "__EVENTTARGET":                   f"{prefix}btnSearchByParty",
            "__EVENTARGUMENT":                 "",
            f"{prefix}txtCaseYear":            "",
            f"{prefix}ddlCaseCategory":        "",
            f"{prefix}txtCaseNumber":          "",
            f"{prefix}rblPartyType":           "P",
            f"{prefix}txtFirstName":           "",
            f"{prefix}txtMiddleName":          "",
            f"{prefix}txtLastName":            "",
            f"{prefix}ddlSuffix":              "",
            f"{prefix}ddlPartyRole":           "",
            f"{prefix}txtCaseYearParty":       "",
            f"{prefix}ddlCaseCategoryParty":   "ES",
            f"{prefix}txtFilingDateFrom":      fmt(date_from),
            f"{prefix}txtFilingDateTo":        fmt(date_to),
        }
        log.info("Probate POST payload: %s", payload)
        time.sleep(1)

        resp = self._session.post(PROBATE_SEARCH, data=payload, timeout=45,
                                  headers={"Referer": PROBATE_SEARCH})
        resp.raise_for_status()
        result_soup = BeautifulSoup(resp.text, "html.parser")
        title = result_soup.find("title")
        log.info("Probate POST title: %s | preview: %s",
                 title.get_text(strip=True) if title else "none",
                 result_soup.get_text(" ", strip=True)[:400])

        rows = self._parse_results(result_soup)
        log.info("Probate results: %d estate filings", len(rows))
        for row in rows:
            rec = self._row_to_record(row)
            if rec:
                self.raw_records.append(rec)

    def _parse_results(self, soup):
        rows = []
        table = None
        for t in soup.find_all("table"):
            trs = t.find_all("tr")
            if len(trs) > 1 and t.find("td"):
                table = t
                break
        if not table:
            log.warning("Probate: no results table found.")
            return rows
        tr_list = table.find_all("tr")
        headers = [th.get_text(strip=True).lower()
                   for th in tr_list[0].find_all(["th", "td"])]
        log.info("Probate table headers: %s", headers)
        def col(candidates):
            for c in candidates:
                for i, h in enumerate(headers):
                    if c in h: return i
            return None
        idx = {
            "case_no":   col(["case number", "case no", "casenumber", "case"]),
            "name":      col(["name", "decedent", "party"]),
            "case_type": col(["case type", "type"]),
            "filed":     col(["filed", "filing date", "date"]),
            "status":    col(["status"]),
        }
        for tr in tr_list[1:]:
            cells = tr.find_all("td")
            if not cells: continue
            def cell(key):
                i = idx.get(key)
                return cells[i].get_text(strip=True) if i is not None and i < len(cells) else ""
            link = tr.find("a", href=True)
            href = link["href"] if link else ""
            if href and not href.startswith("http"):
                href = f"{PROBATE_BASE}/{href.lstrip('/')}"
            rows.append({
                "case_no":    cell("case_no"),
                "name":       cell("name"),
                "case_type":  cell("case_type"),
                "filed":      cell("filed"),
                "status":     cell("status"),
                "detail_url": href,
            })
        return rows

    def _row_to_record(self, row):
        raw_name = row["name"].strip()
        if not raw_name: return None
        if "," in raw_name:
            last, _, first = raw_name.partition(",")
        else:
            parts = raw_name.split()
            last = parts[-1] if parts else raw_name
            first = " ".join(parts[:-1])
        owner = f"{last.strip().upper()}, {first.strip().upper()}" if first.strip() else last.strip().upper()
        return {
            "doc_num":      row["case_no"] or f"PRO-{raw_name[:20]}",
            "doc_type":     "PRO",
            "cat":          "PRO",
            "cat_label":    "Probate / Estate",
            "filed":        row["filed"],
            "owner":        owner,
            "grantee":      "",
            "amount":       None,
            "legal":        "",
            "clerk_url":    row["detail_url"] or PROBATE_SEARCH,
            "prop_address": "",
            "prop_city":    "Cleveland",
            "prop_state":   "OH",
            "prop_zip":     "",
            "mail_address": "", "mail_city": "", "mail_state": "OH", "mail_zip": "",
            "source":       "Cuyahoga Probate Court",
            "neighborhood": "",
            "viol_status":  row["status"],
            "viol_desc":    "",
            "viol_severity": "",
            "pro_case_no":  row["case_no"],
            "pro_decedent": raw_name,
            "pro_status":   row["status"],
        }
# ===========================================================================
# Lis Pendens PDF Parser  (LP)
# ===========================================================================
# How to use:
#   1. Go to cuyahoga.oh.publicsearch.us
#   2. Search "lis pendens", set date range Jan 2026 to today
#   3. Click "Export all Results" → download PDF
#   4. Commit the PDF to repo as data/lp_export.pdf
#   5. It will be auto-parsed on every scraper run
#
# Column x-positions confirmed from Neumo PDF header (page width 792pt):
#   Grantor=24, Grantee=105, Date=267, DocNum=328, Parcel=530, Address=590

_LP_COLS = {
    "grantor": (24,  105),
    "grantee": (105, 187),
    "date":    (267, 328),
    "docnum":  (328, 400),
    "parcel":  (530, 590),
    "address": (590, 654),
}
_LP_CUTOFF = datetime(2026, 1, 1)


def parse_lp_pdf(pdf_path: str) -> list:
    """Parse a Cuyahoga County Recorder lis pendens PDF into LP lead records."""
    try:
        import pdfplumber as _pp
    except ImportError:
        log.warning("pdfplumber not installed — LP PDF skipped. pip install pdfplumber")
        return []

    def _col(words, col):
        lo, hi = _LP_COLS[col]
        return " ".join(w["text"] for w in sorted(
            [w for w in words if lo <= w["x0"] < hi],
            key=lambda w: (w["top"], w["x0"])
        ))

    records = []
    try:
        with _pp.open(pdf_path) as pdf:
            for page in pdf.pages:
                words = [w for w in page.extract_words(x_tolerance=3, y_tolerance=3)
                         if 120 < w["top"] < 750]
                rows = defaultdict(list)
                for w in words:
                    rows[round(w["top"] / 5) * 5].append(w)
                starts = [
                    y for y, rw in sorted(rows.items())
                    if re.search(r"\b\d{12}\b", " ".join(w["text"] for w in rw))
                    and re.search(r"\b\d{1,2}/\d{1,2}/\d{4}\b", " ".join(w["text"] for w in rw))
                ]
                for i, sy in enumerate(starts):
                    ey = starts[i + 1] if i + 1 < len(starts) else 750
                    rw = [w for w in words if sy <= w["top"] < ey]
                    rt = " ".join(w["text"] for w in rw)
                    dm  = re.search(r"\b(\d{12})\b", rt)
                    dtm = re.search(r"\b(\d{1,2}/\d{1,2}/\d{4})\b", rt)
                    if not dm or not dtm:
                        continue
                    try:
                        if datetime.strptime(dtm.group(1), "%m/%d/%Y") < _LP_CUTOFF:
                            continue
                    except Exception:
                        pass
                    pm  = re.search(r"\d{3}-\d{2}-\d{3}", _col(rw, "parcel"))
                    ar  = _col(rw, "address")
                    zm  = re.search(r"\b(44\d{3})\b", ar)
                    om  = re.search(r"([A-Za-z\s]+?),?\s*(?:OHIO|Ohio)", ar)
                    records.append({
                        "doc_num":       dm.group(1),
                        "doc_type":      "LP",
                        "cat":           "LP",
                        "cat_label":     "Lis Pendens",
                        "filed":         dtm.group(1),
                        "owner":         _col(rw, "grantor").title().rstrip(",").strip(),
                        "grantee":       _col(rw, "grantee").title().rstrip(",").strip(),
                        "amount":        None,
                        "legal":         format_parcel(pm.group(0)) if pm else "",
                        "clerk_url":     f"https://cuyahoga.oh.publicsearch.us/doc/{dm.group(1)}",
                        "prop_address":  ar[:om.start()].strip().title() if om else ar.strip().title(),
                        "prop_city":     om.group(1).strip().title() if om else "Cleveland",
                        "prop_state":    "OH",
                        "prop_zip":      zm.group(1) if zm else "",
                        "mail_address":  "",
                        "mail_city":     "",
                        "mail_state":    "OH",
                        "mail_zip":      "",
                        "source":        "Cuyahoga Recorder PDF",
                        "neighborhood":  "",
                        "viol_status":   "",
                        "viol_desc":     "",
                        "viol_severity": "",
                    })
    except Exception as e:
        log.warning("LP PDF parse failed: %s", e)

    log.info("LP PDF: %d lis pendens records (Jan 2026+)", len(records))
    return records


def load_lp_pdf_if_present() -> list:
    """Auto-load LP PDF from data/lp_export.pdf if it exists."""
    lp_pdf = REPO_ROOT / "data" / "lp_export.pdf"
    if lp_pdf.exists():
        log.info("Found LP PDF at %s — parsing ...", lp_pdf)
        return parse_lp_pdf(str(lp_pdf))
    return []


# ===========================================================================
# Bankruptcy Scraper  (BK) — CourtListener API
# ===========================================================================
# Uses the free CourtListener REST API to pull Chapter 7/13 filings from the
# Northern District of Ohio (Cuyahoga County).
# Set env var COURTLISTENER_TOKEN if you have an account (higher rate limits).
# Without a token the API still works but is rate-limited to ~100 req/day.
#
# After pulling filers, each name is looked up in MyPlace. If a parcel match
# is found, owns_property=True. If not, owns_property=False and the lead is
# still created but flagged "No property found" so it sinks in the dashboard.

COURTLISTENER_BASE = "https://www.courtlistener.com/api/rest/v4"
# Cuyahoga County FIPS = 39035, Northern District of Ohio court = ohnb
BK_COURT          = "ohnb"
BK_CHAPTERS       = ["7", "13"]


class BankruptcyScraper:

    def __init__(self):
        self.raw_records: list[dict] = []
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": "CuyahogaLeadScraper/1.0",
            "Accept":     "application/json",
        })
        token = os.getenv("COURTLISTENER_TOKEN", "")
        if token:
            self._session.headers["Authorization"] = f"Token {token}"
            log.info("CourtListener: using auth token")
        else:
            log.info("CourtListener: no token set — using public rate limit")

    def run(self):
        log.info("Scraping Northern District Ohio bankruptcy filings ...")
        cutoff = (datetime.now() - timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%d")
        for chapter in BK_CHAPTERS:
            self._fetch_chapter(chapter, cutoff)
        log.info("Bankruptcy filings: %d records", len(self.raw_records))

    def _fetch_chapter(self, chapter: str, cutoff: str):
        # Use /bankruptcy-information/ endpoint — the correct endpoint for
        # filtering by chapter. Then follow the docket URL for case metadata.
        endpoint = f"{COURTLISTENER_BASE}/bankruptcy-information/"
        params = {
            "docket__court":           BK_COURT,
            "chapter":                 chapter,
            "docket__date_filed__gte": cutoff,
            "order_by":                "-docket__date_filed",
            "page_size":               100,
            "format":                  "json",
            "fields":                  "docket,chapter,date_filed",
        }
        next_url = endpoint
        page = 1
        fetched = 0
        while next_url:
            try:
                r = self._session.get(
                    next_url,
                    params=params if page == 1 else None,
                    timeout=30
                )
                if r.status_code == 429:
                    log.warning("CourtListener rate limited — stopping BK fetch")
                    break
                if r.status_code != 200:
                    log.warning("CourtListener HTTP %d for chapter %s: %s",
                                r.status_code, chapter, r.text[:200])
                    break
                data = r.json()
                results = data.get("results", [])
                log.info("BK chapter %s page %d: %d results", chapter, page, len(results))
                for item in results:
                    rec = self._bkinfo_to_record(item, chapter)
                    if rec:
                        self.raw_records.append(rec)
                        fetched += 1
                next_url = data.get("next")
                page += 1
                time.sleep(1)
            except Exception as e:
                log.warning("CourtListener fetch failed (chapter %s): %s", chapter, e)
                break
        log.info("BK chapter %s: %d records fetched", chapter, fetched)

    def _bkinfo_to_record(self, item: dict, chapter: str) -> Optional[dict]:
        # item has: docket (URL or nested obj), chapter, date_filed
        docket_data = item.get("docket") or {}

        # docket may be a URL string or a nested dict depending on API version
        if isinstance(docket_data, str):
            # Fetch the docket object to get case_name, docket_number etc.
            try:
                r = self._session.get(docket_data,
                                      params={"format": "json",
                                              "fields": "case_name,docket_number,date_filed,absolute_url"},
                                      timeout=15)
                if r.status_code == 200:
                    docket_data = r.json()
                else:
                    docket_data = {}
            except Exception:
                docket_data = {}

        case_name = (docket_data.get("case_name") or "").strip()
        case_num  = (docket_data.get("docket_number") or "").strip()
        filed     = (docket_data.get("date_filed") or item.get("date_filed") or "")
        abs_url   = docket_data.get("absolute_url", "")
        clerk_url = f"https://www.courtlistener.com{abs_url}" if abs_url else \
                    "https://www.courtlistener.com/recap/"

        if not case_name:
            return None

        # Strip "In re:" prefix common in bankruptcy case names
        name = re.sub(r"(?i)^in\s+re:?\s*", "", case_name).strip()
        name = re.sub(r"\s*\(.*?\)", "", name).strip()

        filed_fmt = ""
        if filed:
            try:
                filed_fmt = datetime.strptime(filed[:10], "%Y-%m-%d").strftime("%m/%d/%Y")
            except Exception:
                filed_fmt = filed

        return {
            "doc_num":       case_num or f"BK-{name[:20]}",
            "doc_type":      f"BK{chapter}",
            "cat":           "BK",
            "cat_label":     f"Bankruptcy Ch.{chapter}",
            "filed":         filed_fmt,
            "owner":         name,
            "grantee":       "",
            "amount":        None,
            "legal":         "",
            "clerk_url":     clerk_url,
            "prop_address":  "",
            "prop_city":     "Cleveland",
            "prop_state":    "OH",
            "prop_zip":      "",
            "mail_address":  "",
            "mail_city":     "",
            "mail_state":    "OH",
            "mail_zip":      "",
            "source":        "CourtListener / PACER",
            "neighborhood":  "",
            "viol_status":   "",
            "viol_desc":     "",
            "viol_severity": "",
            "owns_property": None,
            "bk_chapter":    chapter,
        }



# ===========================================================================
# Parcel Lookup — MyPlace enrichment
# ===========================================================================

class ParcelLookup:

    MYPLACE_URL = ("https://gis.cuyahogacounty.us/server/rest/services/"
                   "MyPLACE/Parcels_WMA_GJOIN_WGS84/MapServer/0/query")

    def __init__(self):
        self._by_owner   = defaultdict(list)
        self._by_parcel  = {}
        self._by_address = {}
        self._session    = None
        self._delinq_cache = {}   # parcel_pin -> {"delinquent": bool, "delinq_amt": str}

    def load(self):
        self._session = requests.Session()
        self._session.verify = False
        self._session.headers.update({"User-Agent": "CuyahogaLeadScraper/1.0"})
        try:
            r = self._session.get(self.MYPLACE_URL, params={
                "where": "parcelpin='703010013'",
                "outFields": "parcelpin,parcel_owner,par_addr_all",
                "f": "json",
                "resultRecordCount": 1,
                "returnGeometry": "false",
            }, timeout=15)
            if r.status_code == 200 and r.json().get("features"):
                log.info("MyPlace API working.")
        except Exception as e:
            log.debug("MyPlace test failed: %s", e)

    def enrich_records(self, records: list) -> int:
        if not self._session:
            return 0
        enriched = 0
        for rec in records:
            parcel = rec.get("legal", "")
            if not parcel:
                # PRO and BK records have no parcel yet — try owner-name lookup
                if rec.get("cat") in ("PRO", "BK") and rec.get("owner"):
                    match = self._owner_name_lookup(rec["owner"])
                    if match:
                        self._apply_match(rec, match)
                        if rec.get("cat") == "BK":
                            rec["owns_property"] = True
                        enriched += 1
                    else:
                        if rec.get("cat") == "BK":
                            rec["owns_property"] = False
                continue
            match = self._api_lookup_parcel(parcel)
            if match:
                self._apply_match(rec, match)
                enriched += 1
            time.sleep(0.3)
        return enriched

    def _apply_match(self, rec: dict, match: dict):
        if not rec.get("prop_address") and match.get("site_addr"):
            rec["prop_address"] = match["site_addr"]
            rec["prop_city"]    = match.get("site_city") or "Cleveland"
            rec["prop_zip"]     = match.get("site_zip", "")
        if not rec.get("owner") and match.get("owner"):
            rec["owner"] = match["owner"]
        if not rec.get("legal") and match.get("parcel"):
            rec["legal"] = match["parcel"]
        rec["homestead"]    = match.get("homestead", False)
        rec["appraised"]    = match.get("appraised", "")
        rec["luc"]          = match.get("luc", "")
        rec["luc_desc"]     = match.get("luc_desc", "")

        # --- Delinquency + mailing address: prefer Treasurer ArcGIS layer ---
        # WCF only returns 6 fields and never has MAIL_* or delinquency data.
        # Query the Treasurer delinquent-tax layer instead; if the parcel is not
        # in that layer it simply means it is current (delinquent=False).
        parcel_pin = match.get("parcel") or rec.get("legal", "")
        treas = self._treasurer_delinq_lookup(parcel_pin) if parcel_pin else {}

        if treas.get("mail_addr"):
            rec["mail_address"] = treas["mail_addr"]
            rec["mail_city"]    = treas["mail_city"]
            rec["mail_state"]   = treas["mail_state"] or "OH"
            rec["mail_zip"]     = treas["mail_zip"]
        elif match.get("mail_addr"):
            # Fall back to whatever WCF returned (may still be empty)
            rec["mail_address"] = match["mail_addr"]
            rec["mail_city"]    = match.get("mail_city", "")
            rec["mail_state"]   = match.get("mail_state") or "OH"
            rec["mail_zip"]     = match.get("mail_zip", "")

        rec["delinquent"]   = treas.get("delinquent", match.get("delinquent", False))
        rec["delinq_amt"]   = treas.get("delinq_amt", match.get("delinq_amt", ""))

        # Out-of-state: derive from whichever mail_state we ended up with
        mail_state = rec.get("mail_state", "")
        rec["out_of_state"] = bool(mail_state and mail_state not in ("", "OH"))

    def _owner_name_lookup(self, owner_name: str) -> Optional[dict]:
        """Name-based parcel lookup for PRO records where we have no parcel number yet."""
        if not self._session:
            return None
        try:
            # Try each name variant (SMITH JOHN / JOHN SMITH)
            for variant in name_variants(owner_name):
                url = (
                    f"https://myplace.cuyahogacounty.gov/MyPlaceService.svc/"
                    f"ParcelsAndValuesByAnySearchByAndCity/"
                    f"{requests.utils.quote(variant)}?city=99&searchBy=Owner"
                )
                r = self._session.get(url, timeout=15)
                if r.status_code != 200:
                    continue
                import json as _json
                raw = r.text.strip()
                try:
                    data = _json.loads(raw)
                    if isinstance(data, str):
                        data = _json.loads(data)
                except Exception:
                    continue
                if not data or not data[0]:
                    continue
                attrs = data[0][0] if isinstance(data[0], list) else data[0]
                result = self._attrs_to_match(attrs)
                if result:
                    log.info("Probate parcel match: %s → %s",
                             owner_name, result.get("site_addr", ""))
                    return result
        except Exception as e:
            log.debug("Owner name lookup failed for %s: %s", owner_name, e)
        return None

    def _api_lookup_parcel(self, parcel: str) -> Optional[dict]:
        if not self._session:
            return None
        try:
            log.info("Parcel API lookup: %s", parcel)
            url = (
                f"https://myplace.cuyahogacounty.gov/MyPlaceService.svc/"
                f"ParcelsAndValuesByAnySearchByAndCity/{parcel}?city=99&searchBy=Parcel"
            )
            r = self._session.get(url, timeout=15)
            if r.status_code != 200:
                log.warning("Parcel API HTTP %d for %s", r.status_code, parcel)
                return None
            import json as _json
            raw = r.text.strip()
            try:
                data = _json.loads(raw)
                if isinstance(data, str):
                    data = _json.loads(data)
            except Exception as e:
                log.warning("Parcel JSON parse failed for %s: %s | raw: %s",
                            parcel, e, raw[:200])
                return None
            if not data or not data[0]:
                return None
            attrs = data[0][0] if isinstance(data[0], list) else data[0]
            log.info("WCF attrs sample: %s", dict(list(attrs.items())[:25]))
            result = self._attrs_to_match(attrs)
            if result:
                self._by_parcel[parcel] = result
                log.info("Parcel enriched: %s -> %s, %s",
                         parcel, result.get("site_addr", ""), result.get("site_city", ""))
            return result
        except Exception as e:
            log.warning("MyPlace WCF lookup failed for %s: %s", parcel, e)
        return None

    @staticmethod
    def _attrs_to_match(attrs: dict) -> Optional[dict]:
        owner     = normalize_name(attrs.get("DEEDED_OWNER") or attrs.get("OWNER") or "")
        site_addr = (attrs.get("PHYSICAL_ADDRESS") or "").strip().title()
        city      = (attrs.get("PARCEL_CITY") or "Cleveland").strip().title()
        zipcode   = str(attrs.get("PARCEL_ZIP") or "")
        parcel    = (attrs.get("PARCEL_NUMBER") or attrs.get("PIN") or "")

        # Mailing address — populated when owner mails to a different address
        mail_addr  = (attrs.get("MAIL_ADDRESS") or attrs.get("MAILING_ADDRESS") or "").strip().title()
        mail_city  = (attrs.get("MAIL_CITY")    or "").strip().title()
        mail_state = (attrs.get("MAIL_STATE")   or "OH").strip().upper()
        mail_zip   = str(attrs.get("MAIL_ZIP")  or "")

        # Out-of-state flag — derived from mailing state
        out_of_state = mail_state not in ("", "OH") if mail_state else False

        # Delinquency — try multiple possible WCF field name variants
        try:
            delinq_raw = float(
                attrs.get("DELINQUENT_AMOUNT")
                or attrs.get("BACK_TAX_AMOUNT")
                or attrs.get("DELINQ_AMOUNT")
                or 0
            )
        except (TypeError, ValueError):
            delinq_raw = 0.0
        delinquent = bool(
            attrs.get("DELINQUENT_FLAG")
            or attrs.get("DELINQUENT")
            or attrs.get("IS_DELINQUENT")
            or attrs.get("DELINQ_FLAG")
            or delinq_raw > 0
        )
        delinq_amt = f"{delinq_raw:,.2f}" if delinq_raw > 0 else str(
            attrs.get("DELINQUENT_AMOUNT") or attrs.get("BACK_TAX_AMOUNT") or ""
        )

        # Homestead exemption — try multiple field name variants
        homestead = bool(
            attrs.get("HOMESTEAD")
            or attrs.get("HOMESTEAD_FLAG")
            or attrs.get("HMSTD_FLAG")
            or attrs.get("HMSTD")
        )

        # Land use code
        luc      = str(attrs.get("TAX_LUC") or attrs.get("LUC") or "")
        luc_desc = str(attrs.get("TAX_LUC_DESCRIPTION") or attrs.get("LUC_DESC") or "")

        # Appraised / assessed value
        appraised = str(attrs.get("CERTIFIED_TAX_TOTAL") or attrs.get("APPRAISED") or "")

        if not (owner or site_addr):
            return None

        return {
            "owner":        owner,
            "site_addr":    site_addr,
            "site_city":    city,
            "site_zip":     zipcode,
            "mail_addr":    mail_addr,
            "mail_city":    mail_city,
            "mail_state":   mail_state,
            "mail_zip":     mail_zip,
            "parcel":       parcel,
            "delinquent":   delinquent,
            "delinq_amt":   delinq_amt,
            "homestead":    homestead,
            "appraised":    appraised,
            "out_of_state": out_of_state,
            "luc":          luc,
            "luc_desc":     luc_desc,
        }

    def _lookup_luc(self, parcel: str) -> str:
        return ""

    def _treasurer_delinq_lookup(self, parcel_pin: str) -> dict:
        """Query Cuyahoga County Treasurer delinquent-tax ArcGIS layer by parcel PIN.

        Returns dict with keys: delinquent (bool), delinq_amt (str),
        mail_addr, mail_city, mail_state, mail_zip.
        Falls back to empty dict on any error so enrichment continues normally.
        """
        if not self._session:
            return {}
        # Normalise PIN — strip dashes/spaces, pad to 15 digits if needed
        pin = re.sub(r"[\s\-]", "", parcel_pin)
        if pin in self._delinq_cache:
            return self._delinq_cache[pin]
        try:
            params = {
                "where":            f"PARCEL_NO='{pin}'",
                "outFields":        (
                    "PARCEL_NO,OWNER_NAME,MAIL_ADDRESS,MAIL_CITY,"
                    "MAIL_STATE,MAIL_ZIP,TOTAL_DUE,TAX_YEAR"
                ),
                "f":                "json",
                "resultRecordCount": 1,
                "returnGeometry":   "false",
            }
            r = self._session.get(TREASURER_DELINQ_URL, params=params, timeout=15)
            if r.status_code != 200:
                log.debug("Treasurer API HTTP %d for %s", r.status_code, pin)
                self._delinq_cache[pin] = {}
                return {}
            data = r.json()
            features = data.get("features") or []
            if not features:
                # Parcel not in delinquency layer → not delinquent
                result = {
                    "delinquent": False,
                    "delinq_amt": "",
                    "mail_addr":  "",
                    "mail_city":  "",
                    "mail_state": "",
                    "mail_zip":   "",
                }
                self._delinq_cache[pin] = result
                return result
            attrs = features[0].get("attributes", {})
            total_due = attrs.get("TOTAL_DUE") or 0
            try:
                total_due = float(total_due)
            except (TypeError, ValueError):
                total_due = 0.0
            mail_state = (attrs.get("MAIL_STATE") or "OH").strip().upper()
            result = {
                "delinquent": total_due > 0,
                "delinq_amt": f"{total_due:,.2f}" if total_due > 0 else "",
                "mail_addr":  (attrs.get("MAIL_ADDRESS") or "").strip().title(),
                "mail_city":  (attrs.get("MAIL_CITY")    or "").strip().title(),
                "mail_state": mail_state,
                "mail_zip":   str(attrs.get("MAIL_ZIP")  or "").strip(),
            }
            log.info("Treasurer delinq hit: %s → $%s delinquent=%s mail_state=%s",
                     pin, result["delinq_amt"] or "0", result["delinquent"],
                     mail_state)
            self._delinq_cache[pin] = result
            return result
        except Exception as e:
            log.warning("Treasurer delinq lookup failed for %s: %s", pin, e)
            self._delinq_cache[pin] = {}
            return {}


# ===========================================================================
# Scoring
# ===========================================================================

class LeadScorer:
    WEEK_AGO = datetime.now() - timedelta(days=7)

    @staticmethod
    def score(rec: dict, all_recs: list) -> tuple:
        # Base 15 (was 30). Lower floor spreads the distribution so that
        # single-signal leads land in the 40s and stacked leads reach 80-100.
        flags, points = [], 15
        cat   = rec.get("cat", "")
        dtype = rec.get("doc_type", "")
        owner = rec.get("owner", "")
        amt   = rec.get("amount")

        # --- Primary signal ---
        if cat == "LP":      flags.append("Lis pendens");        points += 20  # was 10
        if cat == "NOFC":    flags.append("Pre-foreclosure");    points += 25  # was 10
        if cat == "TAXDEED": flags.append("Tax deed");           points += 25  # was 10
        if cat == "JUD":     flags.append("Judgment lien");      points += 15  # was 10
        if cat == "CODE":
            flags.append("Code violation")
            points += 10                                          # was 15
            sev = rec.get("viol_severity", "")
            if sev == "high":
                flags.append("Chief Approved violation")
                points += 15
            elif sev == "medium":
                points += 7
        if cat == "CONDEMN": flags.append("Condemned property"); points += 15  # was 20
        if cat == "LIEN":
            if dtype in ("LNIRS", "LNFED", "LNCORPTX"): flags.append("Tax lien")
            elif dtype == "LNMECH": flags.append("Mechanic lien")
            elif dtype == "LNHOA":  flags.append("HOA lien")
            else: flags.append("Lien")
            points += 10
        if cat == "PRO":   flags.append("Probate / estate"); points += 20  # was 10
        if cat == "RELLP": flags.append("Lis pendens");      points += 5
        if cat == "BK":
            chapter = rec.get("bk_chapter", "")
            flags.append(f"Bankruptcy Ch.{chapter}" if chapter else "Bankruptcy")
            points += 20                                          # was 15
            if rec.get("owns_property") is False:
                flags.append("No property found")
                points = min(points, 25)
            elif rec.get("owns_property") is True:
                flags.append("Property confirmed")
                points += 10

        # --- Cross-signal stacking bonuses ---
        norm = normalize_name(owner)
        if norm:
            owner_cats = {r["cat"] for r in all_recs
                          if normalize_name(r.get("owner", "")) == norm}
            if "LP" in owner_cats and owner_cats & {"NOFC", "TAXDEED"}:
                points += 20
            if owner_cats & {"NOFC", "LP"} and owner_cats & {"CODE", "CONDEMN"}:
                flags.append("Foreclosure + violation"); points += 25

        # --- Debt size ---
        if amt:
            if amt > 100_000: flags.append("High debt (>$100k)"); points += 15
            elif amt > 50_000: points += 8

        # --- Recency ---
        try:
            dt = datetime.strptime(rec.get("filed", "").strip(), "%m/%d/%Y")
            if dt >= LeadScorer.WEEK_AGO:
                flags.append("New this week"); points += 5
        except Exception:
            pass

        # --- Enrichment signals (additive but not primary) ---
        if rec.get("prop_address") or rec.get("mail_address"):
            flags.append("Address found"); points += 3           # was 5

        if owner and re.search(r"\b(LLC|INC|CORP|LTD|TRUST|ESTATE)\b", owner, re.I):
            flags.append("LLC / corp owner"); points += 5

        if rec.get("delinquent"):
            flags.append("Delinquent taxes"); points += 20       # was 15 — high signal now that it works
        if rec.get("out_of_state"):
            flags.append("Out-of-state owner"); points += 15     # was 10 — high signal now that it works
        if rec.get("homestead") is False and rec.get("prop_address"):
            flags.append("No homestead exemption"); points += 3  # was 5 — nearly universal, reduce weight

        return min(100, max(0, points)), list(dict.fromkeys(flags))


# ===========================================================================
# GHL CSV Export
# ===========================================================================

GHL_COLS = [
    "First Name", "Last Name", "Mailing Address", "Mailing City",
    "Mailing State", "Mailing Zip",
    "Property Address", "Property City", "Property State", "Property Zip",
    "Lead Type", "Document Type", "Date Filed", "Document Number",
    "Amount/Debt Owed", "Seller Score", "Motivated Seller Flags",
    "Source", "Public Records URL", "Parcel Number", "Appraised Value",
    "Delinquent Taxes", "Delinquent Amount", "Homestead Exemption",
    "Out-of-State Owner", "Neighborhood", "Violation Status",
    "Violation Description", "Violation Severity",
    # PRO-specific columns
    "Probate Case No", "Decedent Name", "Estate Status",
    # BK-specific columns
    "Bankruptcy Chapter", "Owns Property",
]

def split_name(n):
    if not n: return "", ""
    if "," in n:
        p = [x.strip() for x in n.split(",", 1)]
        return p[1].title(), p[0].title()
    p = n.split()
    return (p[0].title(), " ".join(p[1:]).title()) if len(p) > 1 else ("", p[0].title())

def export_csv(records: list, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=GHL_COLS, extrasaction="ignore")
        w.writeheader()
        for r in records:
            fn, ln = split_name(r.get("owner", ""))
            amt = r.get("amount")
            w.writerow({
                "First Name":             fn,
                "Last Name":              ln,
                "Mailing Address":        r.get("mail_address", ""),
                "Mailing City":           r.get("mail_city", ""),
                "Mailing State":          r.get("mail_state", "OH"),
                "Mailing Zip":            r.get("mail_zip", ""),
                "Property Address":       r.get("prop_address", ""),
                "Property City":          r.get("prop_city", "Cleveland"),
                "Property State":         r.get("prop_state", "OH"),
                "Property Zip":           r.get("prop_zip", ""),
                "Lead Type":              r.get("cat_label", ""),
                "Document Type":          r.get("doc_type", ""),
                "Date Filed":             r.get("filed", ""),
                "Document Number":        r.get("doc_num", ""),
                "Amount/Debt Owed":       f"${amt:,.2f}" if amt else "",
                "Seller Score":           r.get("score", 0),
                "Motivated Seller Flags": "; ".join(r.get("flags", [])),
                "Source":                 r.get("source", ""),
                "Public Records URL":     r.get("clerk_url", ""),
                "Parcel Number":          r.get("legal", ""),
                "Appraised Value":        r.get("appraised", ""),
                "Delinquent Taxes":       "Yes" if r.get("delinquent") else "No",
                "Delinquent Amount":      r.get("delinq_amt", ""),
                "Homestead Exemption":    "Yes" if r.get("homestead") else "No",
                "Out-of-State Owner":     "Yes" if r.get("out_of_state") else "No",
                "Neighborhood":           r.get("neighborhood", ""),
                "Violation Status":       r.get("viol_status", ""),
                "Violation Description":  r.get("viol_desc", ""),
                "Violation Severity":     r.get("viol_severity", "").title(),
                # PRO-specific
                "Probate Case No":        r.get("pro_case_no", ""),
                "Decedent Name":          r.get("pro_decedent", ""),
                "Estate Status":          r.get("pro_status", ""),
                # BK-specific
                "Bankruptcy Chapter":     r.get("bk_chapter", ""),
                "Owns Property":          ("Yes" if r.get("owns_property") is True
                                           else "No" if r.get("owns_property") is False
                                           else ""),
            })
    log.info("GHL CSV: %d rows -> %s", len(records), path)


# ===========================================================================
# Save JSON
# ===========================================================================

def save_json(data: dict, *paths: Path):
    for path in paths:
        path.parent.mkdir(parents=True, exist_ok=True)
        existing_manual = []
        if path.exists():
            try:
                with open(path, "r", encoding="utf-8") as f:
                    old = json.load(f)
                existing_manual = [r for r in old.get("records", [])
                                   if r.get("source") == "Manual Upload"]
                if existing_manual:
                    log.info("Preserving %d manual records from %s",
                             len(existing_manual), path.name)
            except Exception:
                pass

        all_recs = data["records"] + existing_manual
        seen = {}
        for rec in all_recs:
            key = rec.get("doc_num") or rec.get("clerk_url") or str(id(rec))
            if key not in seen or rec.get("score", 0) > seen[key].get("score", 0):
                seen[key] = rec
        merged = sorted(seen.values(), key=lambda r: r.get("score", 0), reverse=True)

        output = {
            **data,
            "records":      merged,
            "total":        len(merged),
            "with_address": sum(1 for r in merged
                                if r.get("prop_address") or r.get("mail_address")),
        }
        with open(path, "w", encoding="utf-8") as f:
            json.dump(output, f, indent=2, default=str)
        log.info("Saved %s (%d records)", path, len(merged))


# ===========================================================================
# Main
# ===========================================================================

EXCLUDE_KEYWORDS = {
    "CONDO", "CONDOMINIUM", "APARTMENTS", "APT", "COMMERCIAL",
    "INDUSTRIAL", "WAREHOUSE", "RETAIL", "OFFICE", "PLAZA",
    "SHOPPING", "CHURCH", "SCHOOL", "HOSPITAL", "UNIVERSITY",
    "LLC PORTFOLIO", "HOLDINGS LLC",
}

def is_sfh_or_land(rec: dict) -> bool:
    addr  = (rec.get("prop_address") or "").upper()
    owner = (rec.get("owner") or "").upper()
    legal = (rec.get("legal_desc") or rec.get("legal") or "").upper()
    combined = f"{addr} {owner} {legal}"
    return not any(kw in combined for kw in EXCLUDE_KEYWORDS)


async def main():
    log.info("=" * 60)
    log.info("Cuyahoga County Lead Scraper — NOFC + CODE + CONDEMN + PRO + BK")
    log.info("Lookback: %d days", LOOKBACK_DAYS)
    log.info("=" * 60)

    # 1. Parcel lookup
    parcel = ParcelLookup()
    log.info("Loading parcel data ...")
    parcel.load()

    # 2. Sheriff sales  (NOFC)
    sheriff = SheriffScraper()
    await sheriff.run()
    records = list(sheriff.raw_records)
    log.info("Sheriff sale records: %d", len(records))

    # 3. Code violations + condemnations  (CODE / CONDEMN)
    code = CodeViolationScraper()
    code.run()
    records.extend(code.raw_records)
    log.info("Total after violations: %d", len(records))

    # 4. Probate estates  (PRO)
    probate = ProbateScraper()
    probate.run()
    records.extend(probate.raw_records)
    log.info("Total after probate: %d", len(records))

    # 5. Lis pendens from recorder PDF (place at data/lp_export.pdf to auto-load)
    lp_records = load_lp_pdf_if_present()
    records.extend(lp_records)
    log.info("Total after LP: %d", len(records))

    # 6. Bankruptcy filings (BK) — CourtListener API
    bk = BankruptcyScraper()
    bk.run()
    records.extend(bk.raw_records)
    log.info("Total after BK: %d", len(records))

    # 6. Parcel enrichment
    log.info("Enriching records with parcel data ...")
    enriched = parcel.enrich_records(records)
    log.info("Enriched %d/%d records with parcel data", enriched, len(records))

    # 6. SFH + Land filter
    before = len(records)
    records = [r for r in records if is_sfh_or_land(r)]
    log.info("SFH+Land filter: %d → %d records (removed %d non-qualifying)",
             before, len(records), before - len(records))

    # 7. Score
    for rec in records:
        rec["score"], rec["flags"] = LeadScorer.score(rec, records)

    # 8. Cross-reference boost (foreclosure + violation on same parcel)
    def norm_parcel(p: str) -> str:
        return re.sub(r"[^0-9]", "", p or "")

    parcel_cats = defaultdict(set)
    parcel_recs = defaultdict(list)
    for rec in records:
        p = norm_parcel(rec.get("legal", ""))
        if p:
            parcel_cats[p].add(rec.get("cat", ""))
            parcel_recs[p].append(rec)

    multi_hit_parcels = 0
    for p, cats in parcel_cats.items():
        has_fc   = bool(cats & {"NOFC", "LP", "TAXDEED"})
        has_viol = bool(cats & {"CODE", "CONDEMN"})
        if has_fc and has_viol:
            multi_hit_parcels += 1
            for rec in parcel_recs[p]:
                if "Foreclosure + violation" not in rec["flags"]:
                    rec["flags"].insert(0, "🔥 Foreclosure + violation")
                rec["score"] = min(100, rec["score"] + 25)
        elif len(cats) > 1:
            for rec in parcel_recs[p]:
                if "Multi-hit parcel" not in rec["flags"]:
                    rec["flags"].append("Multi-hit parcel")
                rec["score"] = min(100, rec["score"] + 10)

    log.info("Cross-reference: %d parcels with foreclosure + violation", multi_hit_parcels)

    # 9. Deduplicate
    seen = {}
    for rec in records:
        key = rec.get("doc_num") or rec.get("clerk_url") or str(id(rec))
        if key not in seen or rec["score"] > seen[key]["score"]:
            seen[key] = rec
    records = sorted(seen.values(), key=lambda r: r["score"], reverse=True)
    log.info("Unique records after dedup: %d", len(records))

    # 10. Save
    with_addr = sum(1 for r in records if r.get("prop_address") or r.get("mail_address"))
    output = {
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "source":     "Cuyahoga Court Docket + Code Violations + Probate + Bankruptcy + Manual Uploads",
        "date_range": {
            "from": (datetime.now() - timedelta(days=LOOKBACK_DAYS)).strftime("%m/%d/%Y"),
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
