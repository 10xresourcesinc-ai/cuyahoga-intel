"""
Cuyahoga County Motivated Seller Lead Scraper
Scrapes Cuyahoga Common Pleas Court Sheriff Sale docket + Cleveland Code Violations.
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

import requests
from bs4 import BeautifulSoup

try:
    from playwright.async_api import async_playwright, TimeoutError as PWTimeout
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

COURT_URL        = "https://cpdocket.cp.cuyahogacounty.gov"
SHERIFF_SALE_URL = f"{COURT_URL}/SheriffSearch/search.aspx"
LOOKBACK_DAYS    = int(os.getenv("LOOKBACK_DAYS", "30"))
REPO_ROOT        = Path(__file__).resolve().parent.parent
DASHBOARD_JSON   = REPO_ROOT / "dashboard" / "records.json"
DATA_JSON        = REPO_ROOT / "data" / "records.json"
GHL_CSV          = REPO_ROOT / "data" / "ghl_export.csv"
RETRY            = 3

# Cleveland Open Data ArcGIS endpoints
CLEVELAND_ORG     = "https://services3.arcgis.com/dty2kHktVXHrqO8i/ArcGIS/rest/services"
VIOLATIONS_URL    = f"{CLEVELAND_ORG}/Complaint_Violation_Notices/FeatureServer/0/query"
CONDEMNATIONS_URL = f"{CLEVELAND_ORG}/Current_Condemnations/FeatureServer/0/query"
VIOLATIONS_META   = f"{CLEVELAND_ORG}/Complaint_Violation_Notices/FeatureServer/0"


# ===========================================================================
# Helpers
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
    """Ensure parcel number has dashes in NNN-NN-NNN format."""
    if not parcel:
        return parcel
    p = parcel.replace("-", "").strip()
    if len(p) == 8:
        return f"{p[:3]}-{p[3:5]}-{p[5:]}"
    elif len(p) == 9:
        return f"{p[:3]}-{p[3:5]}-{p[5:]}"
    return parcel


# ===========================================================================
# Sheriff Sale Scraper
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
                if any(x in kl for x in ["datefrom","startdate","fromdate"]):
                    form_data[k] = self.df_str
                elif any(x in kl for x in ["dateto","enddate","todate"]):
                    form_data[k] = self.dt_str

            for fname in ["dateFrom","dateTo","txtDateFrom","txtDateTo",
                          "StartDate","EndDate","dfrom","dto"]:
                kl = fname.lower()
                if any(x in kl for x in ["from","start"]):
                    form_data[fname] = self.df_str
                else:
                    form_data[fname] = self.dt_str

            form_data["__EVENTTARGET"] = ""
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

        att_m = re.search(r'Attorney\s*:\s*([A-Z][A-Z\s,\.]+?)(?=\s+Appraised|\s+Case\s*#|\s+\$)', text, re.I)
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
                addr_raw = re.sub(r'\s+(A\s+SINGLE|DWELLING|COMMERCIAL|VACANT|LOT|WITH\s+).*$', '', addr_raw, flags=re.I)
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
                args=["--no-sandbox","--disable-setuid-sandbox",
                      "--disable-dev-shm-usage","--disable-gpu"],
            )
            page = await (await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                           "AppleWebKit/537.36 Chrome/122.0 Safari/537.36",
                ignore_https_errors=True,
            )).new_page()

            try:
                await page.goto(SHERIFF_SALE_URL, wait_until="networkidle", timeout=40_000)
                await asyncio.sleep(2)

                for sel in ["input[name*='dateFrom']","input[name*='StartDate']","#dateFrom"]:
                    try:
                        await page.fill(sel, self.df_str); break
                    except Exception:
                        continue

                for sel in ["input[name*='dateTo']","input[name*='EndDate']","#dateTo"]:
                    try:
                        await page.fill(sel, self.dt_str); break
                    except Exception:
                        continue

                for sel in ["input[type='submit']","button:has-text('Search')"]:
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
# Code Violation Scraper — Cleveland Open Data
# PHASE 3 UPDATE: schema discovery + violation type classification
# ===========================================================================

# Violation description keyword → (cat subtype, human label)
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
        """Read field names from ArcGIS layer metadata so our queries use the right names."""
        try:
            r = self.session.get(VIOLATIONS_META, params={"f": "json"}, timeout=15)
            r.raise_for_status()
            fields = r.json().get("fields", [])
            self._avail_fields = [f["name"] for f in fields]
            log.info("ArcGIS violation layer fields: %s", self._avail_fields)

            for candidate in ["FILE_DATE","COMPLAINT_DATE","DATE_FILED","OPEN_DATE","CREATEDATE"]:
                if candidate in self._avail_fields:
                    self._date_field = candidate
                    log.info("Using date field: %s", self._date_field)
                    break

            for candidate in ["COMPLAINT_TYPE","VIOLATION_TYPE","VIOLATION_DESC",
                               "DESCRIPTION","RECORD_TYPE","TYPE_DESC","CASE_TYPE",
                               "COMPLAINT_DESC","WORK_DESCRIPTION","NOTICE_TYPE"]:
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
        cutoff_dt   = datetime.now() - timedelta(days=LOOKBACK_DAYS)
        cutoff_sql  = cutoff_dt.strftime("%Y-%m-%d")

        # Only request fields that actually exist in this layer (no type/description field)
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
            f"FILE_DATE >= DATE '{cutoff_sql}'",   # SQL date string — confirmed working
            f"FILE_DATE >= '{cutoff_sql}'",         # plain string fallback
            "1=1",                                  # last resort
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

        # Skip violations that are resolved/closed — not worth following up
        SKIP_STATUSES = {
            "closed", "withdrawn", "void", "cancelled", "resolved",
            "duplicate", "no violation found", "dismissed",
        }
        if status.lower().strip() in SKIP_STATUSES:
            return None

        # This layer has no violation type/description field — label generically
        viol_desc = ""
        cat, cat_label = "CODE", "Code Violation"

        # Severity tier based on status — used by scorer
        if "chief approved" in status.lower():
            viol_severity = "high"
        elif any(x in status.lower() for x in ["mailed", "open", "created"]):
            viol_severity = "medium"
        else:
            viol_severity = "low"

        # FILE_DATE: try ms epoch first, then days epoch (precision:1), then raw
        filed = ""
        raw_date = attrs.get("FILE_DATE")
        if raw_date:
            try:
                val = int(raw_date)
                if val > 1_000_000_000_000:          # looks like ms epoch
                    filed = datetime.fromtimestamp(val / 1000).strftime("%m/%d/%Y")
                elif val > 10_000:                    # looks like days epoch
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
            "source":        "Cleveland Code Enforcement",
            "neighborhood":  nbhd,
            "viol_status":   status,
            "viol_desc":     viol_desc,
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
            "doc_num":      f"CONDEMN-{parcel or address[:20].replace(' ','-')}",
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
        }


# ===========================================================================
# Accela Violation Type Scraper — cached, only hits new records
# ===========================================================================

ACCELA_CACHE_FILE = REPO_ROOT / "data" / "accela_cache.json"

# Keywords to look for in Accela page text → (cat, label)
# Based on real Cleveland Accela page values
ACCELA_KEYWORDS = {
    # Vacant / distressed
    "VACANT AND DISTRESSED":  ("CODE_STRUCT",   "Vacant / Distressed Property"),
    "VACANT":                 ("CODE_STRUCT",   "Vacant / Unsecured"),
    "UNSECURED":              ("CODE_STRUCT",   "Vacant / Unsecured"),
    "OPEN AND VACANT":        ("CODE_STRUCT",   "Vacant / Unsecured"),
    # Condemnation-related complaints
    "CONDEMNATION":           ("CODE_CONDEMN",  "Condemnation Complaint"),
    "ICLB":                   ("CODE_CONDEMN",  "Condemnation Complaint"),
    # Structural / exterior
    "COMPLETE INTERIOR/EXTERIOR": ("CODE_STRUCT", "Interior/Exterior Issue"),
    "INTERIOR/EXTERIOR":      ("CODE_STRUCT",   "Interior/Exterior Issue"),
    "STRUCTURAL":             ("CODE_STRUCT",   "Structural Issue"),
    "EXTERIOR":               ("CODE_STRUCT",   "Structural Issue"),
    "FOUNDATION":             ("CODE_STRUCT",   "Structural Issue"),
    "ROOF":                   ("CODE_STRUCT",   "Structural Issue"),
    # Tall grass / weeds
    "TALL GRASS":             ("CODE_GRASS",    "Tall Grass / Weeds"),
    "WEED":                   ("CODE_GRASS",    "Tall Grass / Weeds"),
    "OVERGROWN":              ("CODE_GRASS",    "Tall Grass / Weeds"),
    # Garbage / debris
    "GARBAGE":                ("CODE_DEBRIS",   "Garbage / Debris"),
    "DEBRIS":                 ("CODE_DEBRIS",   "Garbage / Debris"),
    "TRASH":                  ("CODE_DEBRIS",   "Garbage / Debris"),
    "JUNK":                   ("CODE_DEBRIS",   "Garbage / Debris"),
    "DUMPING":                ("CODE_DEBRIS",   "Garbage / Debris"),
    # Nuisance / vehicle
    "NUISANCE":               ("CODE_NUISANCE", "Property Nuisance"),
    "INOPERABLE":             ("CODE_NUISANCE", "Inoperable Vehicle"),
    # Mechanical / systems
    "ELECTRICAL":             ("CODE_MECH",     "Electrical Issue"),
    "PLUMBING":               ("CODE_MECH",     "Plumbing Issue"),
    # Pest
    "RODENT":                 ("CODE_PEST",     "Rodent / Pest"),
    "RAT":                    ("CODE_PEST",     "Rodent / Pest"),
}


class AccelaScraper:
    """
    For each CODE violation record that has an Accela URL, fetches the page
    and extracts the violation type. Results are cached permanently in
    data/accela_cache.json — only new (unseen) record IDs are ever fetched.
    """

    def __init__(self):
        self._cache: dict[str, dict] = {}   # record_id → {cat, cat_label, viol_desc}
        self._session = requests.Session()
        self._session.verify = False
        self._session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                          "AppleWebKit/537.36 Chrome/122.0 Safari/537.36",
        })

    def load_cache(self):
        if ACCELA_CACHE_FILE.exists():
            try:
                with open(ACCELA_CACHE_FILE, "r", encoding="utf-8") as f:
                    self._cache = json.load(f)
                log.info("Accela cache loaded: %d entries", len(self._cache))
            except Exception as e:
                log.warning("Accela cache load failed: %s", e)
                self._cache = {}

    def save_cache(self):
        ACCELA_CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(ACCELA_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(self._cache, f, indent=2)
        log.info("Accela cache saved: %d entries", len(self._cache))

    def enrich_violations(self, records: list) -> int:
        """
        For CODE records not already in cache, fetch their Accela URL and
        extract violation type. Updates record in place. Returns count enriched.
        """
        to_fetch = [
            r for r in records
            if r.get("cat") == "CODE"
            and r.get("clerk_url")
            and r.get("doc_num") not in self._cache
        ]
        log.info("Accela: %d cached, %d new to fetch", len(self._cache), len(to_fetch))

        enriched = 0
        for i, rec in enumerate(to_fetch):
            result = self._fetch_violation_type(rec["clerk_url"], rec["doc_num"])
            if result:
                self._cache[rec["doc_num"]] = result
                enriched += 1
            time.sleep(0.5)   # be polite to Accela server
            if (i + 1) % 10 == 0:
                self.save_cache()   # save every 10 in case of crash

        if to_fetch:
            self.save_cache()

        # Apply cache to ALL CODE records (including previously cached ones)
        applied = 0
        for rec in records:
            if rec.get("cat") == "CODE" and rec.get("doc_num") in self._cache:
                cached = self._cache[rec["doc_num"]]
                rec["doc_type"]  = cached.get("cat", rec["doc_type"])
                rec["cat_label"] = cached.get("cat_label", rec["cat_label"])
                rec["viol_desc"] = cached.get("viol_desc", rec.get("viol_desc", ""))
                applied += 1

        log.info("Accela: enriched %d new, applied %d from cache", enriched, applied)
        return enriched

    def _fetch_violation_type(self, url: str, record_id: str) -> Optional[dict]:
        try:
            r = self._session.get(url, timeout=20)
            if r.status_code != 200:
                log.debug("Accela HTTP %d for %s", r.status_code, record_id)
                return {"cat": "CODE", "cat_label": "Code Violation", "viol_desc": ""}

            soup = BeautifulSoup(r.text, "lxml")

            # Target the exact fields visible on Cleveland's Accela pages:
            # 1. "Project Description" — e.g. "Vacant and Distressed Property"
            # 2. "Type of Complaint"   — e.g. "Int/ext for condemnation for ICLB"
            # 3. "Descriptive nature of complaint" — free-text detail
            candidate_parts = []

            for label_text in [
                "Project Description",
                "Type of Complaint",
                "Descriptive nature of complaint",
                "Record Type",
                "Description",
            ]:
                # Find the label element
                label_el = soup.find(string=re.compile(
                    r"^\s*" + re.escape(label_text) + r"\s*:?\s*$", re.I
                ))
                if not label_el:
                    # broader search — label may have extra whitespace or be in an element
                    label_el = soup.find(string=re.compile(label_text, re.I))

                if label_el:
                    parent = label_el.find_parent(["td", "th", "dt", "label", "span", "div"])
                    if parent:
                        # Value is typically the next sibling element
                        nxt = parent.find_next_sibling()
                        if nxt:
                            val = nxt.get_text(" ", strip=True)
                            if val and len(val) < 200:
                                candidate_parts.append(val)

            candidate_text = " | ".join(p for p in candidate_parts if p)

            # Fallback: full visible page text (first 600 chars)
            if not candidate_text:
                candidate_text = soup.get_text(" ", strip=True)[:600]

            # Log every new record at DEBUG; log first few at INFO so you can verify
            if len(self._cache) < 5:
                log.info("ACCELA %s → %s", record_id, candidate_text[:250])
            else:
                log.debug("Accela %s → %s", record_id, candidate_text[:150])

            upper = candidate_text.upper()
            for keyword, (cat, label) in ACCELA_KEYWORDS.items():
                if keyword in upper:
                    log.debug("Accela %s matched '%s' → %s", record_id, keyword, label)
                    return {"cat": cat, "cat_label": label, "viol_desc": candidate_text[:150]}

            # No keyword matched — cache with raw text so we can expand keywords later
            log.debug("Accela %s — no keyword match, raw: %s", record_id, candidate_text[:150])
            return {"cat": "CODE", "cat_label": "Code Violation",
                    "viol_desc": candidate_text[:150]}

        except Exception as e:
            log.debug("Accela fetch failed for %s: %s", record_id, e)
            return None


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
        self._working_url = None

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
                "returnGeometry": "false"
            }, timeout=15)
            if r.status_code == 200 and r.json().get("features"):
                self._working_url = self.MYPLACE_URL
                log.info("MyPlace API working: %s", self._working_url)
        except Exception as e:
            log.debug("MyPlace test failed: %s", e)

        if not self._working_url:
            self._working_url = self.MYPLACE_URL
            log.info("Using MyPlace URL as fallback: %s", self._working_url)

    def enrich_records(self, records: list) -> int:
        if not self._session:
            return 0
        enriched = 0
        for rec in records:
            parcel = rec.get("legal", "")
            if not parcel:
                continue
            match = self._api_lookup_parcel(parcel)
            if match:
                if not rec.get("prop_address") and match.get("site_addr"):
                    rec["prop_address"] = match["site_addr"]
                    rec["prop_city"]    = match.get("site_city") or "Cleveland"
                    rec["prop_zip"]     = match.get("site_zip", "")
                if match.get("mail_addr"):
                    rec["mail_address"] = match["mail_addr"]
                    rec["mail_city"]    = match.get("mail_city", "")
                    rec["mail_state"]   = match.get("mail_state") or "OH"
                    rec["mail_zip"]     = match.get("mail_zip", "")
                if not rec.get("owner") and match.get("owner"):
                    rec["owner"] = match["owner"]
                rec["delinquent"]   = match.get("delinquent", False)
                rec["delinq_amt"]   = match.get("delinq_amt", "")
                rec["homestead"]    = match.get("homestead", False)
                rec["appraised"]    = match.get("appraised", "")
                rec["out_of_state"] = match.get("out_of_state", False)
                rec["luc"]          = match.get("luc", "")
                rec["luc_desc"]     = match.get("luc_desc", "")
                enriched += 1
            time.sleep(0.3)
        return enriched

    def _api_lookup_parcel(self, parcel: str) -> Optional[dict]:
        if not self._session:
            return None
        try:
            log.info("Parcel API lookup: %s", parcel)
            url = f"https://myplace.cuyahogacounty.gov/MyPlaceService.svc/ParcelsAndValuesByAnySearchByAndCity/{parcel}?city=99&searchBy=Parcel"
            r = self._session.get(url, timeout=15)
            if r.status_code == 200:
                import json as _json
                data = _json.loads(r.json())
                if data and data[0]:
                    attrs = data[0][0]
                    owner     = normalize_name(attrs.get("DEEDED_OWNER") or "")
                    site_addr = (attrs.get("PHYSICAL_ADDRESS") or "").strip().title()
                    city      = (attrs.get("PARCEL_CITY") or "Cleveland").strip().title()
                    zipcode   = str(attrs.get("PARCEL_ZIP") or "")
                    rec = {
                        "owner":        owner,
                        "site_addr":    site_addr,
                        "site_city":    city,
                        "site_zip":     zipcode,
                        "mail_addr":    "",
                        "mail_city":    "",
                        "mail_state":   "OH",
                        "mail_zip":     "",
                        "parcel":       parcel,
                        "delinquent":   False,
                        "delinq_amt":   "",
                        "homestead":    False,
                        "appraised":    str(attrs.get("CERTIFIED_TAX_TOTAL") or ""),
                        "out_of_state": False,
                        "luc":          "",
                        "luc_desc":     "",
                    }
                    if owner or site_addr:
                        self._by_parcel[parcel] = rec
                        log.info("Parcel enriched: %s -> %s, %s", parcel, site_addr, city)
                        return rec
        except Exception as e:
            log.debug("MyPlace WCF lookup failed: %s", e)
        return None

    def _lookup_luc(self, parcel: str) -> str:
        return ""



# ===========================================================================
# Scoring
# ===========================================================================

class LeadScorer:
    WEEK_AGO = datetime.now() - timedelta(days=7)

    @staticmethod
    def score(rec: dict, all_recs: list) -> tuple:
        flags, points = [], 30
        cat   = rec.get("cat", "")
        dtype = rec.get("doc_type", "")
        owner = rec.get("owner", "")
        amt   = rec.get("amount")

        if cat == "LP":      flags.append("Lis pendens");        points += 10
        if cat == "NOFC":    flags.append("Pre-foreclosure");    points += 10
        if cat == "TAXDEED": flags.append("Tax deed");           points += 10
        if cat == "JUD":     flags.append("Judgment lien");      points += 10
        if cat == "CODE":
            flags.append("Code violation")
            points += 15
            sev = rec.get("viol_severity", "")
            if sev == "high":
                flags.append("Chief Approved violation")
                points += 15
            elif sev == "medium":
                points += 5
        if cat == "CONDEMN": flags.append("Condemned property"); points += 20
        if cat == "LIEN":
            if dtype in ("LNIRS","LNFED","LNCORPTX"): flags.append("Tax lien")
            elif dtype == "LNMECH": flags.append("Mechanic lien")
            elif dtype == "LNHOA":  flags.append("HOA lien")
            else: flags.append("Lien")
            points += 10
        if cat == "PRO":   flags.append("Probate / estate"); points += 10
        if cat == "RELLP": flags.append("Lis pendens");      points += 5

        norm = normalize_name(owner)
        if norm:
            owner_cats = {r["cat"] for r in all_recs
                          if normalize_name(r.get("owner","")) == norm}
            if "LP" in owner_cats and owner_cats & {"NOFC","TAXDEED"}:
                points += 20
            if owner_cats & {"NOFC","LP"} and owner_cats & {"CODE","CONDEMN"}:
                flags.append("Foreclosure + violation"); points += 25

        if amt:
            if amt > 100_000: flags.append("High debt (>$100k)"); points += 15
            elif amt > 50_000: points += 10

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

        if rec.get("delinquent"):
            flags.append("Delinquent taxes"); points += 15
        if rec.get("out_of_state"):
            flags.append("Out-of-state owner"); points += 10
        if rec.get("homestead") is False and rec.get("prop_address"):
            flags.append("No homestead exemption"); points += 5

        return min(100, max(0, points)), list(dict.fromkeys(flags))


# ===========================================================================
# GHL CSV
# ===========================================================================

GHL_COLS = [
    "First Name","Last Name","Mailing Address","Mailing City","Mailing State","Mailing Zip",
    "Property Address","Property City","Property State","Property Zip",
    "Lead Type","Document Type","Date Filed","Document Number","Amount/Debt Owed",
    "Seller Score","Motivated Seller Flags","Source","Public Records URL",
    "Parcel Number","Appraised Value","Delinquent Taxes","Delinquent Amount",
    "Homestead Exemption","Out-of-State Owner","Neighborhood","Violation Status",
    "Violation Description","Violation Severity",
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
        w = csv.DictWriter(f, fieldnames=GHL_COLS)
        w.writeheader()
        for r in records:
            fn, ln = split_name(r.get("owner", ""))
            amt = r.get("amount")
            w.writerow({
                "First Name": fn, "Last Name": ln,
                "Mailing Address":   r.get("mail_address", ""),
                "Mailing City":      r.get("mail_city", ""),
                "Mailing State":     r.get("mail_state", "OH"),
                "Mailing Zip":       r.get("mail_zip", ""),
                "Property Address":  r.get("prop_address", ""),
                "Property City":     r.get("prop_city", "Cleveland"),
                "Property State":    r.get("prop_state", "OH"),
                "Property Zip":      r.get("prop_zip", ""),
                "Lead Type":         r.get("cat_label", ""),
                "Document Type":     r.get("doc_type", ""),
                "Date Filed":        r.get("filed", ""),
                "Document Number":   r.get("doc_num", ""),
                "Amount/Debt Owed":  f"${amt:,.2f}" if amt else "",
                "Seller Score":      r.get("score", 0),
                "Motivated Seller Flags": "; ".join(r.get("flags", [])),
                "Source":            r.get("source", "Court Docket"),
                "Public Records URL": r.get("clerk_url", ""),
                "Parcel Number":     r.get("legal", ""),
                "Appraised Value":   r.get("appraised", ""),
                "Delinquent Taxes":  "Yes" if r.get("delinquent") else "No",
                "Delinquent Amount": r.get("delinq_amt", ""),
                "Homestead Exemption": "Yes" if r.get("homestead") else "No",
                "Out-of-State Owner": "Yes" if r.get("out_of_state") else "No",
                "Neighborhood":      r.get("neighborhood", ""),
                "Violation Status":  r.get("viol_status", ""),
                "Violation Description": r.get("viol_desc", ""),
                "Violation Severity": r.get("viol_severity", "").title(),
            })
    log.info("GHL CSV: %d rows -> %s", len(records), path)


# ===========================================================================
# Save
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

        output = {**data, "records": merged, "total": len(merged),
                  "with_address": sum(1 for r in merged
                                      if r.get("prop_address") or r.get("mail_address"))}
        with open(path, "w", encoding="utf-8") as f:
            json.dump(output, f, indent=2, default=str)
        log.info("Saved %s (%d records)", path, len(merged))


# ===========================================================================
# Main
# ===========================================================================

async def main():
    log.info("=" * 60)
    log.info("Cuyahoga County Lead Scraper — Court Docket + Code Violations")
    log.info("Range: %s -> %s",
             (datetime.now() - timedelta(days=LOOKBACK_DAYS)).strftime("%m/%d/%Y"),
             datetime.now().strftime("%m/%d/%Y"))
    log.info("=" * 60)

    # 1. Load parcel lookup
    parcel = ParcelLookup()
    log.info("Loading parcel data ...")
    parcel.load()

    # 2. Scrape sheriff sales
    scraper = SheriffScraper()
    await scraper.run()
    records = list(scraper.raw_records)
    log.info("Sheriff sale records: %d", len(records))

    # 3. Scrape code violations + condemnations
    code_scraper = CodeViolationScraper()
    code_scraper.run()
    records.extend(code_scraper.raw_records)
    log.info("Total records after adding violations: %d", len(records))

    # 3b. Enrich violation types from Accela (cached — only fetches new records)
    accela = AccelaScraper()
    accela.load_cache()
    accela.enrich_violations(records)

    # 4. Enrich all records with parcel owner/address data
    log.info("Enriching records with parcel data ...")
    enriched = parcel.enrich_records(records)
    log.info("Enriched %d/%d records with parcel data", enriched, len(records))

    # 4b. Filter to SFH + Vacant Land using available signals
    # LUC not available from these endpoints — infer from description and owner signals
    EXCLUDE_KEYWORDS = {
        "CONDO", "CONDOMINIUM", "APARTMENTS", "APT", "COMMERCIAL",
        "INDUSTRIAL", "WAREHOUSE", "RETAIL", "OFFICE", "PLAZA",
        "SHOPPING", "CHURCH", "SCHOOL", "HOSPITAL", "UNIVERSITY",
        "LLC PORTFOLIO", "HOLDINGS LLC",
    }
    def is_sfh_or_land(rec: dict) -> bool:
        # Always keep if no address info to filter on
        addr  = (rec.get("prop_address") or "").upper()
        owner = (rec.get("owner") or "").upper()
        legal = (rec.get("legal_desc") or rec.get("legal") or "").upper()
        combined = f"{addr} {owner} {legal}"
        # Exclude known non-SFH keywords
        if any(kw in combined for kw in EXCLUDE_KEYWORDS):
            return False
        return True

    before_sfh = len(records)
    records = [r for r in records if is_sfh_or_land(r)]
    log.info("SFH+Land filter: %d → %d records (removed %d non-qualifying)",
             before_sfh, len(records), before_sfh - len(records))

    # 5. Score all records
    for rec in records:
        rec["score"], rec["flags"] = LeadScorer.score(rec, records)

    # 5b. Parcel cross-reference — boost records where same parcel has multiple hit types
    # Groups by parcel number, much more reliable than owner name matching
    parcel_cats = defaultdict(set)
    parcel_recs = defaultdict(list)
    for rec in records:
        p = rec.get("legal", "")
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
            # Multiple violation types on same parcel — smaller boost
            for rec in parcel_recs[p]:
                if "Multi-hit parcel" not in rec["flags"]:
                    rec["flags"].append("Multi-hit parcel")
                rec["score"] = min(100, rec["score"] + 10)

    log.info("Cross-reference: %d parcels with foreclosure + violation", multi_hit_parcels)

    # 6. Deduplicate
    seen = {}
    for rec in records:
        key = rec.get("doc_num") or rec.get("clerk_url") or str(id(rec))
        if key not in seen or rec["score"] > seen[key]["score"]:
            seen[key] = rec
    records = sorted(seen.values(), key=lambda r: r["score"], reverse=True)
    log.info("Unique records after dedup: %d", len(records))

    # 7. Save
    with_addr = sum(1 for r in records if r.get("prop_address") or r.get("mail_address"))
    output = {
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "source":     "Cuyahoga County Court Docket + Code Violations + Manual Uploads",
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
