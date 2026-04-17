"""
Cuyahoga County Motivated Seller Lead Scraper
Scrapes Cuyahoga Common Pleas Court Sheriff Sale docket.
Real data format (from logs):
  Attorney: ELLEN FORNASH Appraised $120,000.00 Case #: CV19924853
  Minimum Bid $80,000.00 Residential PRIMELENDING, A PLAINSCAPITAL COMPANY
  VS Sale Date 1/12/2026 MITCHELL/RUTH/ Status CANCELLED BY ATTORNEY
  Parcel # Address Description 703-01-013 1395 SOUTH BELVOIR BOULEVARD
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
LOOKBACK_DAYS    = int(os.getenv("LOOKBACK_DAYS", "7"))
REPO_ROOT        = Path(__file__).resolve().parent.parent
DASHBOARD_JSON   = REPO_ROOT / "dashboard" / "records.json"
DATA_JSON        = REPO_ROOT / "data" / "records.json"
GHL_CSV          = REPO_ROOT / "data" / "ghl_export.csv"
RETRY            = 3


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

    # ------------------------------------------------------------------
    async def run(self):
        log.info("Scraping Cuyahoga Common Pleas Court docket ...")
        log.info("Date range: %s - %s", self.df_str, self.dt_str)
        self._scrape_sheriff()
        if not self.raw_records and PLAYWRIGHT_AVAILABLE:
            log.info("Trying Playwright fallback ...")
            await self._playwright_scrape()
        log.info("Court docket: %d records collected", len(self.raw_records))

    # ------------------------------------------------------------------
    def _scrape_sheriff(self):
        log.info("Trying Sheriff Sale search ...")
        try:
            # GET page for hidden fields
            r = self.session.get(SHERIFF_SALE_URL, timeout=30)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "lxml")

            form_data = {}
            for inp in soup.find_all("input"):
                n = inp.get("name", "")
                v = inp.get("value", "")
                if n:
                    form_data[n] = v

            # Fill date fields
            for k in list(form_data.keys()):
                kl = k.lower()
                if any(x in kl for x in ["datefrom","startdate","fromdate"]):
                    form_data[k] = self.df_str
                elif any(x in kl for x in ["dateto","enddate","todate"]):
                    form_data[k] = self.dt_str

            # Common field names
            for fname in ["dateFrom","dateTo","txtDateFrom","txtDateTo",
                          "StartDate","EndDate","dfrom","dto"]:
                kl = fname.lower()
                if any(x in kl for x in ["from","start"]):
                    form_data[fname] = self.df_str
                else:
                    form_data[fname] = self.dt_str

            form_data["__EVENTTARGET"] = ""
            form_data["__EVENTARGUMENT"] = ""

            # Find submit button
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

    # ------------------------------------------------------------------
    def _parse_html(self, html: str) -> list[dict]:
        """
        Parse sheriff sale results. Each property is a text blob in one cell.
        Real format:
          Attorney: NAME Appraised $X Case #: CVxxxxxx
          Minimum Bid $X TYPE PLAINTIFF VS Sale Date M/D/YYYY
          OWNER/NAME/ Status ... Parcel # Address Description
          NNN-NN-NNN STREET_NUMBER STREET_NAME ...
        """
        records = []
        soup = BeautifulSoup(html, "lxml")

        # Collect all text blocks that contain a CV case number
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

    # ------------------------------------------------------------------
    def _extract(self, text: str, row) -> Optional[dict]:
        """
        Extract fields using regex patterns matched to real Cuyahoga
        sheriff sale text format.
        """

        # ── Case number ──────────────────────────────────────────────
        case_m = re.search(r'Case\s*#\s*:?\s*(CV\s*\d+)', text, re.I)
        if not case_m:
            case_m = re.search(r'\b(CV\d{4,})\b', text, re.I)
        if not case_m:
            return None
        case_num = case_m.group(1).replace(" ", "").upper()

        # ── Attorney / Plaintiff ─────────────────────────────────────
        # "Attorney: ELLEN FORNASH Appraised"
        att_m = re.search(r'Attorney\s*:\s*([A-Z][A-Z\s,\.]+?)(?=\s+Appraised|\s+Case\s*#|\s+\$)', text, re.I)
        plaintiff = att_m.group(1).strip().title() if att_m else ""

        # ── Sale date ────────────────────────────────────────────────
        # "VS Sale Date 1/12/2026"
        date_m = re.search(r'Sale\s+Date\s+(\d{1,2}/\d{1,2}/\d{4})', text, re.I)
        if not date_m:
            date_m = re.search(r'\b(\d{1,2}/\d{1,2}/\d{4})\b', text)
        sale_date = date_m.group(1) if date_m else ""

        # Note: Sheriff sale listings show scheduled sale dates which may be
        # months in the past or future. We keep all active listings.
        # The filing/case date is what matters for our lookback window.
        # Don't filter by sale_date here.

        # ── Owner / Defendant ────────────────────────────────────────
        # "VS Sale Date 1/12/2026 MITCHELL/RUTH/ Status"
        owner_m = re.search(
            r'VS\s+Sale\s+Date\s+\d{1,2}/\d{1,2}/\d{4}\s+([A-Z][A-Z/\s,\.\-]{2,60}?)\s+Status',
            text, re.I
        )
        if not owner_m:
            # "VS MITCHELL/RUTH/ Status" without date in between
            owner_m = re.search(
                r'\bVS\b\s+([A-Z][A-Z/\s,\.\-]{2,60}?)\s+Status',
                text, re.I
            )
        owner = ""
        if owner_m:
            raw = owner_m.group(1).strip()
            parts = [p.strip() for p in re.split(r'[/,]', raw) if p.strip()]
            owner = " ".join(p.title() for p in parts)

        # ── Parcel number ─────────────────────────────────────────────
        # "703-01-013"
        parcel_m = re.search(r'\b(\d{3}-\d{2}-\d{3})\b', text)
        parcel = parcel_m.group(1) if parcel_m else ""

        # ── Property address ──────────────────────────────────────────
        # "703-01-013 1395 SOUTH BELVOIR BOULEVARD"
        prop_address = ""
        if parcel:
            # Address comes right after parcel number
            addr_m = re.search(
                re.escape(parcel) + r'\s+(\d{1,5}\s+[A-Z][A-Z0-9\s]{3,50})',
                text, re.I
            )
            if addr_m:
                # Trim at "A SINGLE" or "DWELLING" or similar description words
                addr_raw = addr_m.group(1)
                addr_raw = re.sub(r'\s+(A\s+SINGLE|DWELLING|COMMERCIAL|VACANT|LOT|WITH\s+).*$', '', addr_raw, flags=re.I)
                prop_address = addr_raw.strip().title()

        if not prop_address:
            # Fallback: find any street address pattern
            addr_m = re.search(
                r'\b(\d{3,5}\s+[A-Z][A-Z\s]{3,40}'
                r'(?:STREET|ST|AVENUE|AVE|DRIVE|DR|ROAD|RD|BOULEVARD|BLVD|'
                r'LANE|LN|COURT|CT|PLACE|PL|WAY|CIRCLE|CIR))\b',
                text, re.I
            )
            if addr_m:
                prop_address = addr_m.group(1).strip().title()

        # ── Zip code ──────────────────────────────────────────────────
        zip_m = re.search(r'\b(44\d{3})\b', text)
        prop_zip = zip_m.group(1) if zip_m else ""

        # ── Amount ───────────────────────────────────────────────────
        # "Appraised $120,000.00"
        amt_m = re.search(r'Appraised\s+\$?([\d,]+(?:\.\d{2})?)', text, re.I)
        if not amt_m:
            amt_m = re.search(r'Minimum\s+Bid\s+\$?([\d,]+)', text, re.I)
        amount = parse_amount(amt_m.group(1)) if amt_m else None

        # ── Link ─────────────────────────────────────────────────────
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
        }

    # ------------------------------------------------------------------
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
# Parcel Lookup — MyPlace + GIS enrichment
# ===========================================================================

class ParcelLookup:
    """
    Enriches leads with parcel data from multiple Cuyahoga County sources:
    1. MyPlace GIS API — owner, mailing address, assessed value, homestead
    2. Bulk parcel dataset — for batch owner name lookups
    """

    # MyPlace parcel service (from Geocortex directory)
    MYPLACE_URL = ("https://gis.cuyahogacounty.us/server/rest/services/"
                   "MyPLACE/Parcels_WMA_GJOIN_WGS84/MapServer/0/query")

    # CAMA parcel service with tax/assessment data — try new geospatial hub first
    CAMA_URLS = [
        # New CuyahogaGIS Hub (replacing old opendata site)
        "https://geospatial.gis.cuyahogacounty.gov/server/rest/services/Parcels/FeatureServer/0/query",
        "https://geospatial.gis.cuyahogacounty.gov/server/rest/services/CCFO/TaxParcels_WGS84/FeatureServer/0/query",
        # MyPlace service
        "https://gis.cuyahogacounty.us/server/rest/services/MyPLACE/Parcels_WMA_GJOIN_WGS84/MapServer/0/query",
        # Old opendata (deprecated but may still work)
        "https://data-cuyahoga.opendata.arcgis.com/datasets/ffaaa1651d5540419469375d680f3245_0/query",
    ]

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

        # Test each GIS URL but don't do bulk load yet
        # (bulk load returns empty from deprecated endpoints)
        # Instead we do per-parcel lookups during enrichment
        for url in self.CAMA_URLS:
            try:
                r = self._session.get(url, params={
                    "where": "PARCELNO='703-01-013'",
                    "outFields": "OWNER,PARCELNO,SITEADDR",
                    "f": "json",
                    "resultRecordCount": 1,
                    "returnGeometry": "false"
                }, timeout=15)
                if r.status_code == 200:
                    data = r.json()
                    if data.get("features"):
                        self._working_url = url
                        log.info("Parcel per-parcel URL: %s", url)
                        break
            except Exception:
                continue

        # Test MyPlace URL
        try:
            r = self._session.get(self.MYPLACE_URL, params={
                "where": "PARCELNO='703-01-013'",
                "outFields": "OWNER,PARCELNO",
                "f": "json",
                "resultRecordCount": 1,
                "returnGeometry": "false"
            }, timeout=15)
            if r.status_code == 200 and r.json().get("features"):
                if not self._working_url:
                    self._working_url = "https://gis.cuyahogacounty.us/server/rest/services/MyPLACE/Parcels_WMA_GJOIN_WGS84/MapServer/0/query"
                log.info("MyPlace per-parcel URL working")
        except Exception:
            pass

        if not self._working_url:
            log.info("Will use per-parcel lookups via: %s", self._working_url)
        else:
            log.warning("No parcel API working — will use MyPlace web scrape")

    def enrich_records(self, records: list) -> int:
        """Enrich records with parcel data. Returns count enriched."""
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
                rec["delinquent"]   = match.get("delinquent", False)
                rec["delinq_amt"]   = match.get("delinq_amt", "")
                rec["homestead"]    = match.get("homestead", False)
                rec["appraised"]    = match.get("appraised", "")
                rec["out_of_state"] = match.get("out_of_state", False)
                enriched += 1
            time.sleep(0.3)  # be polite to the API
        return enriched

    def _load_bulk(self) -> int:
        """Load bulk parcel data into memory indexes."""
        offset, size, total = 0, 1000, 0
        fields = ("OWNER,OWN1,SITEADDR,SITE_ADDR,SITESTREET,SITE_CITY,SITE_ZIP,"
                  "MAILADR1,ADDR_1,MAILCITY,CITY,STATE,MAILZIP,ZIP,"
                  "PARCELNO,PARID,APN,"
                  "DELINQUENT,DELINQ,DELINQ_AMT,BACK_TAX,"
                  "HOMESTEAD,HMSTD,HOMESTEAD_EXM,"
                  "APPVAL,APPRVAL,ASSESSED,LAND_VALUE,BLDG_VALUE,"
                  "OWNER_OCC,OUTOFSTATE,OUT_OF_STATE")
        try:
            while True:
                params = {
                    "where":             "1=1",
                    "outFields":         fields,
                    "f":                 "json",
                    "resultOffset":      offset,
                    "resultRecordCount": size,
                    "returnGeometry":    "false",
                }
                r = self._session.get(self._working_url, params=params, timeout=60)
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
            log.warning("Bulk load error: %s", e)
        log.info("Parcels loaded: %d", total)
        return total

    def _ingest(self, r: dict):
        def g(*keys):
            for k in keys:
                for key in [k, k.upper(), k.lower()]:
                    v = r.get(key)
                    if v and str(v).strip() not in ("", "None", "null", "0"):
                        return str(v).strip()
            return ""

        owner   = g("OWNER", "OWN1")
        parcel  = g("PARCELNO", "PARID", "APN")
        site    = g("SITEADDR", "SITE_ADDR", "SITESTREET")
        city    = g("SITE_CITY")
        zipcode = g("SITE_ZIP")

        rec = {
            "owner":       normalize_name(owner),
            "site_addr":   site,
            "site_city":   city or "Cleveland",
            "site_zip":    zipcode,
            "mail_addr":   g("MAILADR1", "ADDR_1"),
            "mail_city":   g("MAILCITY", "CITY"),
            "mail_state":  g("STATE") or "OH",
            "mail_zip":    g("MAILZIP", "ZIP"),
            "parcel":      parcel,
            # Enrichment fields
            "delinquent":  g("DELINQUENT", "DELINQ", "BACK_TAX") not in ("", "N", "0", "No"),
            "delinq_amt":  g("DELINQ_AMT"),
            "homestead":   g("HOMESTEAD", "HMSTD", "HOMESTEAD_EXM") not in ("", "N", "0", "No"),
            "appraised":   g("APPVAL", "APPRVAL", "ASSESSED"),
            "out_of_state":g("OUTOFSTATE", "OUT_OF_STATE") not in ("", "N", "0", "No"),
        }

        if owner:
            for v in name_variants(owner):
                self._by_owner[v].append(rec)
        if parcel:
            self._by_parcel[parcel] = rec
        if site:
            self._by_address[normalize_name(site)] = rec

    def lookup_parcel(self, parcel_num: str) -> Optional[dict]:
        """Look up by parcel number — most reliable."""
        if not parcel_num:
            return None
        clean = parcel_num.strip()
        hit = self._by_parcel.get(clean)
        if hit:
            return hit
        # Try without dashes
        nodash = clean.replace("-", "")
        for k, v in self._by_parcel.items():
            if k.replace("-", "") == nodash:
                return v
        # Try per-API lookup if not in bulk data
        return self._api_lookup_parcel(clean)

    def lookup_owner(self, name: str) -> Optional[dict]:
        """Look up by owner name."""
        if not name:
            return None
        for v in name_variants(name):
            hits = self._by_owner.get(v)
            if hits:
                return hits[0]
        return None

    def lookup_address(self, address: str) -> Optional[dict]:
        """Look up by property address."""
        if not address:
            return None
        norm = normalize_name(address)
        hit = self._by_address.get(norm)
        if hit:
            return hit
        # Try partial match
        for k, v in self._by_address.items():
            if norm in k or k in norm:
                return v
        return None

    def lookup(self, name: str) -> Optional[dict]:
        """Convenience method — look up by owner name."""
        return self.lookup_owner(name)

    def _api_lookup_parcel(self, parcel: str) -> Optional[dict]:
        """Per-parcel API lookup using MyPlace web scrape as fallback."""
        if not self._session:
            return None

        # Try GIS API first
        if not self._working_url:
            try:
                clean = parcel.replace("-","")
                r = self._session.get(self._working_url, params={
                    "where":          f"UPPER(REPLACE(PARCELNO,'-',''))='{clean}' OR UPPER(REPLACE(PARID,'-',''))='{clean}'",
                    "outFields":      "*",
                    "f":              "json",
                    "resultRecordCount": 1,
                    "returnGeometry": "false",
                }, timeout=15)
                if r.status_code == 200:
                    features = r.json().get("features", [])
                    if features:
                        self._ingest(features[0].get("attributes", {}))
                        hit = self._by_parcel.get(parcel)
                        if hit:
                            return hit
            except Exception as e:
                log.debug("GIS per-parcel lookup failed: %s", e)

        # Fallback: scrape MyPlace web page
        try:
            url = f"https://myplace.cuyahogacounty.gov/en-US/REPI.aspx?parcelid={parcel}"
            r = self._session.get(url, timeout=15)
            if r.status_code == 200:
                soup = BeautifulSoup(r.text, "lxml")
                rec = self._parse_myplace_html(soup, parcel)
                if rec:
                    self._by_parcel[parcel] = rec
                    return rec
        except Exception as e:
            log.debug("MyPlace web lookup failed: %s", e)

        return None

    def _parse_myplace_html(self, soup, parcel: str) -> Optional[dict]:
        """Parse MyPlace property detail page."""
        text = soup.get_text(" ", strip=True)

        def find_after(label: str, text: str, chars: int = 80) -> str:
            idx = text.upper().find(label.upper())
            if idx >= 0:
                return text[idx+len(label):idx+len(label)+chars].strip()
            return ""

        owner     = find_after("Owner Name:", text, 60)
        site_addr = find_after("Property Address:", text, 60)
        mail_addr = find_after("Mailing Address:", text, 60)
        city      = find_after("City:", text, 30)
        state     = find_after("State:", text, 10)
        zipcode   = find_after("Zip:", text, 10)
        appraised = find_after("Appraised Value:", text, 20)
        homestead = "homestead" in text.lower() and "yes" in text[text.lower().find("homestead"):text.lower().find("homestead")+50].lower()
        delinquent = "delinquent" in text.lower()

        if not owner and not site_addr:
            return None

        return {
            "owner":       normalize_name(owner),
            "site_addr":   site_addr,
            "site_city":   city or "Cleveland",
            "site_zip":    re.search(r'\b(44\d{3})\b', zipcode or text or "").group(1) if re.search(r'\b(44\d{3})\b', zipcode or text or "") else "",
            "mail_addr":   mail_addr,
            "mail_city":   city,
            "mail_state":  state or "OH",
            "mail_zip":    zipcode,
            "parcel":      parcel,
            "delinquent":  delinquent,
            "delinq_amt":  "",
            "homestead":   homestead,
            "appraised":   appraised,
            "out_of_state": False,
        }


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

        if cat == "LP":      flags.append("Lis pendens");      points += 10
        if cat == "NOFC":    flags.append("Pre-foreclosure");  points += 10
        if cat == "TAXDEED": flags.append("Tax deed");         points += 10
        if cat == "JUD":     flags.append("Judgment lien");    points += 10
        if cat == "LIEN":
            if dtype in ("LNIRS","LNFED","LNCORPTX"): flags.append("Tax lien")
            elif dtype == "LNMECH": flags.append("Mechanic lien")
            elif dtype == "LNHOA":  flags.append("HOA lien")
            else: flags.append("Lien")
            points += 10
        if cat == "PRO":  flags.append("Probate / estate"); points += 10
        if cat == "RELLP": flags.append("Lis pendens");     points += 5

        norm = normalize_name(owner)
        if norm:
            owner_cats = {r["cat"] for r in all_recs
                          if normalize_name(r.get("owner","")) == norm}
            if "LP" in owner_cats and owner_cats & {"NOFC","TAXDEED"}:
                points += 20

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
    "Homestead Exemption","Out-of-State Owner",
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
            })
    log.info("GHL CSV: %d rows -> %s", len(records), path)


# ===========================================================================
# Save (merges with manual uploads)
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
    log.info("Cuyahoga County Lead Scraper — Court Docket")
    log.info("Range: %s -> %s",
             (datetime.now() - timedelta(days=LOOKBACK_DAYS)).strftime("%m/%d/%Y"),
             datetime.now().strftime("%m/%d/%Y"))
    log.info("=" * 60)

    # 1. Load parcel data
    parcel = ParcelLookup()
    log.info("Loading parcel data ...")
    parcel.load()

    # 2. Scrape
    scraper = SheriffScraper()
    await scraper.run()
    records = scraper.raw_records

    # 3. Enrich addresses from parcel data
    log.info("Enriching records with parcel data ...")
    enriched = parcel.enrich_records(records)
    log.info("Enriched %d/%d records with parcel data", enriched, len(records))

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

    # 6. Save
    with_addr = sum(1 for r in records if r.get("prop_address") or r.get("mail_address"))
    output = {
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "source":     "Cuyahoga County Court Docket + Manual Uploads",
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
