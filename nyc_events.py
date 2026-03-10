#!/usr/bin/env python3
"""
Event aggregator (polite, robots-aware, dedupe fixed, simplified output)

Output in README.md between markers:

<!-- NYC_EVENTS_START -->
<!-- NYC_EVENTS_END -->

Table columns: EVENTNAME | LINK | SOURCE

Dependencies:
  pip install requests beautifulsoup4 feedparser thefuzz

Optional:
  pip install python-Levenshtein  # speeds up thefuzz
"""

from __future__ import annotations
import os
import re
import time
import logging
import json
import calendar
from dataclasses import dataclass
from datetime import datetime, date
from typing import List, Optional, Dict, Tuple
from urllib.parse import urlparse, urljoin
from urllib import robotparser

import requests
from bs4 import BeautifulSoup
import feedparser
from thefuzz import fuzz

# ---------- Config ----------
USER_AGENT = os.environ.get("EVENT_DIGEST_USER_AGENT", "NYCEventDigestBot/1.0 (+contact:you@example.com)")
REQUEST_TIMEOUT = 15
# minimum seconds between requests to same domain
PER_DOMAIN_DELAY = float(os.environ.get("PER_DOMAIN_DELAY", "0.8"))
MAX_RETRIES = 2

START_MARKER = "<!-- NYC_EVENTS_START -->"
END_MARKER = "<!-- NYC_EVENTS_END -->"
README_PATH = os.environ.get("README_PATH", "README.md")

logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO").upper(), format="%(levelname)s: %(message)s")
logger = logging.getLogger("event-agg")

# ---------- Data model (simplified per request) ----------
@dataclass
class Row:
    name: str
    link: str
    source: str

# ---------- Politeness / robots ----------

class RobotsManager:
    """Cache RobotFileParser per origin and enforce allowed() checks."""
    def __init__(self, user_agent: str):
        self.user_agent = user_agent
        self.parsers: Dict[str, robotparser.RobotFileParser] = {}

    def allowed(self, url: str) -> bool:
        parsed = urlparse(url)
        origin = f"{parsed.scheme}://{parsed.netloc}"
        if origin not in self.parsers:
            rp = robotparser.RobotFileParser()
            robots_url = urljoin(origin, "/robots.txt")
            try:
                rp.set_url(robots_url)
                rp.read()
            except Exception:
                # Conservative fallback: allow when robots can't be fetched
                logger.debug("Could not fetch robots.txt for %s; allowing by default.", origin)
                rp = None
            self.parsers[origin] = rp
        rp = self.parsers[origin]
        if rp is None:
            return True
        try:
            return rp.can_fetch(self.user_agent, url)
        except Exception:
            return True

# per-domain rate limiting
class DomainThrottle:
    def __init__(self, min_delay_seconds: float):
        self.min_delay_seconds = min_delay_seconds
        self.last_request: Dict[str, float] = {}

    def wait(self, url: str):
        domain = urlparse(url).netloc
        now = time.time()
        last = self.last_request.get(domain, 0)
        wait_for = self.min_delay_seconds - (now - last)
        if wait_for > 0:
            logger.debug("Throttling: sleeping %.2fs for domain %s", wait_for, domain)
            time.sleep(wait_for)
        self.last_request[domain] = time.time()

# ---------- HTTP session with polite fetching ----------
session = requests.Session()
session.headers.update({"User-Agent": USER_AGENT})
robots = RobotsManager(USER_AGENT)
throttle = DomainThrottle(PER_DOMAIN_DELAY)

def polite_get(url: str, params: dict = None) -> Optional[str]:
    """Fetch text while respecting robots.txt and domain throttle + retries."""
    if not robots.allowed(url):
        logger.info("Blocked by robots.txt: %s", url)
        return None

    # simple retry/backoff on 5xx or transient network errors
    attempt = 0
    while attempt <= MAX_RETRIES:
        attempt += 1
        try:
            throttle.wait(url)
            resp = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
            if 200 <= resp.status_code < 300:
                return resp.text
            # treat 4xx as permanent failure
            if 400 <= resp.status_code < 500:
                logger.warning("HTTP %s for %s", resp.status_code, url)
                return None
            # for 5xx, retry
            logger.warning("Server error %s for %s (attempt %d)", resp.status_code, url, attempt)
        except requests.RequestException as e:
            logger.warning("Request error for %s: %s (attempt %d)", url, e, attempt)
        # exponential backoff small
        backoff = 0.5 * (2 ** (attempt - 1))
        time.sleep(backoff)
    logger.error("Exhausted retries for %s", url)
    return None

# ---------- Parsing helpers ----------
def extract_jsonld(soup: BeautifulSoup) -> List[dict]:
    results = []
    for script in soup.find_all("script", {"type": "application/ld+json"}):
        raw = script.string
        if not raw:
            continue
        try:
            obj = json.loads(raw)
        except Exception:
            # sometimes multiple JSON objects glued together; try find first {...}
            try:
                m = re.search(r"\{.*\}", raw, flags=re.DOTALL)
                if m:
                    obj = json.loads(m.group(0))
                else:
                    continue
            except Exception:
                continue
        if isinstance(obj, list):
            for item in obj:
                if isinstance(item, dict):
                    results.append(item)
        elif isinstance(obj, dict):
            results.append(obj)
    return results

def normalize_title(t: str) -> str:
    s = (t or "").strip()
    s = re.sub(r"\s+", " ", s)
    return s

# ---------- Site parsers (yield Row) ----------
def fetch_rss_feed(url: str, source_name: str) -> List[Row]:
    out: List[Row] = []
    logger.debug("Fetching RSS: %s", url)
    # feedparser supports passing agent via parse() but many versions ignore it;
    # we fetch raw with polite_get then parse to ensure UA is honored.
    text = polite_get(url)
    if not text:
        return out
    feed = feedparser.parse(text)
    for entry in getattr(feed, "entries", []):
        title = getattr(entry, "title", "") or ""
        link = getattr(entry, "link", "") or ""
        if not title:
            continue
        out.append(Row(name=title.strip(), link=link.strip() or url, source=f"RSS: {source_name}"))
    logger.info("RSS %s -> %d items", source_name, len(out))
    return out

def fetch_genre_events(url: str = "https://genreevents.com/downstate-new-york/") -> List[Row]:
    out: List[Row] = []
    html = polite_get(url)
    if not html:
        return out
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table")
    if not table:
        logger.debug("GenreEvents: no table found")
        return out
    rows = table.find_all("tr")
    if not rows:
        return out
    # header mapping by text
    header_cells = rows[0].find_all(["th", "td"])
    headers = [c.get_text(" ", strip=True).lower() for c in header_cells]
    idx = {h: i for i, h in enumerate(headers)}
    def find_col(*cands):
        for cand in cands:
            cand_l = cand.lower()
            for h, i in idx.items():
                if cand_l == h or cand_l in h:
                    return i
        return None
    i_name = find_col("name of event", "event")
    if i_name is None:
        i_name = 0
    for tr in rows[1:]:
        cells = tr.find_all("td")
        if not cells:
            continue
        name_cell = cells[i_name] if i_name < len(cells) else None
        if not name_cell:
            continue
        a = name_cell.find("a", href=True)
        name = (a.get_text(" ", strip=True) if a else name_cell.get_text(" ", strip=True)).strip()
        href = (a["href"].strip() if a and a.has_attr("href") else url)
        href = urljoin(url, href)
        if name:
            out.append(Row(name=name, link=href, source="GenreEvents"))
    logger.info("GenreEvents -> %d items", len(out))
    return out

def fetch_ny_event_radar(url: str = "https://ny-event-radar.com/") -> List[Row]:
    out: List[Row] = []
    html = polite_get(url)
    if not html:
        return out
    soup = BeautifulSoup(html, "html.parser")
    # Try JSON-LD
    objs = extract_jsonld(soup)
    for obj in objs:
        typ = obj.get("@type") or obj.get("type")
        if typ and ("Event" in str(typ) or "event" in str(typ).lower()):
            name = obj.get("name") or obj.get("headline") or ""
            link = obj.get("url") or url
            if name:
                out.append(Row(name=name.strip(), link=link.strip(), source="NY Event Radar (json-ld)"))
    # Heuristic fallback: linked headings
    if not out:
        for a in soup.select("a[href]"):
            title = a.get_text(" ", strip=True)
            if title and len(title) > 6:
                href = urljoin(url, a["href"])
                out.append(Row(name=title.strip(), link=href, source="NY Event Radar (heuristic)"))
    logger.info("NY Event Radar -> %d items", len(out))
    return out

def fetch_parks_socrata(start: Optional[date]=None, end: Optional[date]=None) -> List[Row]:
    """
    Fetch NYC Parks 'upcoming' dataset; we will treat returned rows as events without requiring date.
    """
    endpoint = "https://data.cityofnewyork.us/resource/w3wp-dpdi.json"
    out: List[Row] = []
    # Build a safe query; if start/end provided, include them; if not, just fetch a modest limit
    params = {"$limit": 500, "$order": "start_date ASC"}
    if start and end:
        params["$where"] = f"start_date between '{start.isoformat()}T00:00:00' and '{end.isoformat()}T23:59:59'"
    # We'll use polite_get but Socrata is JSON; use requests with UA after robots check + throttle
    if not robots.allowed(endpoint):
        logger.info("Parks endpoint disallowed by robots.txt: %s", endpoint)
        return out
    throttle.wait(endpoint)
    try:
        resp = session.get(endpoint, params=params, timeout=REQUEST_TIMEOUT)
        if resp.status_code != 200:
            logger.warning("Parks API returned %s", resp.status_code)
            return out
        data = resp.json()
    except Exception as e:
        logger.warning("Parks request failed: %s", e)
        return out
    for row in data if isinstance(data, list) else []:
        name = (row.get("event_name") or row.get("name") or "").strip()
        link = (row.get("event_url") or row.get("event_link") or "").strip() or "https://www.nycgovparks.org/events"
        if name:
            out.append(Row(name=name, link=link, source="NYC Parks (Socrata)"))
    logger.info("NYC Parks -> %d items", len(out))
    return out

# ---------- Aggregation & dedupe ----------
def dedupe_rows(rows: List[Row]) -> List[Row]:
    """
    Deduplicate rows. Rules:
      - Keep first occurrence when exact link matches.
      - If no link or different links, dedupe only when title fuzzy match >= 95.
      - Preserves at least one entry for near-duplicates (prevents collapsing many into one).
    """
    seen_links = set()
    uniques: List[Row] = []
    for r in rows:
        # normalize
        rname = normalize_title(r.name)
        rlink = (r.link or "").strip()
        if rlink and rlink in seen_links:
            logger.debug("Dropping duplicate by exact link: %s", rlink)
            continue
        # compare to existing uniques by fuzzy title
        is_dup = False
        for u in uniques:
            # exact link match
            if rlink and u.link and (rlink == u.link):
                is_dup = True
                break
            score = fuzz.token_sort_ratio(rname.lower(), normalize_title(u.name).lower())
            if score >= 95:
                # treat as duplicate only when extremely high similarity
                is_dup = True
                logger.debug("Dropping duplicate by fuzzy match %d: %s ~ %s", score, r.name, u.name)
                break
        if not is_dup:
            uniques.append(r)
            if rlink:
                seen_links.add(rlink)
    logger.info("Deduped %d -> %d", len(rows), len(uniques))
    return uniques

# ---------- README update ----------
def update_readme(rows: List[Row]) -> None:
    # Build markdown block
    now = datetime.now().strftime("%Y-%m-%d")
    lines = []
    lines.append(f"### NYC Event Digest (Updated: {now})")
    lines.append(f"*Found {len(rows)} unique events.*")
    lines.append("")
    lines.append("| Event | Link | Source |")
    lines.append("| :--- | :--- | :--- |")
    for r in rows:
        name = r.name.replace("\n", " ").strip().replace("|", "\\|")
        link = r.link
        src = r.source.replace("|", "\\|")
        lines.append(f"| {name} | [Link]({link}) | {src} |")
    block = "\n".join(lines) + "\n"

    # Ensure README exists and has markers
    if not os.path.exists(README_PATH):
        with open(README_PATH, "w", encoding="utf-8") as f:
            f.write(f"{START_MARKER}\n{END_MARKER}\n")

    with open(README_PATH, "r", encoding="utf-8") as f:
        content = f.read()

    if START_MARKER not in content or END_MARKER not in content:
        content = content.rstrip() + f"\n\n{START_MARKER}\n{END_MARKER}\n"

    pattern = re.escape(START_MARKER) + r".*?" + re.escape(END_MARKER)
    replacement = START_MARKER + "\n\n" + block + "\n" + END_MARKER
    updated = re.sub(pattern, replacement, content, flags=re.DOTALL)

    with open(README_PATH, "w", encoding="utf-8") as f:
        f.write(updated)

    logger.info("%s updated with %d events", README_PATH, len(rows))

# ---------- Orchestration ----------
def main():
    logger.info("Starting event aggregation (robots-aware, polite).")
    all_rows: List[Row] = []

    # Sources: always attempt (we no longer strictly require date overlap)
    # Parks: may be limited to upcoming 14 days but useful
    all_rows.extend(fetch_parks_socrata())

    # RSS sources
    all_rows.extend(fetch_rss_feed("https://www.theskint.com/feed/", "The Skint"))
    all_rows.extend(fetch_rss_feed("https://www.thrillist.com/rss/locations/new-york", "Thrillist"))

    # HTML table sources
    all_rows.extend(fetch_genre_events())

    # NY Event Radar
    all_rows.extend(fetch_ny_event_radar())

    # Normalize order: prefer Parks then genre then RSS then other
    logger.info("Collected total rows before dedupe: %d", len(all_rows))

    # If you still want to limit sources manually, you can filter here.

    deduped = dedupe_rows(all_rows)

    # Sort alphabetically for stable output
    deduped.sort(key=lambda x: (x.source, x.name.lower()))

    if deduped:
        update_readme(deduped)
    else:
        logger.info("No events collected; README not updated.")

if __name__ == "__main__":
    main()
