#!/usr/bin/env python3
"""
nyc_events.py (extended)

Adds fetchers for:
  - donyc.com
  - Timeout (events calendar; month-aware URL configurable)
  - SecretNYC
  - NYC Bucket List

Output remains: EVENT | LINK | SOURCE in README between markers.

Dependencies:
  pip install requests beautifulsoup4 feedparser thefuzz

Env vars:
  TIMEOUT_URL - optional, override the Timeout calendar URL (if not set it will use the current month)
  PER_DOMAIN_DELAY - seconds between requests to same domain (default 0.8)
  EVENT_DIGEST_USER_AGENT - custom User-Agent if desired
  README_PATH - path to README file (default README.md)
"""

from __future__ import annotations
import os
import re
import time
import logging
import json
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
import feedparser
from thefuzz import fuzz

# ---------- Config ----------
USER_AGENT = os.environ.get("EVENT_DIGEST_USER_AGENT", "NYCEventDigestBot/1.0 (+contact:you@example.com)")
REQUEST_TIMEOUT = 15
PER_DOMAIN_DELAY = float(os.environ.get("PER_DOMAIN_DELAY", "0.8"))
MAX_RETRIES = 2

START_MARKER = "<!-- NYC_EVENTS_START -->"
END_MARKER = "<!-- NYC_EVENTS_END -->"
README_PATH = os.environ.get("README_PATH", "README.md")
TIMEOUT_URL_OVERRIDE = os.environ.get("TIMEOUT_URL")  # optional override

logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO").upper(), format="%(levelname)s: %(message)s")
logger = logging.getLogger("nyc-events")

session = requests.Session()
session.headers.update({"User-Agent": USER_AGENT, "Accept-Language": "en-US,en;q=0.9", "Accept": "text/html,application/xhtml+xml"})

def throttle_sleep(domain_delay=PER_DOMAIN_DELAY):
    time.sleep(domain_delay)

@dataclass
class Row:
    name: str
    link: str
    source: str

# ---------- Helpers ----------
def extract_jsonld(soup: BeautifulSoup):
    out = []
    for s in soup.find_all("script", {"type": "application/ld+json"}):
        raw = s.string
        if not raw:
            continue
        try:
            obj = json.loads(raw)
            if isinstance(obj, list):
                out.extend(obj)
            else:
                out.append(obj)
        except Exception:
            m = re.search(r"\{.*\}", raw, flags=re.DOTALL)
            if m:
                try:
                    obj = json.loads(m.group(0))
                    out.append(obj)
                except Exception:
                    continue
    return out

def polite_get_text(url: str, params: dict = None) -> Optional[str]:
    """Simple polite fetch: throttle + retries (no robots)."""
    attempt = 0
    while attempt <= MAX_RETRIES:
        attempt += 1
        try:
            throttle_sleep()
            resp = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
            if 200 <= resp.status_code < 300:
                return resp.text
            if 400 <= resp.status_code < 500:
                logger.warning("HTTP %s for %s", resp.status_code, url)
                return None
            logger.warning("Server error %s for %s (attempt %d)", resp.status_code, url, attempt)
        except requests.RequestException as e:
            logger.warning("Request error for %s: %s (attempt %d)", url, e, attempt)
        time.sleep(0.5 * (2 ** (attempt - 1)))
    logger.error("Failed to fetch %s after retries", url)
    return None

def normalize_title(t: str) -> str:
    return re.sub(r"\s+", " ", (t or "").strip())

def collect_anchors_by_selectors(soup: BeautifulSoup, base_url: str, selectors: List[str]) -> List[Row]:
    out = []
    seen = set()
    for sel in selectors:
        for a in soup.select(sel):
            if not a or not a.has_attr("href"):
                continue
            txt = a.get_text(" ", strip=True)
            href = urljoin(base_url, a["href"])
            txt = normalize_title(txt)
            if txt and len(txt) > 6:
                key = (txt.lower(), href)
                if key in seen:
                    continue
                seen.add(key)
                out.append(Row(name=txt, link=href, source="Generic"))
    return out

# ---------- Source fetchers ----------

def fetch_rss_feed(url: str, source_name: str) -> List[Row]:
    out = []
    logger.debug("Fetching RSS feed: %s", url)
    text = polite_get_text(url)
    if not text:
        return out
    feed = feedparser.parse(text)
    for e in getattr(feed, "entries", []):
        title = getattr(e, "title", "") or ""
        link = getattr(e, "link", "") or url
        if title:
            out.append(Row(name=normalize_title(title), link=link, source=f"RSS: {source_name}"))
    logger.info("RSS %s -> %d items", source_name, len(out))
    return out

def fetch_genre_events(url: str = "https://genreevents.com/downstate-new-york/") -> List[Row]:
    out = []
    html = polite_get_text(url)
    if not html:
        return out
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table")
    if not table:
        logger.debug("GenreEvents: no table found at %s", url)
        return out
    rows = table.find_all("tr")
    if not rows:
        return out
    for tr in rows[1:]:
        tds = tr.find_all("td")
        if not tds:
            continue
        cell = tds[0]
        a = cell.find("a", href=True)
        name = a.get_text(" ", strip=True) if a else cell.get_text(" ", strip=True)
        href = urljoin(url, a["href"]) if a and a.has_attr("href") else url
        name = normalize_title(name)
        if name:
            out.append(Row(name=name, link=href, source="GenreEvents"))
    logger.info("GenreEvents -> %d items", len(out))
    return out

# ----------------- New site: donyc.com -----------------
def fetch_donyc(url: str = "https://donyc.com/") -> List[Row]:
    """
    donyc.com is a blog-like site with event posts — attempt JSON-LD, RSS, and heuristics.
    """
    out = []
    html = polite_get_text(url)
    if not html:
        return out
    soup = BeautifulSoup(html, "html.parser")

    # JSON-LD
    for j in extract_jsonld(soup):
        typ = j.get("@type") or j.get("type") or ""
        if isinstance(typ, list):
            typ = typ[0] if typ else ""
        if "Event" in str(typ) or "event" in str(typ).lower() or "Article" in str(typ):
            name = j.get("name") or j.get("headline") or ""
            link = j.get("url") or url
            if name:
                out.append(Row(name=normalize_title(name), link=link, source="DoNYC (jsonld)"))

    # RSS
    # try common feed locations
    for feed_url in ("/feed/", "/rss/", "/?feed=rss2"):
        candidate = urljoin(url, feed_url)
        items = fetch_rss_feed(candidate, "DoNYC")
        if items:
            for r in items:
                r.source = "DoNYC (rss)"
            out.extend(items)
            break

    # Heuristic anchors if no JSON-LD / RSS
    if not out:
        selectors = ["h1 a", "h2 a", "h3 a", ".entry-title a", ".post-title a", "article a"]
        anchors = collect_anchors_by_selectors(soup, url, selectors)
        for a in anchors:
            a.source = "DoNYC (heuristic)"
        out.extend(anchors)

    logger.info("DoNYC -> %d items", len(out))
    return out

# ----------------- New site: Timeout events calendar -----------------
def build_timeout_url_for_current_month() -> str:
    """Return a Timeout events calendar URL for the current month (format used by Timeout site)."""
    if TIMEOUT_URL_OVERRIDE:
        return TIMEOUT_URL_OVERRIDE
    now = datetime.utcnow()
    # Example pattern: https://www.timeout.com/newyork/events-calendar/march-events-calendar
    month_name = now.strftime("%B").lower()
    return f"https://www.timeout.com/newyork/events-calendar/{month_name}-events-calendar"

def fetch_timeout_calendar(url: Optional[str] = None) -> List[Row]:
    out = []
    if not url:
        url = build_timeout_url_for_current_month()
    html = polite_get_text(url)
    if not html:
        return out
    soup = BeautifulSoup(html, "html.parser")

    # Try JSON-LD
    for j in extract_jsonld(soup):
        typ = j.get("@type") or j.get("type") or ""
        if isinstance(typ, list):
            typ = typ[0] if typ else ""
        if "Event" in str(typ) or "Article" in str(typ):
            name = j.get("name") or j.get("headline") or ""
            link = j.get("url") or url
            if name:
                out.append(Row(name=normalize_title(name), link=link, source="TimeOut (jsonld)"))

    # Try RSS via link rel alternate
    for link_tag in soup.select("link[rel='alternate']"):
        t = link_tag.get("type", "")
        if "rss" in t or "atom" in t:
            rss = urljoin(url, link_tag.get("href"))
            items = fetch_rss_feed(rss, "TimeOut")
            for r in items:
                r.source = "TimeOut (rss)"
            out.extend(items)
            break

    # Heuristic anchors
    selectors = [
        ".card__content a", ".card a", ".listing a",
        ".event-card a", ".component-article a",
        "h2 a", "h3 a", ".kicker a", ".teaser a"
    ]
    anchors = collect_anchors_by_selectors(soup, url, selectors)
    for a in anchors:
        a.source = "TimeOut (heuristic)"
    out.extend(anchors)

    logger.info("TimeOut -> %d items from %s", len(out), url)
    return out

# ----------------- New site: SecretNYC -----------------
def fetch_secretnyc(url: str = "https://secretnyc.co/") -> List[Row]:
    out = []
    html = polite_get_text(url)
    if not html:
        return out
    soup = BeautifulSoup(html, "html.parser")

    # JSON-LD
    for j in extract_jsonld(soup):
        typ = j.get("@type") or j.get("type") or ""
        if isinstance(typ, list):
            typ = typ[0] if typ else ""
        if "Event" in str(typ) or "Article" in str(typ):
            name = j.get("name") or j.get("headline") or ""
            link = j.get("url") or url
            if name:
                out.append(Row(name=normalize_title(name), link=link, source="SecretNYC (jsonld)"))

    # RSS
    for feed_url in ("/feed/", "/rss/"):
        items = fetch_rss_feed(urljoin(url, feed_url), "SecretNYC")
        if items:
            for r in items:
                r.source = "SecretNYC (rss)"
            out.extend(items)
            break

    # Heuristic anchors
    selectors = ["h2.entry-title a", ".post-title a", ".featured a", "article a", "a[href]"]
    anchors = collect_anchors_by_selectors(soup, url, selectors)
    for a in anchors:
        a.source = "SecretNYC (heuristic)"
    out.extend(anchors)

    logger.info("SecretNYC -> %d items", len(out))
    return out

# ----------------- New site: NYC Bucket List -----------------
def fetch_nyc_bucket_list(url: str = "https://www.nycbucketlist.com/") -> List[Row]:
    out = []
    html = polite_get_text(url)
    if not html:
        return out
    soup = BeautifulSoup(html, "html.parser")

    # JSON-LD check
    for j in extract_jsonld(soup):
        typ = j.get("@type") or j.get("type") or ""
        if isinstance(typ, list):
            typ = typ[0] if typ else ""
        if "Event" in str(typ) or "Article" in str(typ):
            name = j.get("name") or j.get("headline") or ""
            link = j.get("url") or url
            if name:
                out.append(Row(name=normalize_title(name), link=link, source="NYC Bucket List (jsonld)"))

    # RSS
    for feed_url in ("/feed/", "/?format=rss"):
        items = fetch_rss_feed(urljoin(url, feed_url), "NYC Bucket List")
        if items:
            for r in items:
                r.source = "NYC Bucket List (rss)"
            out.extend(items)
            break

    # Heuristic anchors
    selectors = ["h2.entry-title a", ".post a", ".article a", "article a", "a[href]"]
    anchors = collect_anchors_by_selectors(soup, url, selectors)
    for a in anchors:
        a.source = "NYC Bucket List (heuristic)"
    out.extend(anchors)

    logger.info("NYC Bucket List -> %d items", len(out))
    return out

# ----------------- NY Event Radar (improved fallback kept) -----------------
def fetch_ny_event_radar(base_url: str = "https://ny-event-radar.com/") -> List[Row]:
    out = []
    tried_urls = [base_url, urljoin(base_url, "page/1/")]
    for u in tried_urls:
        logger.debug("Trying NY Event Radar URL: %s", u)
        html = polite_get_text(u)
        if not html:
            continue
        soup = BeautifulSoup(html, "html.parser")

        # JSON-LD
        for j in extract_jsonld(soup):
            typ = j.get("@type") or j.get("type") or ""
            if isinstance(typ, list):
                typ = typ[0] if typ else ""
            if "Event" in str(typ) or "Article" in str(typ):
                name = j.get("name") or j.get("headline") or ""
                link = j.get("url") or u
                if name:
                    out.append(Row(name=normalize_title(name), link=link, source="NY Event Radar (jsonld)"))

        # attempt RSS link
        rss_link = None
        for link_tag in soup.select("link[rel='alternate']"):
            t = link_tag.get("type", "")
            if "rss" in t or "atom" in t:
                rss_link = urljoin(u, link_tag.get("href"))
                break
        if rss_link:
            logger.info("NY Event Radar RSS found: %s", rss_link)
            items = fetch_rss_feed(rss_link, "NY Event Radar")
            for r in items:
                r.source = "NY Event Radar (rss)"
            out.extend(items)
            continue

        # heading/article anchors heuristics
        selectors = ("h1 a", "h2 a", "h3 a", ".entry-title a", ".post-title a", "article a", "a[href]")
        anchors = collect_anchors_by_selectors(soup, u, list(selectors))
        for a in anchors:
            a.source = "NY Event Radar (heuristic)"
        out.extend(anchors)

        if out:
            break

    logger.info("NY Event Radar -> %d items", len(out))
    if not out:
        logger.warning("NY Event Radar: no items found — possibly JS-rendered or using API endpoints.")
    return out

# ---------- Dedupe ----------
def dedupe_rows(rows: List[Row]) -> List[Row]:
    uniques: List[Row] = []
    seen_links = set()
    for r in rows:
        if r.link and r.link in seen_links:
            continue
        is_dup = False
        for u in uniques:
            if r.link and u.link and r.link == u.link:
                is_dup = True
                break
            score = fuzz.token_sort_ratio(r.name.lower(), u.name.lower())
            if score >= 95:
                is_dup = True
                break
        if not is_dup:
            uniques.append(r)
            if r.link:
                seen_links.add(r.link)
    logger.info("Deduped %d -> %d", len(rows), len(uniques))
    return uniques

# ---------- README update ----------
def update_readme(rows: List[Row]):
    now = datetime.utcnow().strftime("%Y-%m-%d")
    lines = []
    lines.append(f"### NYC Event Digest (Updated: {now})")
    lines.append(f"*Found {len(rows)} unique events.*")
    lines.append("")
    lines.append("| Event | Link | Source |")
    lines.append("| :--- | :--- | :--- |")
    for r in rows:
        name = r.name.replace("|", "\\|")
        link = r.link
        src = r.source.replace("|", "\\|")
        lines.append(f"| {name} | [Link]({link}) | {src} |")
    block = "\n".join(lines) + "\n"

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

# ---------- Orchestrator ----------
def main():
    logger.info("Starting aggregator (extended sources).")
    rows: List[Row] = []

    # RSS
    rows.extend(fetch_rss_feed("https://www.theskint.com/feed/", "The Skint"))
    rows.extend(fetch_rss_feed("https://www.thrillist.com/rss/locations/new-york", "Thrillist"))

    # HTML tables
    rows.extend(fetch_genre_events())

    # Additional sites requested
    rows.extend(fetch_donyc("https://donyc.com/"))
    rows.extend(fetch_timeout_calendar())  # uses current month or TIMEOUT_URL override
    rows.extend(fetch_secretnyc("https://secretnyc.co/"))
    rows.extend(fetch_nyc_bucket_list("https://www.nycbucketlist.com/"))

    # NY Event Radar (keep the improved heuristics)
    rows.extend(fetch_ny_event_radar())

    logger.info("Collected rows before dedupe: %d", len(rows))
    rows = dedupe_rows(rows)
    rows.sort(key=lambda r: (r.source, r.name.lower()))

    if rows:
        update_readme(rows)
    else:
        logger.info("No events to write.")

if __name__ == "__main__":
    main()
