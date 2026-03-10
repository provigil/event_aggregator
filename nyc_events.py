#!/usr/bin/env python3
"""
nyc_events.py (updated)

- No NYC Parks call
- No robots.txt checks
- Output: EVENT | LINK | SOURCE in README between markers
- Tries to be robust with NY Event Radar by using multiple heuristics and better headers
"""

from __future__ import annotations
import os
import re
import time
import logging
import json
from dataclasses import dataclass
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
            # some sites embed multiple objects or trailing commas; try a best-effort substring
            m = re.search(r"\{.*\}", raw, flags=re.DOTALL)
            if m:
                try:
                    obj = json.loads(m.group(0))
                    out.append(obj)
                except Exception:
                    continue
    return out

def polite_get_text(url: str, params: dict = None) -> Optional[str]:
    """Simple polite fetch: throttle + retries (no robots.txt checks)."""
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

# ---------- Source fetchers (no Parks) ----------

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
    # attempt to find first column as title
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

def fetch_ny_event_radar(base_url: str = "https://ny-event-radar.com/") -> List[Row]:
    """
    Attempt several strategies:
      - fetch homepage and /page/1
      - look for JSON-LD Event objects
      - look for <link rel='alternate' type='application/rss+xml'>
      - fallback: find <h1/h2/h3> or anchor text with minimum length, dedupe
    """
    out = []
    tried_urls = [base_url, urljoin(base_url, "page/1/")]
    # try both base and page/1
    for u in tried_urls:
        logger.debug("Trying NY Event Radar URL: %s", u)
        html = polite_get_text(u)
        if not html:
            continue
        soup = BeautifulSoup(html, "html.parser")

        # 1) JSON-LD events
        jsonlds = extract_jsonld(soup)
        for j in jsonlds:
            typ = j.get("@type") or j.get("type") or ""
            if isinstance(typ, list):
                typ = typ[0] if typ else ""
            if "Event" in str(typ) or "event" in str(typ).lower():
                name = j.get("name") or j.get("headline") or ""
                link = j.get("url") or u
                if name:
                    out.append(Row(name=normalize_title(name), link=link, source="NY Event Radar (jsonld)"))

        # 2) check for RSS/atom link
        rss_link = None
        for link_tag in soup.select("link[rel='alternate']"):
            t = link_tag.get("type", "")
            if "rss" in t or "atom" in t:
                rss_link = link_tag.get("href")
                break
        if rss_link:
            rss_link = urljoin(u, rss_link)
            logger.info("NY Event Radar RSS found: %s", rss_link)
            out.extend(fetch_rss_feed(rss_link, "NY Event Radar"))
            # if we found RSS, don't fallback heuristics for this page
            continue

        # 3) heuristic: find heading links or article titles
        # prefer selectors commonly used by WP themes
        candidates = []
        for sel in ("h1 a", "h2 a", "h3 a", ".entry-title a", ".post-title a", "article a", "a[href]"):
            for a in soup.select(sel):
                txt = a.get_text(" ", strip=True)
                href = a.get("href")
                if txt and href and len(txt) > 6:
                    candidates.append((normalize_title(txt), urljoin(u, href)))
        # dedupe candidate anchors by title
        seen = set()
        for title, href in candidates:
            if title.lower() in seen:
                continue
            seen.add(title.lower())
            out.append(Row(name=title, link=href, source="NY Event Radar (heuristic)"))

        # if any rows found on this page, we can stop trying other pages
        if out:
            break

    logger.info("NY Event Radar -> %d items", len(out))
    if not out:
        logger.warning("NY Event Radar: no items found — likely JS-rendered or uses different endpoint")
    return out

# ---------- Dedupe ----------
def dedupe_rows(rows: List[Row]) -> List[Row]:
    uniques: List[Row] = []
    seen_links = set()
    for r in rows:
        if r.link and r.link in seen_links:
            continue
        # fuzzy dedupe by title with very high threshold
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
    now = time.strftime("%Y-%m-%d")
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
    logger.info("Starting aggregator (no parks, no robots).")
    rows: List[Row] = []

    # RSS
    rows.extend(fetch_rss_feed("https://www.theskint.com/feed/", "The Skint"))
    rows.extend(fetch_rss_feed("https://www.thrillist.com/rss/locations/new-york", "Thrillist"))

    # HTML tables
    rows.extend(fetch_genre_events())

    # NY Event Radar (try heuristics)
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
