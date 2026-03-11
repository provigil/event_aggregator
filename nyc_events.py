#!/usr/bin/env python3
"""
nyc_events.py (lightweight, source-robust)

Output table: EVENT | LINK | SOURCE

Key improvements:
- Time Out: selects month page from the Time Out events calendar hub, then extracts many items.
- NYPL: scrapes /events/calendar (paginated) for event titles + links.
- DoNYC: if direct fetch gets 403, optionally fallback to Google News RSS 'site:donyc.com when:30d'
- Thrillist: old RSS is 404; optional fallback to Google News RSS 'site:thrillist.com when:30d'
- SecretNYC: scrape /things-to-do/ listing pages instead of relying on RSS.
- Bucket Listers: scrape NYC explore page.

Dependencies:
  pip install requests beautifulsoup4 feedparser thefuzz python-Levenshtein
"""

from __future__ import annotations

import os
import re
import time
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Iterable, Dict
from urllib.parse import urljoin, urlparse, quote_plus

import requests
from bs4 import BeautifulSoup
import feedparser
from thefuzz import fuzz

# ------------------ Configuration ------------------

README_PATH = os.environ.get("README_PATH", "README.md")
START_MARKER = "<!-- NYC_EVENTS_START -->"
END_MARKER = "<!-- NYC_EVENTS_END -->"

# Keep it lightweight: bound how much we crawl per big source.
NYPL_PAGES = int(os.environ.get("NYPL_PAGES", "2"))          # 1-3 recommended
SECRETNYC_PAGES = int(os.environ.get("SECRETNYC_PAGES", "1"))  # 1-2 recommended

ENABLE_GOOGLE_NEWS_FALLBACKS = os.environ.get("ENABLE_GOOGLE_NEWS_FALLBACKS", "1") == "1"

REQUEST_TIMEOUT_S = int(os.environ.get("REQUEST_TIMEOUT_S", "20"))
PER_DOMAIN_DELAY_S = float(os.environ.get("PER_DOMAIN_DELAY_S", "0.8"))
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "2"))

# Use a browser-like UA for compatibility (does not guarantee bypass of 403 bot blocks).
USER_AGENT = os.environ.get(
    "EVENT_DIGEST_USER_AGENT",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(levelname)s: %(message)s")
logger = logging.getLogger("nyc-events")

session = requests.Session()
session.headers.update({
    "User-Agent": USER_AGENT,
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
})

_last_request_by_domain: Dict[str, float] = {}

@dataclass(frozen=True)
class Row:
    name: str
    link: str
    source: str

# ------------------ HTTP helpers ------------------

def _domain(url: str) -> str:
    try:
        return urlparse(url).netloc.lower()
    except Exception:
        return ""

def _throttle(url: str):
    d = _domain(url)
    if not d:
        time.sleep(PER_DOMAIN_DELAY_S)
        return
    last = _last_request_by_domain.get(d)
    now = time.time()
    if last is not None:
        wait = PER_DOMAIN_DELAY_S - (now - last)
        if wait > 0:
            time.sleep(wait)
    _last_request_by_domain[d] = time.time()

def fetch_url(url: str, params: dict | None = None, headers: dict | None = None) -> Optional[requests.Response]:
    attempt = 0
    while attempt <= MAX_RETRIES:
        attempt += 1
        try:
            _throttle(url)
            resp = session.get(url, params=params, headers=headers, timeout=REQUEST_TIMEOUT_S)
            if 200 <= resp.status_code < 300:
                return resp
            if 400 <= resp.status_code < 500:
                logger.warning("HTTP %s for %s", resp.status_code, url)
                return resp  # caller may want to branch on 403/404
            logger.warning("HTTP %s for %s (attempt %d)", resp.status_code, url, attempt)
        except requests.RequestException as e:
            logger.warning("Request error for %s: %s (attempt %d)", url, e, attempt)
        time.sleep(0.6 * (2 ** (attempt - 1)))
    logger.error("Failed to fetch %s after retries", url)
    return None

def soup_from(url: str, params: dict | None = None) -> Optional[BeautifulSoup]:
    resp = fetch_url(url, params=params)
    if not resp or not resp.text:
        return None
    return BeautifulSoup(resp.text, "html.parser")

def normalize_text(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"\s+", " ", s)
    return s

# ------------------ Extraction helpers ------------------

def extract_links(
    soup: BeautifulSoup,
    base_url: str,
    selectors: Iterable[str],
    source: str,
    max_items: int = 80,
    deny_text: set[str] | None = None,
    deny_href_substrings: Iterable[str] = (),
) -> List[Row]:
    deny_text = deny_text or set()
    rows: List[Row] = []
    seen = set()

    for sel in selectors:
        for a in soup.select(sel):
            if not a or not a.has_attr("href"):
                continue
            name = normalize_text(a.get_text(" ", strip=True))
            if not name or len(name) < 7:
                continue
            if name.lower() in deny_text:
                continue

            href = urljoin(base_url, a["href"])
            if any(bad in href for bad in deny_href_substrings):
                continue

            key = (name.lower(), href)
            if key in seen:
                continue
            seen.add(key)

            rows.append(Row(name=name, link=href, source=source))
            if len(rows) >= max_items:
                return rows
    return rows

def dedupe(rows: List[Row]) -> List[Row]:
    """Deduplicate primarily by link, secondarily by fuzzy title."""
    out: List[Row] = []
    seen_links = set()

    for r in rows:
        if r.link in seen_links:
            continue

        dup = False
        for u in out:
            if r.link == u.link:
                dup = True
                break
            score = fuzz.token_sort_ratio(r.name.lower(), u.name.lower())
            if score >= 95:
                dup = True
                break

        if not dup:
            out.append(r)
            seen_links.add(r.link)

    logger.info("Deduped %d -> %d", len(rows), len(out))
    return out

# ------------------ RSS sources ------------------

def fetch_rss(url: str, source_name: str, max_items: int = 40) -> List[Row]:
    resp = fetch_url(url, headers={"Accept": "application/rss+xml,application/xml;q=0.9,*/*;q=0.8"})
    if not resp or resp.status_code >= 400:
        return []
    parsed = feedparser.parse(resp.content)
    rows: List[Row] = []
    for entry in getattr(parsed, "entries", []):
        title = normalize_text(getattr(entry, "title", ""))
        link = getattr(entry, "link", None)
        if title and link:
            rows.append(Row(name=title, link=link, source=source_name))
        if len(rows) >= max_items:
            break
    logger.info("RSS %s -> %d items", source_name, len(rows))
    return rows

def google_news_rss(query: str, source_name: str, max_items: int = 25) -> List[Row]:
    """
    Google News RSS search endpoint.
    Example format widely documented by the community:
      https://news.google.com/rss/search?q={query}&hl=en-US&gl=US&ceid=US:en
    """
    q = quote_plus(query)
    url = f"https://news.google.com/rss/search?q={q}&hl=en-US&gl=US&ceid=US:en"
    return fetch_rss(url, source_name=source_name, max_items=max_items)

# ------------------ HTML sources ------------------

def fetch_genre_events() -> List[Row]:
    url = "https://genreevents.com/downstate-new-york/"
    soup = soup_from(url)
    if not soup:
        return []
    rows: List[Row] = []
    for tr in soup.select("table tr")[1:]:
        cols = tr.find_all("td")
        if not cols:
            continue
        cell = cols[0]
        a = cell.find("a", href=True)
        name = normalize_text(a.get_text(" ", strip=True) if a else cell.get_text(" ", strip=True))
        link = urljoin(url, a["href"]) if a else url
        if name:
            rows.append(Row(name=name, link=link, source="GenreEvents"))
    logger.info("GenreEvents -> %d items", len(rows))
    return rows

def fetch_timeout_current_month(max_items: int = 60) -> List[Row]:
    """
    Pull current-month link from the Time Out NYC events calendar hub,
    then scrape the month page.
    """
    hub = "https://www.timeout.com/newyork/events-calendar"
    hub_soup = soup_from(hub)
    if not hub_soup:
        return []

    month_name = datetime.utcnow().strftime("%B").lower()  # e.g., "march"
    # find the link whose text contains "March events" etc.
    month_link = None
    for a in hub_soup.select("a[href]"):
        txt = normalize_text(a.get_text(" ", strip=True)).lower()
        if txt == f"{month_name} events":
            month_link = urljoin(hub, a["href"])
            break

    if not month_link:
        # If no match, fallback to the old pattern.
        month_link = f"https://www.timeout.com/newyork/events-calendar/{month_name}-events-calendar"

    month_soup = soup_from(month_link)
    if not month_soup:
        return []

    deny_text = {
        "read more", "facebook", "twitter", "pinterest",
        "terms of use", "privacy policy", "copy link", "subscribe",
    }
    deny_href = ("/privacy", "/terms", "facebook.com", "twitter.com", "pinterest.", "whatsapp")

    # Prefer links inside <article> if present.
    article = month_soup.find("article") or month_soup
    rows = extract_links(
        article, month_link,
        selectors=["h2 a[href]", "h3 a[href]", "a[href]"],
        source="Time Out (NYC events calendar)",
        max_items=max_items,
        deny_text=deny_text,
        deny_href_substrings=deny_href,
    )

    # Filter out obvious navigational junk by requiring “non-trivial” titles
    rows = [r for r in rows if len(r.name) >= 10]

    logger.info("Time Out -> %d items (from %s)", len(rows), month_link)
    return rows

def fetch_secretnyc(pages: int = SECRETNYC_PAGES, max_items_per_page: int = 40) -> List[Row]:
    base = "https://secretnyc.co/things-to-do/"
    out: List[Row] = []

    for p in range(1, max(1, pages) + 1):
        url = base if p == 1 else f"{base}page/{p}/"
        soup = soup_from(url)
        if not soup:
            continue

        deny_href = ("facebook.com", "twitter.com", "tiktok.com", "youtube.com", "instagram.com")
        rows = extract_links(
            soup, url,
            selectors=["h2 a[href]", "h3 a[href]"],
            source="SecretNYC (Things To Do)",
            max_items=max_items_per_page,
            deny_text=set(),
            deny_href_substrings=deny_href
        )
        out.extend(rows)

    logger.info("SecretNYC -> %d items", len(out))
    return out

def fetch_bucketlisters_nyc(max_items: int = 80) -> List[Row]:
    """
    Scrape Bucket Listers’ NYC city discovery page (server-rendered list of experiences).
    """
    url = "https://bucketlisters.com/explore/city/NYC"
    soup = soup_from(url)
    if not soup:
        return []

    deny_href = ("apps.apple.com", "instagram.com", "tiktok.com", "youtube.com", "facebook.com")
    deny_text = {"sign in", "find your city", "show more", "add to your bucket list"}

    rows = extract_links(
        soup, url,
        selectors=["a[href]"],
        source="Bucket Listers (NYC)",
        max_items=max_items,
        deny_text=deny_text,
        deny_href_substrings=deny_href
    )

    # Heuristic: keep only items that look like experiences (remove very short/empty labels)
    rows = [r for r in rows if len(r.name) >= 10]

    logger.info("Bucket Listers NYC -> %d items", len(rows))
    return rows

def fetch_nypl_calendar(pages: int = NYPL_PAGES, max_items_per_page: int = 60) -> List[Row]:
    """
    NYPL events calendar listing pages are paginated by ?page=2 etc.
    We only fetch the first N pages to stay lightweight.
    """
    base = "https://www.nypl.org/events/calendar"
    out: List[Row] = []

    for p in range(1, max(1, pages) + 1):
        url = base if p == 1 else f"{base}?page={p}"
        soup = soup_from(url)
        if not soup:
            continue

        # NYPL event detail links commonly include /events/programs/
        rows = []
        for a in soup.select("a[href]"):
            href = a.get("href", "")
            if "/events/programs/" not in href:
                continue
            name = normalize_text(a.get_text(" ", strip=True))
            if not name or len(name) < 7:
                continue
            rows.append(Row(name=name, link=urljoin(url, href), source="NYPL Events Calendar"))

            if len(rows) >= max_items_per_page:
                break

        out.extend(rows)

    logger.info("NYPL -> %d items (pages=%d)", len(out), max(1, pages))
    return out

def fetch_ny_event_radar() -> List[Row]:
    """
    NY Event Radar currently does not expose usable links in the server response.
    Keep this as a placeholder so we can log it clearly rather than silently failing.
    """
    url = "https://www.ny-event-radar.com/"
    resp = fetch_url(url)
    if not resp or resp.status_code >= 400:
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    candidates = extract_links(soup, url, selectors=["a[href]"], source="NY Event Radar", max_items=25)

    # If there are no anchors, or only trivial anchors, skip.
    candidates = [c for c in candidates if len(c.name) >= 10]
    if not candidates:
        logger.warning("NY Event Radar -> 0 usable links (likely JS-only or placeholder HTML).")
    return candidates

def fetch_donyc_with_fallback() -> List[Row]:
    """
    Try DoNYC directly; if blocked (403), optionally use Google News RSS fallback.
    """
    url = "https://donyc.com/"
    resp = fetch_url(url)
    if resp and resp.status_code == 200:
        soup = BeautifulSoup(resp.text, "html.parser")
        rows = extract_links(
            soup, url,
            selectors=["h2 a[href]", "h3 a[href]", ".entry-title a[href]", "article a[href]"],
            source="DoNYC",
            max_items=40
        )
        logger.info("DoNYC direct -> %d items", len(rows))
        return rows

    if resp and resp.status_code == 403:
        logger.warning("DoNYC direct blocked by 403.")
        if ENABLE_GOOGLE_NEWS_FALLBACKS:
            return google_news_rss("site:donyc.com when:30d", "DoNYC (Google News fallback)", max_items=20)
    return []

# ------------------ README update ------------------

def update_readme(rows: List[Row]) -> None:
    now = datetime.utcnow().strftime("%Y-%m-%d")
    table = []
    table.append(f"### NYC Event Digest (Updated: {now})")
    table.append(f"*Found {len(rows)} items after dedupe.*")
    table.append("")
    table.append("| Event | Link | Source |")
    table.append("| :--- | :--- | :--- |")

    for r in rows:
        safe_name = r.name.replace("|", "\\|")
        safe_source = r.source.replace("|", "\\|")
        table.append(f"| {safe_name} | [Link]({r.link}) | {safe_source} |")

    block = "\n".join(table) + "\n"

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

    logger.info("%s updated with %d rows", README_PATH, len(rows))

# ------------------ Main ------------------

def main():
    logger.info("Starting aggregator (robust sources + NYPL).")

    rows: List[Row] = []

    # Known-good RSS
    rows.extend(fetch_rss("https://www.theskint.com/feed/", "The Skint", max_items=40))

    # Thrillist: old RSS is broken in your logs; use Google News fallback if enabled
    if ENABLE_GOOGLE_NEWS_FALLBACKS:
        rows.extend(google_news_rss("site:thrillist.com (\"New York\" OR NYC) when:30d", "Thrillist (Google News fallback)", max_items=15))

    # Works well as-is
    rows.extend(fetch_genre_events())

    # Improve Time Out extraction significantly
    rows.extend(fetch_timeout_current_month(max_items=60))

    # SecretNYC listing pages (better than RSS in practice)
    rows.extend(fetch_secretnyc(pages=SECRETNYC_PAGES, max_items_per_page=40))

    # Bucket Listers NYC (the “NYC Bucket List” ecosystem)
    rows.extend(fetch_bucketlisters_nyc(max_items=80))

    # NYPL calendar (first N pages only)
    rows.extend(fetch_nypl_calendar(pages=NYPL_PAGES, max_items_per_page=60))

    # DoNYC direct (likely blocked) + fallback
    rows.extend(fetch_donyc_with_fallback())

    # NY Event Radar (currently not usable; logs but does not crash)
    rows.extend(fetch_ny_event_radar())

    logger.info("Collected rows before dedupe: %d", len(rows))
    rows = dedupe(rows)
    rows.sort(key=lambda r: (r.source.lower(), r.name.lower()))

    if rows:
        update_readme(rows)
    else:
        logger.warning("No rows to write.")

if __name__ == "__main__":
    main()
