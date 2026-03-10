#!/usr/bin/env python3
"""
NYC event digest aggregator (lightweight, extensible).

Key fixes vs prior version:
- Milestone date calculation no longer crashes after the 15th (uses date + calendar).
- Window comparisons are date-based (won't accidentally drop "today" items).
- GenreEvents parser reads Beginning Date / Ending Date by header name.
- README update uses stable markers and safe regex replacement.
- Adds generic Schema.org JSON-LD Event extraction for easier onboarding of more sites.

Dependencies:
  pip install requests beautifulsoup4 feedparser thefuzz
Optional (recommended for richer parsing): none required beyond above.
"""

from __future__ import annotations

import calendar
import json
import logging
import os
import re
import time
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup
import feedparser
from thefuzz import fuzz


# --------------------
# Config
# --------------------
USER_AGENT = os.environ.get(
    "EVENT_DIGEST_USER_AGENT",
    "NYCEventDigestBot/1.0 (+contact: you@example.com)"
)

REQUEST_TIMEOUT_S = 15
PAUSE_BETWEEN_REQUESTS_S = 0.8  # polite-by-default

# README markers (must be stable + unique)
START_MARKER = "<!-- NYC_EVENTS_START -->"
END_MARKER = "<!-- NYC_EVENTS_END -->"


# --------------------
# Data model
# --------------------
@dataclass(frozen=True)
class Event:
    name: str
    start: Optional[date]          # event start date (date-only)
    end: Optional[date]            # optional end date (date-only)
    loc: str
    link: str
    source: str
    raw_date: str = ""

    @property
    def display_date(self) -> str:
        if self.start and self.end and self.end != self.start:
            return f"{self.start.isoformat()} – {self.end.isoformat()}"
        if self.start:
            return self.start.isoformat()
        return self.raw_date or "TBD"


# --------------------
# Date window logic
# --------------------
def get_milestone_window(now: Optional[datetime] = None) -> Tuple[date, date]:
    """
    Returns (window_start_date, window_end_date).

    Rule:
      - If today is on/before the 15th, target the 15th of this month.
      - Otherwise target the last day of this month.
    """
    now = now or datetime.now()
    today = now.date()

    if today.day <= 15:
        target = date(today.year, today.month, 15)
    else:
        last_day = calendar.monthrange(today.year, today.month)[1]
        target = date(today.year, today.month, last_day)

    return today, target


def intersects_window(ev: Event, start: date, end: date) -> bool:
    """
    Keep events that overlap the window.
    - If event has only a single start date: keep if start <= ev.start <= end
    - If event has a range: keep if ranges overlap
    - If event is undated: drop (keeps digest date-bounded)
    """
    if ev.start is None:
        return False

    ev_end = ev.end or ev.start
    return (ev.start <= end) and (ev_end >= start)


# --------------------
# Helpers
# --------------------
def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT})
    return s


def safe_get_text(session: requests.Session, url: str, params: Optional[dict] = None) -> Optional[str]:
    try:
        resp = session.get(url, params=params, timeout=REQUEST_TIMEOUT_S)
        resp.raise_for_status()
        return resp.text
    except Exception as e:
        logging.warning("GET failed: %s (%s)", url, e)
        return None


def parse_mmddyyyy(value: str) -> Optional[date]:
    value = (value or "").strip()
    if not value:
        return None
    # Expect mm/dd/yyyy as seen on GenreEvents
    try:
        return datetime.strptime(value, "%m/%d/%Y").date()
    except Exception:
        return None


def sanitize_md_cell(text: str) -> str:
    # Escape Markdown table pipes and replace newlines
    t = (text or "").replace("\n", " ").strip()
    return t.replace("|", r"\|")


def normalize_title(title: str) -> str:
    t = (title or "").lower()
    t = re.sub(r"\s+", " ", t)
    t = re.sub(r"[^\w\s]", "", t)
    return t.strip()


def deduplicate(events: List[Event], threshold: int = 88) -> List[Event]:
    """
    Deduplicate using:
      - exact URL match, OR
      - fuzzy title match + same start date
    """
    uniques: List[Event] = []
    for ev in events:
        is_dup = False
        for u in uniques:
            if ev.link and u.link and ev.link == u.link:
                is_dup = True
                break
            if ev.start and u.start and ev.start == u.start:
                score = fuzz.token_sort_ratio(normalize_title(ev.name), normalize_title(u.name))
                if score >= threshold:
                    is_dup = True
                    break
        if not is_dup:
            uniques.append(ev)
    return uniques


# --------------------
# Generic JSON-LD Event extraction (helps add more sites)
# --------------------
def extract_jsonld_events(soup: BeautifulSoup) -> List[Dict[str, Any]]:
    """
    Returns raw JSON-LD objects from <script type="application/ld+json">.
    """
    out: List[Dict[str, Any]] = []
    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        raw = script.string
        if not raw:
            continue
        try:
            obj = json.loads(raw)
        except Exception:
            continue

        if isinstance(obj, list):
            out.extend([x for x in obj if isinstance(x, dict)])
        elif isinstance(obj, dict):
            out.append(obj)
    return out


def jsonld_to_events(objs: List[Dict[str, Any]], source: str) -> List[Event]:
    """
    Converts Schema.org-like JSON-LD objects to Event rows when possible.
    """
    events: List[Event] = []

    def is_event_type(t: Any) -> bool:
        if isinstance(t, str):
            return t.lower().endswith("event")
        if isinstance(t, list):
            return any(isinstance(x, str) and x.lower().endswith("event") for x in t)
        return False

    for obj in objs:
        if not is_event_type(obj.get("@type")):
            continue

        name = (obj.get("name") or obj.get("headline") or "").strip()
        url = (obj.get("url") or "").strip()

        start_raw = obj.get("startDate")
        end_raw = obj.get("endDate")

        # Prefer date-only parsing where possible
        start_d = None
        end_d = None
        if isinstance(start_raw, str) and len(start_raw) >= 10:
            start_d = parse_mmddyyyy(start_raw) or _try_iso_date(start_raw)
        if isinstance(end_raw, str) and len(end_raw) >= 10:
            end_d = parse_mmddyyyy(end_raw) or _try_iso_date(end_raw)

        loc = ""
        location_obj = obj.get("location")
        if isinstance(location_obj, dict):
            loc = (location_obj.get("name") or "").strip()
        elif isinstance(location_obj, list) and location_obj and isinstance(location_obj[0], dict):
            loc = (location_obj[0].get("name") or "").strip()

        if name and start_d:
            events.append(Event(
                name=name,
                start=start_d,
                end=end_d,
                loc=loc or source,
                link=url or "",
                source=source,
                raw_date=start_raw if isinstance(start_raw, str) else ""
            ))

    return events


def _try_iso_date(value: str) -> Optional[date]:
    """
    Try parsing ISO-8601 date or datetime strings via stdlib.
    """
    v = (value or "").strip()
    if not v:
        return None
    # date only?
    if re.match(r"^\d{4}-\d{2}-\d{2}$", v):
        try:
            return date.fromisoformat(v)
        except Exception:
            return None
    # datetime (take first 10)
    if len(v) >= 10 and re.match(r"^\d{4}-\d{2}-\d{2}", v):
        try:
            return date.fromisoformat(v[:10])
        except Exception:
            return None
    return None


# --------------------
# Source fetchers
# --------------------
def fetch_parks_upcoming_14_days(session: requests.Session, start: date, end: date) -> List[Event]:
    """
    NYC Parks Public Events – Upcoming 14 Days.
    Note: The dataset itself is limited to ~2 weeks. It may not contain events beyond that horizon.
    """
    endpoint = "https://data.cityofnewyork.us/resource/w3wp-dpdi.json"

    # Use SoQL when possible to avoid client-side filtering.
    # Some Socrata instances store start_date as a timestamp string; if so, BETWEEN works.
    start_ts = f"{start.isoformat()}T00:00:00"
    end_ts = f"{end.isoformat()}T23:59:59"
    where = f"start_date between '{start_ts}' and '{end_ts}'"

    params = {
        "$limit": 2000,
        "$order": "start_date ASC",
        "$where": where,
    }

    events: List[Event] = []
    try:
        resp = session.get(endpoint, params=params, timeout=REQUEST_TIMEOUT_S)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logging.warning("NYC Parks API failed (%s): %s", endpoint, e)
        return events

    for row in data if isinstance(data, list) else []:
        # Field names are based on common usage in this dataset; keep defensive.
        name = (row.get("event_name") or row.get("name") or "NYC Parks Event").strip()
        start_raw = (row.get("start_date") or "")  # often ISO datetime
        sdate = _try_iso_date(start_raw)

        loc = (row.get("location") or row.get("park_name") or "NYC Parks").strip()
        link = (row.get("event_url") or row.get("event_link") or "https://www.nycgovparks.org/events").strip()

        if not sdate:
            continue

        events.append(Event(
            name=name,
            start=sdate,
            end=None,
            loc=loc,
            link=link,
            source="NYC Parks (Upcoming 14 Days)",
            raw_date=start_raw[:10] if start_raw else sdate.isoformat(),
        ))

    time.sleep(PAUSE_BETWEEN_REQUESTS_S)
    return events


def fetch_rss(url: str, source_name: str, window_start: date, window_end: date) -> List[Event]:
    """
    Treat each RSS entry as a row dated by its publication date.
    (This does NOT parse multiple events inside a single post.)
    """
    events: List[Event] = []

    try:
        # feedparser supports agent= to set User-Agent
        feed = feedparser.parse(url, agent=USER_AGENT)  # see feedparser docs
    except Exception as e:
        logging.warning("RSS parse failed: %s (%s)", url, e)
        return events

    for entry in getattr(feed, "entries", []):
        # Prefer published_parsed; otherwise updated_parsed.
        dt_struct = getattr(entry, "published_parsed", None) or getattr(entry, "updated_parsed", None)
        if not dt_struct:
            continue

        pub_dt = datetime(*dt_struct[:6]).date()
        if not (window_start <= pub_dt <= window_end):
            continue

        title = (getattr(entry, "title", "") or "").strip()
        link = (getattr(entry, "link", "") or "").strip()
        if not title:
            continue

        events.append(Event(
            name=title,
            start=pub_dt,
            end=None,
            loc=source_name,
            link=link,
            source=f"RSS: {source_name}",
            raw_date=pub_dt.isoformat(),
        ))

    return events


def fetch_genre_events_downstate(session: requests.Session, start: date, end: date) -> List[Event]:
    """
    Parse GenreEvents Downstate New York table and capture Beginning Date / Ending Date.
    """
    url = "https://genreevents.com/downstate-new-york/"
    html = safe_get_text(session, url)
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table")
    if not table:
        logging.warning("GenreEvents: no table found at %s", url)
        return []

    rows = table.find_all("tr")
    if not rows:
        return []

    # Build header -> index mapping
    header_cells = rows[0].find_all(["th", "td"])
    headers = [c.get_text(" ", strip=True).lower() for c in header_cells]
    idx = {h: i for i, h in enumerate(headers)}

    def find_col(*candidates: str) -> Optional[int]:
        for cand in candidates:
            cand_l = cand.lower()
            for h, i in idx.items():
                if cand_l == h:
                    return i
        # fallback: "contains" matching
        for cand in candidates:
            cand_l = cand.lower()
            for h, i in idx.items():
                if cand_l in h:
                    return i
        return None

    i_name = find_col("name of event", "event")
    i_city = find_col("city, village, or hamlet", "city")
    i_county = find_col("county or borough", "county", "borough")
    i_begin = find_col("beginning date", "begin date", "start date")
    i_end = find_col("ending date", "end date")

    if i_name is None or i_begin is None:
        logging.warning("GenreEvents: could not locate Name/Beginning Date columns reliably")
        return []

    events: List[Event] = []
    for r in rows[1:]:
        cells = r.find_all("td")
        if not cells or len(cells) <= i_begin:
            continue

        # Name + link often in the first column as <a>
        name_cell = cells[i_name] if i_name < len(cells) else None
        if not name_cell:
            continue
        a = name_cell.find("a", href=True)
        name = (a.get_text(" ", strip=True) if a else name_cell.get_text(" ", strip=True)).strip()
        link = (a["href"].strip() if a else url)

        begin_raw = cells[i_begin].get_text(" ", strip=True) if i_begin < len(cells) else ""
        end_raw = cells[i_end].get_text(" ", strip=True) if (i_end is not None and i_end < len(cells)) else ""

        begin_d = parse_mmddyyyy(begin_raw)
        end_d = parse_mmddyyyy(end_raw) or begin_d

        # Location construction
        city = cells[i_city].get_text(" ", strip=True) if (i_city is not None and i_city < len(cells)) else ""
        county = cells[i_county].get_text(" ", strip=True) if (i_county is not None and i_county < len(cells)) else ""
        loc = ", ".join([x for x in [city, county] if x]) or "Downstate NY"

        if not begin_d:
            continue

        ev = Event(
            name=name,
            start=begin_d,
            end=end_d,
            loc=loc,
            link=link,
            source="GenreEvents: Downstate NY",
            raw_date=f"{begin_raw} – {end_raw}".strip(" –"),
        )

        if intersects_window(ev, start, end):
            events.append(ev)

    time.sleep(PAUSE_BETWEEN_REQUESTS_S)
    return events


def fetch_ny_event_radar(session: requests.Session, start: date, end: date) -> List[Event]:
    """
    NY Event Radar: try JSON-LD Event extraction first; if unavailable, try lightweight heuristics.
    Current site may not expose structured dated listings; in that case this will return [].
    """
    url = "https://ny-event-radar.com/"
    html = safe_get_text(session, url)
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")

    # 1) JSON-LD event extraction (best cross-site strategy)
    jsonld_objs = extract_jsonld_events(soup)
    events = jsonld_to_events(jsonld_objs, source="NY Event Radar")
    events = [e for e in events if intersects_window(e, start, end)]

    # 2) Heuristic fallback: look for linked headings with a nearby date string
    if not events:
        # Example patterns; adapt as site evolves.
        for a in soup.select("a[href]"):
            title = a.get_text(" ", strip=True)
            if not title or len(title) < 6:
                continue
            # Try to find a date-like string in parent text
            blob = (a.parent.get_text(" ", strip=True) if a.parent else "")
            iso_match = re.search(r"\b(\d{4}-\d{2}-\d{2})\b", blob)
            if iso_match:
                d = _try_iso_date(iso_match.group(1))
                if d and (start <= d <= end):
                    events.append(Event(
                        name=title,
                        start=d,
                        end=None,
                        loc="NY Event Radar",
                        link=a["href"],
                        source="NY Event Radar (heuristic)",
                        raw_date=iso_match.group(1),
                    ))

    time.sleep(PAUSE_BETWEEN_REQUESTS_S)
    return events


# --------------------
# README update
# --------------------
def update_readme(readme_path: str, events: List[Event], window_end: date) -> None:
    """
    Replace content between START_MARKER and END_MARKER with a generated table.
    """
    now_str = datetime.now().strftime("%Y-%m-%d")
    content_block = []
    content_block.append(f"### NYC Event Digest (Updated: {now_str})")
    content_block.append(f"**Targeting through: {window_end.strftime('%B %d, %Y')}**")
    content_block.append(f"*Found {len(events)} unique events.*")
    content_block.append("")
    content_block.append("| Event | Date | Location | Link | Source |")
    content_block.append("| :--- | :--- | :--- | :--- | :--- |")

    for e in events:
        content_block.append(
            f"| {sanitize_md_cell(e.name)} | {sanitize_md_cell(e.display_date)} | {sanitize_md_cell(e.loc)} | "
            f"[Link]({e.link}) | {sanitize_md_cell(e.source)} |"
        )

    new_block = "\n" + "\n".join(content_block) + "\n"

    if not os.path.exists(readme_path):
        with open(readme_path, "w", encoding="utf-8") as f:
            f.write(f"{START_MARKER}\n{END_MARKER}\n")

    with open(readme_path, "r", encoding="utf-8") as f:
        existing = f.read()

    if START_MARKER not in existing or END_MARKER not in existing:
        existing = existing.rstrip() + f"\n\n{START_MARKER}\n{END_MARKER}\n"

    pattern = re.escape(START_MARKER) + r".*?" + re.escape(END_MARKER)
    replacement = START_MARKER + new_block + END_MARKER
    updated = re.sub(pattern, replacement, existing, flags=re.DOTALL)

    with open(readme_path, "w", encoding="utf-8") as f:
        f.write(updated)

    print(f"{readme_path} updated with {len(events)} events.")


# --------------------
# Main
# --------------------
def main() -> None:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO").upper())

    start, end = get_milestone_window()
    logging.info("Scraping for milestone window: %s -> %s", start.isoformat(), end.isoformat())

    session = make_session()

    all_events: List[Event] = []
    all_events.extend(fetch_parks_upcoming_14_days(session, start, end))

    # RSS sources (publication-date rows)
    all_events.extend(fetch_rss("https://www.theskint.com/feed/", "The Skint", start, end))
    all_events.extend(fetch_rss("https://www.thrillist.com/rss/locations/new-york", "Thrillist", start, end))

    # HTML table sources
    all_events.extend(fetch_genre_events_downstate(session, start, end))

    # NY Event Radar (best-effort; may currently return [])
    all_events.extend(fetch_ny_event_radar(session, start, end))

    # Final window filter (safety) + dedupe + sort
    all_events = [e for e in all_events if intersects_window(e, start, end)]
    all_events = deduplicate(all_events)
    all_events.sort(key=lambda e: (e.start or date.max, normalize_title(e.name)))

    if all_events:
        update_readme("README.md", all_events, end)
    else:
        print("No dated events found within the milestone window.")


if __name__ == "__main__":
    main()

