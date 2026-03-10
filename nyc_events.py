import requests
from bs4 import BeautifulSoup
import feedparser
from datetime import datetime, timedelta
from thefuzz import fuzz
import re
import os

# --- DATE LOGIC ---
def get_milestone_dates():
    today = datetime.now()
    if today.day <= 15:
        target = today.replace(day=15, hour=23, minute=59)
    else:
        next_month = today.replace(day=28) + timedelta(days=4)
        target = next_month - timedelta(days=next_month.day, hour=23, minute=59)
    return today, target

# --- DEDUPLICATION ---
def deduplicate(events, threshold=80):
    unique_list = []
    for new_event in events:
        is_duplicate = False
        for existing in unique_list:
            score = fuzz.token_sort_ratio(new_event['name'], existing['name'])
            # If names are 80%+ similar and dates match, it's a dupe
            if score > threshold and new_event['date'] == existing['date']:
                is_duplicate = True
                break
        if not is_duplicate:
            unique_list.append(new_event)
    return unique_list

# --- SOURCE FETCHERS ---

def fetch_parks_api(start, end):
    url = "https://data.cityofnewyork.us/resource/w3wp-dpdi.json"
    # Simplified params to avoid the "non-tabular" error
    params = {"$limit": 100, "$order": "start_date DESC"}
    try:
        r = requests.get(url, params=params, timeout=10)
        data = r.json()
        events = []
        for e in data:
            date_str = e.get('start_date', '')[:10]
            try:
                e_date = datetime.strptime(date_str, '%Y-%m-%d')
                if start <= e_date <= end:
                    events.append({
                        'name': e.get('event_name', 'Unnamed Park Event'),
                        'date': date_str,
                        'loc': e.get('location', 'NYC Park'),
                        'link': e.get('event_url', 'https://www.nycgovparks.org/events')
                    })
            except: continue
        return events
    except: return []

def fetch_rss_source(url, source_name, target_date):
    events = []
    try:
        feed = feedparser.parse(url)
        for entry in feed.entries:
            # Handle cases where published_parsed might be missing
            if not hasattr(entry, 'published_parsed'): continue
            pub_date = datetime(*entry.published_parsed[:6])
            if pub_date <= target_date:
                events.append({
                    'name': entry.title,
                    'date': pub_date.strftime('%Y-%m-%d'),
                    'loc': source_name,
                    'link': entry.link
                })
    except: pass
    return events

def fetch_genre_events():
    events = []
    url = "https://genreevents.com/downstate-new-york/"
    try:
        r = requests.get(url, timeout=10)
        soup = BeautifulSoup(r.text, 'html.parser')
        for row in soup.select('table tr')[1:]:
            cols = row.find_all('td')
            if len(cols) >= 2:
                events.append({
                    'name': cols[0].text.strip(),
                    'date': cols[1].text.strip(),
                    'loc': 'Downstate NY',
                    'link': url
                })
    except: pass
    return events

def fetch_ny_event_radar():
    events = []
    url = "https://ny-event-radar.com/"
    try:
        r = requests.get(url, timeout=10)
        soup = BeautifulSoup(r.text, 'html.parser')
        for item in soup.select('.entry-title a'):
            events.append({
                'name': item.text.strip(),
                'date': 'Upcoming',
                'loc': 'NY Event Radar',
                'link': item['href']
            })
    except: pass
    return events

# --- README UPDATE LOGIC ---

def update_readme(events, target_date):
    start_marker = ""
    end_marker = ""

    # 1. Build the Table
    new_content = f"\n### NYC Event Digest (Updated: {datetime.now().strftime('%Y-%m-%d')})\n"
    new_content += f"**Targeting through: {target_date.strftime('%B %d')}**\n"
    new_content += f"*Found {len(events)} unique events.*\n\n"
    new_content += "| Event | Date | Location | Link |\n| :--- | :--- | :--- | :--- |\n"
    for e in events:
        new_content += f"| {e['name']} | {e['date']} | {e['loc']} | [Link]({e['link']}) |\n"
    new_content += "\n"

    # 2. Read existing content
    if not os.path.exists("README.md"):
        with open("README.md", "w") as f: f.write(f"{start_marker}\n{end_marker}")
    
    with open("README.md", "r", encoding="utf-8") as f:
        content = f.read()

    # 3. Surgical Replacement (The "Anti-Repeat" fix)
    if start_marker not in content or end_marker not in content:
        # If markers are missing, append them to the end
        content += f"\n\n{start_marker}\n{end_marker}"

    pattern = f"{start_marker}.*?{end_marker}"
    replacement = f"{start_marker}{new_content}{end_marker}"
    
    # re.DOTALL is critical here to match across multiple lines
    updated_content = re.sub(pattern, replacement, content, flags=re.DOTALL)

    with open("README.md", "w", encoding="utf-8") as f:
        f.write(updated_content)
    print(f"README.md updated with {len(events)} events.")

if __name__ == "__main__":
    start_date, end_date = get_milestone_dates()
    all_raw_events = []

    print(f"Scraping for milestone: {end_date.strftime('%Y-%m-%d')}")

    all_raw_events.extend(fetch_parks_api(start_date, end_date))
    all_raw_events.extend(fetch_rss_source("https://www.theskint.com/feed/", "The Skint", end_date))
    all_raw_events.extend(fetch_rss_source("https://www.thrillist.com/rss/locations/new-york", "Thrillist", end_date))
    all_raw_events.extend(fetch_genre_events())
    all_raw_events.extend(fetch_ny_event_radar())

    final_list = deduplicate(all_raw_events)

    if final_list:
        update_readme(final_list, end_date)
    else:
        print("No events found to update.")
