import requests
from bs4 import BeautifulSoup
import feedparser
from datetime import datetime, timedelta
from thefuzz import fuzz
import os

# --- DATE LOGIC ---
def get_milestone_dates():
    today = datetime.now()
    # If today is 1st-15th, target the 15th. Otherwise target end of month.
    if today.day <= 15:
        target = today.replace(day=15, hour=23, minute=59)
    else:
        next_month = today.replace(day=28) + timedelta(days=4)
        target = next_month - timedelta(days=next_month.day, hour=23, minute=59)
    return today, target

# --- DEDUPLICATION ---
def deduplicate(events, threshold=85):
    unique_list = []
    for new_event in events:
        is_duplicate = False
        for existing in unique_list:
            # Match similarity of names and identical dates
            score = fuzz.token_sort_ratio(new_event['name'], existing['name'])
            if score > threshold and new_event['date'] == existing['date']:
                is_duplicate = True
                break
        if not is_duplicate:
            unique_list.append(new_event)
    return unique_list

# --- SOURCE FETCHERS ---

def fetch_parks_api(start, end):
    # Simplified call to avoid SQL-style query errors
    url = "https://data.cityofnewyork.us/resource/w3wp-dpdi.json"
    params = {"$limit": 100, "$order": "start_date DESC"}
    try:
        r = requests.get(url, params=params, timeout=10)
        data = r.json()
        if not isinstance(data, list): return []
        
        events = []
        for e in data:
            date_str = e.get('start_date', '')[:10]
            try:
                e_date = datetime.strptime(date_str, '%Y-%m-%d')
                if start <= e_date <= end:
                    events.append({
                        'name': e.get('event_name', 'Unnamed Event'),
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
    try:
        r = requests.get("https://genreevents.com/downstate-new-york/", timeout=10)
        soup = BeautifulSoup(r.text, 'html.parser')
        for row in soup.select('table tr')[1:]:
            cols = row.find_all('td')
            if len(cols) >= 2:
                events.append({
                    'name': cols[0].text.strip(),
                    'date': cols[1].text.strip(),
                    'loc': 'Downstate NY',
                    'link': "https://genreevents.com/downstate-new-york/"
                })
    except: pass
    return events

def fetch_ny_event_radar():
    events = []
    try:
        r = requests.get("https://ny-event-radar.com/", timeout=10)
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
    
    # 1. Generate the New Table
    new_table = f"\n### NYC Event Digest (Updated: {datetime.now().strftime('%Y-%m-%d')})\n"
    new_table += f"**Events through: {target_date.strftime('%B %d')}**\n"
    new_table += f"*Found {len(events)} unique events.*\n\n"
    new_table += "| Event | Date | Location | Link |\n| :--- | :--- | :--- | :--- |\n"
    for e in events:
        new_table += f"| {e['name']} | {e['date']} | {e['loc']} | [Link]({e['link']}) |\n"
    new_table += "\n"

    # 2. Read existing README
    if not os.path.exists("README.md"):
        content = f"{start_marker}\n{end_marker}"
    else:
        with open("README.md", "r", encoding="utf-8") as f:
            content = f.read()

    # 3. Secure Replacement
    if start_marker not in content or end_marker not in content:
        print("Markers missing! Appending to bottom.")
        content += f"\n\n{start_marker}\n{end_marker}"

    try:
        # Split content to surgically replace the middle
        pre_content = content.split(start_marker)[0]
        post_content = content.split(end_marker)[1]
        final_content = f"{pre_content}{start_marker}{new_table}{end_marker}{post_content}"
        
        with open("README.md", "w", encoding="utf-8") as f:
            f.write(final_content)
        print("README.md successfully updated.")
    except Exception as e:
        print(f"Update Error: {e}")

# --- MAIN EXECUTION ---

if __name__ == "__main__":
    start_date, end_date = get_milestone_dates()
    all_raw_events = []

    print(f"Scraping events for {start_date.strftime('%Y-%m-%d')} milestone...")

    all_raw_events.extend(fetch_parks_api(start_date, end_date))
    all_raw_events.extend(fetch_rss_source("https://www.theskint.com/feed/", "The Skint", end_date))
    all_raw_events.extend(fetch_rss_source("https://www.thrillist.com/rss/locations/new-york", "Thrillist", end_date))
    all_raw_events.extend(fetch_genre_events())
    all_raw_events.extend(fetch_ny_event_radar())

    final_list = deduplicate(all_raw_events)
    update_readme(final_list, end_date)
