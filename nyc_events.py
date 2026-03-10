import requests
from bs4 import BeautifulSoup
import feedparser
from datetime import datetime, timedelta
from thefuzz import fuzz
import re
import os

# --- CONFIG & DATE LOGIC ---
def get_milestone_dates():
    today = datetime.now()
    if today.day <= 15:
        target = today.replace(day=15, hour=23, minute=59)
    else:
        # Calculate last day of month
        next_month = today.replace(day=28) + timedelta(days=4)
        target = next_month - timedelta(days=next_month.day, hour=23, minute=59)
    return today, target

# --- DEDUPLICATION ---
def deduplicate(events, threshold=85):
    unique_list = []
    for new_event in events:
        is_duplicate = False
        for existing in unique_list:
            score = fuzz.token_sort_ratio(new_event['name'], existing['name'])
            if score > threshold and new_event['date'] == existing['date']:
                is_duplicate = True
                break
        if not is_duplicate:
            unique_list.append(new_event)
    return unique_list

# --- SOURCE FETCHERS ---

def fetch_parks_api(start, end):
    url = "https://data.cityofnewyork.us/resource/w3wp-dpdi.json"
    params = {
        "$where": f"start_date >= '{start.strftime('%Y-%m-%dT%H:%M:%S')}' AND start_date <= '{end.strftime('%Y-%m-%dT%H:%M:%S')}'",
        "$limit": 100
    }
    try:
        r = requests.get(url, params=params, timeout=10)
        return [{
            'name': e.get('event_name', 'Unnamed Event'),
            'date': e.get('start_date', '')[:10],
            'loc': e.get('location', 'NYC Park'),
            'link': e.get('event_url', 'https://www.nycgovparks.org/events')
        } for e in r.json()]
    except Exception as e:
        print(f"Parks API Error: {e}")
        return []

def fetch_rss_source(url, source_name, target_date):
    events = []
    try:
        feed = feedparser.parse(url)
        for entry in feed.entries:
            # Convert RSS time to datetime
            pub_date = datetime(*entry.published_parsed[:6])
            if pub_date <= target_date:
                events.append({
                    'name': entry.title,
                    'date': pub_date.strftime('%Y-%m-%d'),
                    'loc': source_name,
                    'link': entry.link
                })
    except Exception as e:
        print(f"RSS Error ({source_name}): {e}")
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
    # 1. Generate the Table string
    new_content = f"\n### NYC Event Digest (Updated: {datetime.now().strftime('%Y-%m-%d')})\n"
    new_content += f"**Events through: {target_date.strftime('%B %d')}**\n\n"
    new_content += "| Event | Date | Location | Link |\n| :--- | :--- | :--- | :--- |\n"
    for e in events:
        new_content += f"| {e['name']} | {e['date']} | {e['loc']} | [Link]({e['link']}) |\n"
    new_content += "\n"

    # 2. Read existing README
    if not os.path.exists("README.md"):
        # Create a blank README with markers if it doesn't exist
        with open("README.md", "w") as f:
            f.write("\n")
    
    with open("README.md", "r") as f:
        content = f.read()
    
    # 3. Use markers to replace only the relevant section
    pattern = r".*?"
    replacement = f"{new_content}"
    
    # Check if markers exist
    if "" not in content:
        print("Error: Could not find marker in README.md")
        return

    updated_content = re.sub(pattern, replacement, content, flags=re.DOTALL)

    # 4. Save
    with open("README.md", "w") as f:
        f.write(updated_content)
    print("README.md has been updated.")

# --- MAIN EXECUTION ---

if __name__ == "__main__":
    start_date, end_date = get_milestone_dates()
    all_raw_events = []

    print(f"Scraping events between {start_date.date()} and {end_date.date()}...")

    # 1. API & RSS
    all_raw_events.extend(fetch_parks_api(start_date, end_date))
    all_raw_events.extend(fetch_rss_source("https://www.theskint.com/feed/", "The Skint", end_date))
    all_raw_events.extend(fetch_rss_source("https://www.thrillist.com/rss/locations/new-york", "Thrillist", end_date))

    # 2. Lite Scrapes
    all_raw_events.extend(fetch_genre_events())
    all_raw_events.extend(fetch_ny_event_radar())

    # 3. Deduplicate
    final_list = deduplicate(all_raw_events)

    # 4. Update the File
    if final_list:
        update_readme(final_list, end_date)
    else:
        print("No events found.")
