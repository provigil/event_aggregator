import os
import requests
from bs4 import BeautifulSoup
import feedparser
from datetime import datetime, timedelta
from thefuzz import fuzz
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

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
            # Match similarity and date
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
        # This targets the specific table structure on GenreEvents
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
        # Target the post-title links which usually hold the event names
        for item in soup.select('.entry-title a'):
            events.append({
                'name': item.text.strip(),
                'date': 'Upcoming',
                'loc': 'NY Event Radar',
                'link': item['href']
            })
    except: pass
    return events

# --- MAIN EXECUTION ---

def main():
    start_date, end_date = get_milestone_dates()
    all_raw_events = []

    # 1. API Calls
    all_raw_events.extend(fetch_parks_api(start_date, end_date))

    # 2. RSS Feeds
    all_raw_events.extend(fetch_rss_source("https://www.theskint.com/feed/", "The Skint", end_date))
    all_raw_events.extend(fetch_rss_source("https://www.thrillist.com/rss/locations/new-york", "Thrillist", end_date))

    # 3. Lite Scrapes
    all_raw_events.extend(fetch_genre_events())
    all_raw_events.extend(fetch_ny_event_radar())

    # 4. Deduplicate
    final_list = deduplicate(all_raw_events)

    # 5. Send via SendGrid
    if final_list:
        send_email(final_list, end_date)
    else:
        print("No events found for this period.")

def send_email(events, target_date):
    rows = ""
    for e in events:
        rows += f"""
        <tr>
            <td style="padding:8px; border:1px solid #ddd;">{e['name']}</td>
            <td style="padding:8px; border:1px solid #ddd;">{e['date']}</td>
            <td style="padding:8px; border:1px solid #ddd;">{e['loc']}</td>
            <td style="padding:8px; border:1px solid #ddd;"><a href="{e['link']}">View</a></td>
        </tr>"""

    html_body = f"""
    <html>
    <body style="font-family: Arial, sans-serif;">
        <h2>NYC Event Digest (Through {target_date.strftime('%B %d')})</h2>
        <table style="width:100%; border-collapse: collapse;">
            <tr style="background-color: #f2f2f2;">
                <th style="padding:8px; border:1px solid #ddd; text-align:left;">Event</th>
                <th style="padding:8px; border:1px solid #ddd; text-align:left;">Date</th>
                <th style="padding:8px; border:1px solid #ddd; text-align:left;">Source/Location</th>
                <th style="padding:8px; border:1px solid #ddd; text-align:left;">Link</th>
            </tr>
            {rows}
        </table>
    </body>
    </html>
    """

    message = Mail(
        from_email=os.environ.get('SENDER_EMAIL'),
        to_emails=os.environ.get('RECEIVER_EMAIL'),
        subject=f"NYC Events Digest: {datetime.now().strftime('%m/%d')} Milestone",
        html_content=html_body
    )

    try:
        sg = SendGridAPIClient(os.environ.get('SENDGRID_API_KEY'))
        sg.send(message)
        print("Email sent successfully!")
    except Exception as e:
        print(f"Error sending email: {e}")

if __name__ == "__main__":
    main()
