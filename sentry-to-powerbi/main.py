import requests
import json
import os
from datetime import datetime, timezone, timedelta
import time

# --- CONFIG ---
SENTRY_TOKEN = os.environ["SENTRY_TOKEN"]

ISSUE_API_URL = os.environ.get("ISSUE_API_URL", "")
EVENT_API_URL = os.environ.get("EVENT_API_URL", "")

POWER_BI_ISSUE_URL = os.environ.get("POWER_BI_ISSUE_URL", "")
POWER_BI_EVENT_URL = os.environ.get("POWER_BI_EVENT_URL", "")

ISSUE_CHECKPOINT_FILE = os.path.join(os.path.dirname(__file__), "last_issues_time.txt")
EVENT_CHECKPOINT_FILE = os.path.join(os.path.dirname(__file__), "last_events_time.txt")

BATCH_SIZE = 10

def convert_to_ist(dt_str):
    utc = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
    ist = utc.astimezone(timezone(timedelta(hours=5, minutes=30)))
    return ist.strftime("%Y-%m-%d %H:%M:%S")

def unix_to_ist(unix_ts):
    utc = datetime.fromtimestamp(unix_ts, tz=timezone.utc)
    ist = utc.astimezone(timezone(timedelta(hours=5, minutes=30)))
    return ist.strftime("%Y-%m-%d %H:%M:%S")

def load_last_processed_time(filename):
    if os.path.exists(filename):
        with open(filename, "r") as f:
            try:
                line = f.read().strip()
                return int(line.split()[0])
            except:
                pass
    default_start = datetime(2025, 4, 18, 0, 0, tzinfo=timezone.utc)
    return int(default_start.timestamp() * 1000)

def save_last_processed_time(timestamp_ms, filename):
    utc_dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
    ist_dt = utc_dt.astimezone(timezone(timedelta(hours=5, minutes=30)))
    with open(filename, "w") as f:
        f.write(f"{timestamp_ms}  # {utc_dt.strftime('%Y-%m-%d %H:%M:%S')} UTC [{ist_dt.strftime('%Y-%m-%d %H:%M:%S')} IST]\n")

def fetch_sentry_issues(from_ts_ms, to_ts_ms, retries=3):
    events = []
    url = ISSUE_API_URL
    headers = {
        "Authorization": f"Bearer {SENTRY_TOKEN}",
        "Accept": "application/json"
    }
    after = datetime.fromtimestamp(from_ts_ms / 1000, tz=timezone.utc).isoformat()
    before = datetime.fromtimestamp(to_ts_ms / 1000, tz=timezone.utc).isoformat()
    params = {
        "statsPeriod": "24h",
        "query": f"firstSeen:>={after} lastSeen:<={before}"
    }
    attempt = 1
    while url and attempt <= retries:
        try:
            response = requests.get(url, headers=headers, params=params if attempt == 1 else {}, timeout=60)
            if response.status_code == 429:
                print(f"Rate limit hit. Attempt {attempt}. Waiting 30s...")
                time.sleep(30)
                continue
            response.raise_for_status()
            data = response.json()
            events.extend(data)
            link_header = response.headers.get("Link", "")
            if 'rel="next"' in link_header and 'results="true"' in link_header:
                links = link_header.split(",")
                next_url = None
                for link in links:
                    if 'rel="next"' in link and 'results="true"' in link:
                        start = link.find("<") + 1
                        end = link.find(">")
                        next_url = link[start:end]
                        break
                url = next_url
                params = {}
            else:
                url = None
        except Exception as e:
            print(f"Error during fetch attempt {attempt}: {e}")
            time.sleep(10)
            attempt += 1
    return events

def fetch_sentry_events(retries=3):
    events = []
    url = EVENT_API_URL
    headers = {
        "Authorization": f"Bearer {SENTRY_TOKEN}",
        "Accept": "application/json"
    }
    attempt = 1
    while attempt <= retries:
        try:
            response = requests.get(url, headers=headers, timeout=60)
            response.raise_for_status()
            events.extend(response.json())
            break
        except Exception as e:
            print(f"Error fetching events (attempt {attempt}): {e}")
            time.sleep(10)
            attempt += 1
    return events

def get_issue_time(issue):
    ts = issue.get("lastSeen") or issue.get("firstSeen")
    if ts:
        try:
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            return int(dt.timestamp() * 1000)
        except:
            return 0
    return 0

def get_event_time(event):
    ts = event.get("dateCreated") or event.get("timestamp")
    if ts:
        try:
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            return int(dt.timestamp() * 1000)
        except:
            return 0
    return 0

def transform_issues(issues):
    result = []
    for issue in issues:
        stats_list = issue.get("stats", {}).get("24h", [])
        stats_breakdown = [f"{unix_to_ist(ts)}: {count}" for ts, count in stats_list if count > 0]
        result.append({
            "id": issue.get("id"),
            "title": issue.get("title"),
            "culprit": issue.get("culprit"),
            "level": issue.get("level"),
            "platform": issue.get("platform"),
            "count": issue.get("count"),
            "priority": issue.get("priority"),
            "first_seen": convert_to_ist(issue.get("firstSeen")),
            "last_seen": convert_to_ist(issue.get("lastSeen")),
            "permalink": issue.get("permalink"),
            "stats": "\n".join(stats_breakdown),
            "status": issue.get("status")
        })
    return result

def transform_events(events):
    result = []
    for event in events:
        tags = {tag['key']: tag['value'] for tag in event.get("tags", [])}
        user = event.get("user", {})
        message = event.get("message") or ""
        if message and len(message) > 3990:
            message = message[:3990] + "..." 
        result.append({
            "primary_id": event.get("id"),
            "event_type": event.get("event.type"),
            "groupID": event.get("groupID"),
            "message": message,
            "location": event.get("location"),
            "culprit": event.get("culprit"),
            "title": event.get("title"),
            "user_id": user.get("id"),
            "role": (user.get("data") or {}).get("role"),
            "url": tags.get("url"),
            "level": tags.get("level"),
            "environment": tags.get("environment"),
            "created_time": event.get("dateCreated"),
            "platform": event.get("platform"),
            "filename": event.get("metadata", {}).get("filename"),
            "function_name": event.get("metadata", {}).get("function")
        })
    return result

def push_to_powerbi_in_batches(data, url):
    headers = {"Content-Type": "application/json"}
    total = len(data)
    for i in range(0, total, BATCH_SIZE):
        batch = data[i:i + BATCH_SIZE]
        response = requests.post(url, headers=headers, data=json.dumps(batch))
        if response.status_code != 200:
            print(f"Failed batch {i} → {i + len(batch)}: {response.status_code} {response.text}")
            return False
        print(f"Pushed {len(batch)} rows (Batch {i} → {i + len(batch)})")
    return True

# --- MAIN ---
if __name__ == "__main__":
    now_ts = int(datetime.now(timezone.utc).timestamp() * 1000)

    # ---- ISSUES ----
    last_issue_ts = load_last_processed_time(ISSUE_CHECKPOINT_FILE)
    from_dt_utc = datetime.fromtimestamp(last_issue_ts / 1000, tz=timezone.utc)
    to_dt_utc = datetime.fromtimestamp(now_ts / 1000, tz=timezone.utc)
    from_dt_ist = from_dt_utc.astimezone(timezone(timedelta(hours=5, minutes=30)))
    to_dt_ist = to_dt_utc.astimezone(timezone(timedelta(hours=5, minutes=30)))

    print("\nISSUES CHECKPOINT RANGE:")
    print(f"From: {from_dt_utc.strftime('%Y-%m-%d %H:%M:%S')} UTC [{from_dt_ist.strftime('%Y-%m-%d %H:%M:%S')} IST]")
    print(f"To  : {to_dt_utc.strftime('%Y-%m-%d %H:%M:%S')} UTC [{to_dt_ist.strftime('%Y-%m-%d %H:%M:%S')} IST]")

    issues = fetch_sentry_issues(last_issue_ts, now_ts)
    new_issues = [e for e in issues if last_issue_ts < get_issue_time(e) <= now_ts]

    print(f"\nTotal issues fetched: {len(issues)}")
    print(f"New issues to push: {len(new_issues)}")

    if new_issues:
        new_issues.sort(key=get_issue_time)
        transformed_issues = transform_issues(new_issues)
        if push_to_powerbi_in_batches(transformed_issues, POWER_BI_ISSUE_URL):
            save_last_processed_time(max(get_issue_time(e) for e in new_issues), ISSUE_CHECKPOINT_FILE)

    # ---- EVENTS ----
    last_event_ts = load_last_processed_time(EVENT_CHECKPOINT_FILE)
    from_event_dt_utc = datetime.fromtimestamp(last_event_ts / 1000, tz=timezone.utc)
    from_event_dt_ist = from_event_dt_utc.astimezone(timezone(timedelta(hours=5, minutes= 30)))

    print("\nEVENTS CHECKPOINT RANGE:")
    print(f"From: {from_event_dt_utc.strftime('%Y-%m-%d %H:%M:%S')} UTC [{from_event_dt_ist.strftime('%Y-%m-%d %H:%M:%S')} IST]")
    print(f"To  : {to_dt_utc.strftime('%Y-%m-%d %H:%M:%S')} UTC [{to_dt_ist.strftime('%Y-%m-%d %H:%M:%S')} IST]")

    events = fetch_sentry_events()
    print(f"\nTotal raw events fetched from Sentry: {len(events)}")

    new_events = [e for e in events if last_event_ts < get_event_time(e) <= now_ts]
    print(f"New events to push: {len(new_events)}")

    if new_events:
        print("Sample event:\n", json.dumps(new_events[0], indent=2))
        new_events.sort(key=get_event_time)
        transformed_events = transform_events(new_events)
        if push_to_powerbi_in_batches(transformed_events, POWER_BI_EVENT_URL):
            save_last_processed_time(max(get_event_time(e) for e in new_events), EVENT_CHECKPOINT_FILE)
