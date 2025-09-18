# discogs_price_updater.py
# Benötigt: pip install requests python-dotenv

import requests, csv, time, json, os
from urllib.parse import quote_plus
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

DISCOGS_TOKEN = os.getenv("DISCOGS_TOKEN")
USER_AGENT = os.getenv("USER_AGENT", "vinyl-price-app/1.0 +d.vonechte@gmail.com")
CSV_FILE = "records.csv"
OUT_FILE = "prices_db.json"
SLEEP_BETWEEN = 1.0  # Sekunden Pause zwischen API-Calls (freundlich)

HEADERS = {
    "Authorization": f"Discogs token={DISCOGS_TOKEN}" if DISCOGS_TOKEN else None,
    "User-Agent": USER_AGENT
}

# Helper: Suche Release-ID anhand Artist, Title, Year
def find_release_id(artist, title, year):
    query = f"{artist} {title}"
    params = {
        "q": query,
        "type": "release",
        "year": year,
        "per_page": 5,
        "page": 1
    }
    url = "https://api.discogs.com/database/search"
    resp = requests.get(url, headers=HEADERS, params=params)
    if resp.status_code != 200:
        print("Search error", resp.status_code, resp.text)
        return None
    data = resp.json()
    # match best: exact artist/title/year if possible
    for r in data.get("results", []):
        r_title = r.get("title", "").lower()
        if str(year) in (str(r.get("year", "")), ""):
            # simple heuristic
            if title.lower() in r_title or artist.lower() in r_title:
                return r.get("id") or r.get("resource_url").split("/")[-1]
    # fallback first result
    if data.get("results"):
        return data["results"][0].get("id")
    return None

# Helper: Get price suggestions
def get_price_suggestions(release_id):
    url = f"https://api.discogs.com/marketplace/price_suggestions/{release_id}"
    resp = requests.get(url, headers=HEADERS)
    if resp.status_code != 200:
        print("Price suggestions error", resp.status_code, resp.text)
        return None
    return resp.json()

def main():
    if not DISCOGS_TOKEN:
        print("DISCOGS_TOKEN not set in environment. Exiting.")
        return

    prices_db = {}
    if os.path.exists(OUT_FILE):
        with open(OUT_FILE, "r", encoding="utf-8") as f:
            try:
                prices_db = json.load(f)
            except:
                prices_db = {}

    with open(CSV_FILE, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            artist = row.get("artist") or row.get("Interpret") or row.get("interpret")
            title = row.get("title") or row.get("Titel") or row.get("titel")
            year = row.get("year") or row.get("Erscheinungsjahr") or row.get("jahr")
            if not (artist and title):
                print("Skipping invalid row:", row)
                continue
            key = f"{artist} — {title} ({year})"
            print("Processing:", key)
            release_id = find_release_id(artist, title, year)
            if not release_id:
                print("  -> No release id found")
                prices_db[key] = {"error": "no_release_found", "last_checked": datetime.utcnow().isoformat()}
                continue
            time.sleep(SLEEP_BETWEEN)
            ps = get_price_suggestions(release_id)
            if not ps:
                prices_db[key] = {"error": "no_price_data", "release_id": release_id, "last_checked": datetime.utcnow().isoformat()}
                continue
            # Example ps structure check: price_suggestions may contain 'lowest_price' or suggestions by condition
            # Store raw plus parsed subset
            entry = {
                "release_id": release_id,
                "raw": ps,
                "parsed": {},
                "last_checked": datetime.utcnow().isoformat()
            }
            # Try to extract condition-specific suggestions if present
            # The structure can vary — store everything and try to map common keys
            # Typical keys might include: 'overall', 'average', 'median' or conditions list
            if "suggestions" in ps:
                for s in ps["suggestions"]:
                    cond = s.get("condition") or s.get("rating") or s.get("label") or "unknown"
                    price = s.get("price") or s.get("value") or None
                    entry["parsed"][cond] = price
            # fallback: entire ps as 'suggestion_summary'
            prices_db[key] = entry
            # small pause to respect API rate limits
            time.sleep(SLEEP_BETWEEN)

    with open(OUT_FILE, "w", encoding="utf-8") as f:
        json.dump(prices_db, f, ensure_ascii=False, indent=2)
    print("Done. Wrote", OUT_FILE)

if __name__ == "__main__":
    main()
