import requests, pandas as pd
from bs4 import BeautifulSoup

SEARCH_URL = "https://eservices.railway.gov.lk/schedule/searchTrain.action"
PARAMS = {"lang": "en"}  # English labels; IDs are the same across languages.

def fetch_stations():
    r = requests.get(SEARCH_URL, params=PARAMS, timeout=40)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    # Both #startStation and #endStation carry the same master list
    options = soup.select("select#startStation option")
    rows = []
    for opt in options:
        val = (opt.get("value") or "").strip()
        name = (opt.get_text() or "").strip()
        if not val or val == "-1" or not name or name == "--- Select ---":
            continue
        rows.append({"station_id": int(val), "station_name": name})
    return pd.DataFrame(rows).drop_duplicates().sort_values("station_name")

if __name__ == "__main__":
    df = fetch_stations()
    df.to_csv("stations.csv", index=False)
    print(f"Saved stations.csv with {len(df)} stations")
