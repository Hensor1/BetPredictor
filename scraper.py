import os
import requests
import csv
import time

#Config

RAPID_API_KEY = os.getenv("API_KEY")
TEAM_ID = 35 #Manchester United
BASE_URL = "https://sofasport.p.rapidapi.com"

HEADERS = {
    "x-rapidapi-key": RAPID_API_KEY,
    "x-rapidapi-host": "sofasport.p.rapidapi.com"
}

CSV_FILENAME = "stats.csv"

#Functions

def get_team_matches(team_id):
    url = f"{BASE_URL}/v1/teams/events"
    params = {"page":"0","course_events":"last","team_id": team_id}
    

    r = requests.get(url, headers=HEADERS, params=params)
    r.raise_for_status()
    events = r.json().get("data", {}).get("events", [])
    match_ids = [e["id"] for e in events]
    return match_ids

def get_event_data(event_id):
    url = f"{BASE_URL}/v1/events/data"
    params = {"event_id": event_id}

    r = requests.get(url, headers=HEADERS, params=params)
    r.raise_for_status()
    return r.json()

def get_lineup(event_id):
    url = f"{BASE_URL}/v1/events/lineups"
    params = {"event_id": event_id}

    r = requests.get(url, headers=HEADERS, params=params)
    r.raise_for_status()
    return r.json()

def get_standings(seasons_id, tournament_id):
    url = f"{BASE_URL}/v1/seasons/standings"
    params = {"seasons_id": seasons_id, "tournament_id": tournament_id, "standing_type":"total"}

    r = requests.get(url, headers=HEADERS, params=params)
    r.raise_for_status()
    return r.json()

def get_seasonIds(tournament_id):
    url = f"{BASE_URL}/v1/tournaments/seasons"
    params = {"tournament_id": tournament_id}

    r = requests.get(url, headers=HEADERS, params=params)
    r.raise_for_status()
    return r.json()

def scrape_stats(event_ids):
    rows = []

    for event_id in event_ids:
        print(f"📊 Henter kamp {event_id}...")

        try:
            lineup_data = get_lineup(event_id)
            event = get_event_data(event_id)
            home_name = event["data"]["homeTeam"]["name"]
            away_name = event["data"]["awayTeam"]["name"]

            # Table positions
            season_ids = get_seasonIds(1)
            seasons_id = season_ids["data"]["seasons"][0]["id"]
            tournament_id = 1
            standings = get_standings(seasons_id, tournament_id)

            home_pos = 0
            away_pos = 0
            table = standings.get("data", [])
            if not table:
                raise ValueError("Standings mangler 'data'")

            rows = table[0].get("rows", [])

            for team in rows:
                if team["team"]["name"] == home_name:
                    home_pos = team["position"]
                if team["team"]["name"] == away_name:
                    away_pos = team["position"]

            lineup = lineup_data.get("data", {})
            home_formation = lineup.get("home", {}).get("formation")
            away_formation = lineup.get("away", {}).get("formation")
            home_team_id = event.get("data", {}).get("homeTeam", {}).get("id")

            # Home team statistics
            home_players = lineup.get("home", {}).get("players", {})
            for entry in home_players:
                player = entry.get("player", {})
                statistics = entry.get("statistics", {})

                row = {
                    "event_id": event_id,
                    "team": home_name,
                    "team_id": home_team_id,
                    "team_pos": home_pos,
                    "home_or_away": "home game",
                    "formation": home_formation,
                    "opposition": away_name,
                    "opposition_pos": away_pos,
                    "opposition_formation": away_formation,
                    "player_id": player.get("id"),
                    "player_name": player.get("name"),
                    "position": player.get("position")
                }
                row.update(statistics)
                rows.append(row)


        except Exception as e:
            print(f"⚠️ Feil i kamp {event_id}: {e}")

        time.sleep(0.3)

    # Save CSV
    if rows:
        all_keys = set()
        for r in rows:
            all_keys.update(r.keys())
        all_keys = list(all_keys)
        
        with open(CSV_FILENAME, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=all_keys)
            writer.writeheader()
            writer.writerows(rows)
        print(f"💾 Lagret {len(rows)} rader til {CSV_FILENAME}")
    else:
        print("⚠️ Ingen data hentet.")


if __name__ == "__main__":
    #MATCH_IDS = get_team_matches(TEAM_ID, SEASON_ID)
    MATCH_IDS = [12436611]
    scrape_stats(MATCH_IDS)
