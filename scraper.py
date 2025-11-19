import os
import requests
import csv
import time
import re
from pathlib import Path

#Config

RAPID_API_KEY = os.getenv("API_KEY")
TEAM_ID = 35 #Manchester United
BASE_URL = "https://sofasport.p.rapidapi.com"

HEADERS = {
    "x-rapidapi-key": RAPID_API_KEY,
    "x-rapidapi-host": "sofasport.p.rapidapi.com"
}

OUT_DIR = Path("output_csvs")
OUT_DIR.mkdir(exist_ok=True)

def sanitize_filename(s: str) -> str:
    s = re.sub(r"[^\w\s-]", "", s)
    s = re.sub(r"\s+", "_", s).strip("_")
    return s[:120]

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
    players_dict = {}

    for event_id in event_ids:
        print(f"📊 Henter kamp {event_id}...")

        try:
            lineup_data = get_lineup(event_id)
            event = get_event_data(event_id)

            home_name = event["data"]["homeTeam"]["name"]
            away_name = event["data"]["awayTeam"]["name"]

            home_team_id = event["data"]["homeTeam"]["id"]
            away_team_id = event["data"]["awayTeam"]["id"]

            lineup = lineup_data.get("data", {})
            home_block = lineup["home"]
            away_block = lineup["away"]
            home_formation = home_block["formation"]
            away_formation = away_block["formation"]

            # Table positions
            season_ids = get_seasonIds(1)
            seasons_id = season_ids["data"]["seasons"][0]["id"]
            tournament_id = 1
            standings = get_standings(seasons_id, tournament_id)

            table_rows = standings["data"][0]["rows"]

            home_pos = next((t["position"] for t in table_rows if t["team"]["name"] == home_name), 0)
            away_pos = next((t["position"] for t in table_rows if t["team"]["name"] == away_name), 0)

            if TEAM_ID == home_team_id:
                target_block = home_block
                target_team_name = home_name
                target_team_id = home_team_id
                target_team_pos = home_pos
                opp_team_name = away_name
                opp_team_id = away_team_id
                opp_team_pos = away_pos
                target_formation = home_formation
                opp_formation = away_formation
                home_or_away = "home"
            elif TEAM_ID == away_team_id:
                target_block = away_block
                target_team_name = away_name
                target_team_id = away_team_id
                target_team_pos = away_pos
                opp_team_name = home_name
                opp_team_id = home_team_id
                opp_team_pos = home_pos
                target_formation = away_formation
                opp_formation = home_formation
                home_or_away = "away"
            

            # Team statistics
            players_block = target_block.get("players", {})
            if isinstance(players_block, dict):
                players = players_block.values()
            else:
                players = players_block
            
            for entry in players:
                player = entry.get("player", {})
                statistics = entry.get("statistics", {})

                player_name = player.get("name", "Unknown Player")
                safe_name = sanitize_filename(player_name)
                

                row = {
                    "event_id": event_id,
                    "team": target_team_name,
                    "team_id": target_team_id,
                    "team_pos": target_team_pos,
                    "home_or_away": home_or_away,
                    "formation": target_formation,
                    "opposition": opp_team_name,
                    "opposition_id": opp_team_id,
                    "opposition_pos": opp_team_pos,
                    "opposition_formation": opp_formation,
                    "player_id": player.get("id"),
                    "player_name": player_name,
                    "position": player.get("position")
                }

                for k, v in statistics.items():
                    if k in row:
                        row[f"stat_{k}"] = v
                    else:
                        row[k] = v

                players_dict.setdefault(safe_name, []).append(row)

        except Exception as e:
            print(f"⚠️ Feil i kamp {event_id}: {e}")

        time.sleep(0.2)

    meta_keys = [
        "event_id", "team", "team_id", "team_pos", "home_or_away", "formation",
        "opposition", "opposition_id", "opposition_pos", "opposition_formation",
        "player_id", "player_name", "position"
    ]

    for safe_name, rows in players_dict.items():

        # Save CSV
        all_keys = set()
        for r in rows:
            all_keys.update(r.keys())
        
        other_keys = sorted([k for k in all_keys if k not in meta_keys])

        header = [k for k in meta_keys if k in all_keys] + other_keys

        filename = OUT_DIR / f"{safe_name}.csv"
        file_exists = filename.exists()

        if file_exists:
            with open(filename, "r", encoding="utf-8") as f_read:
                reader = csv.DictReader(f_read)
                try:
                    existing_header = next(reader)
                except StopIteration:
                    existing_header = []
            
            new_cols = [c for c in header if c not in existing_header]
            final_header = existing_header + new_cols
            write_mode = "a"
        else:
            final_header = header
            write_mode = "w"

        with open(filename, write_mode, newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=final_header)
            if write_mode == "w":
                writer.writeheader() 
            for r in rows:
                out_row = {k: r.get(k, "") for k in final_header}
                writer.writerow(out_row)

        print(f"💾 Lagret {len(rows)} rader for {safe_name} → {filename}")



if __name__ == "__main__":
    #MATCH_IDS = get_team_matches(TEAM_ID, SEASON_ID)
    MATCH_IDS = [12436611]
    scrape_stats(MATCH_IDS)
