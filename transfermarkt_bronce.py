# ingesta.py - se ejecuta DENTRO del contenedor con spark-submit
import json
import profile
from urllib.parse import quote

import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from pyspark.sql import SparkSession
import pyspark
import time

base_url = "https://righteous-antonina-overenthusiastically.ngrok-free.dev"
session = requests.Session()

NAMESPACE = "transfermarkt"
leagues = {}
teams = {}
players = {}

def get_data():
    # Aquí vive todo el código específico de Transfermarkt
    # getBig5, getTeams, getPlayers, etc.
    
    leagues_dict, leagues_list = getBig5()        # lista de dicts
    teams_dict, teams_list = getTeams(leagues_dict)     # lista de dicts
    players = getPlayers(teams_dict) # lista de dicts

    # Devuelves un dict con nombre de tabla → lista de dicts
    return {
        "leagues": leagues_list,
        "teams": teams_list,
        "players": players
    }

def getBig5():
    print("Getting Big 5 Leagues...")

    all_leagues = []
    leagues = {"Premier League": "England", "LaLiga": "Spain", "Serie A": "Italy", "Bundesliga": "Germany", "Ligue 1": "France"}
    big5 = {}

    # Función para obtener una liga
    def fetch_league(name, country):
        safe_name = quote(name)
        url = f"{base_url}/competitions/search/{safe_name}"
        print(url)
        res = session.get(url)
        if res.status_code != 200:
            print(f"Error {res.status_code} fetching {name}")
            return []
        data = res.json()
        results = []
        for league in data.get("results", []):
            if (league["name"], league["country"]) == (name, country):
                results.append(league)
        return results

    # Paralelizar requests de ligas
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = {executor.submit(fetch_league, name, country): name for name, country in leagues.items()}
        for future in as_completed(futures):
            for league in future.result():
                big5[league["name"]] = league["id"]
                all_leagues.append(league)

    return big5, all_leagues

def getTeams(big5_leagues):
    print("Getting Teams...")
    teams = {}

    def fetch_teams(league_name, league_id):
        url = f"{base_url}/competitions/{league_id}/clubs"
        res = session.get(url)
        if res.status_code != 200:
            print(f"Error fetching clubs for {league_name}: {res.status_code}")
            return league_name, []
        data = res.json()
        league_teams = [(team["id"], team["name"]) for team in data.get("clubs", []) if team.get("id") and team.get("name")]
        return league_name, league_teams

    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = {executor.submit(fetch_teams, name, id): name for name, id in big5_leagues.items()}
        for future in as_completed(futures):
            league_name, league_teams = future.result()
            teams[league_name] = league_teams

    print("Teams retrieved successfully.")
    teams_profiles = writeTeamsToIceberg(teams)
    return teams, teams_profiles

def writeTeamsToIceberg(teamsList):
    def fetch_team_profile(team_id):
        url = f"{base_url}/clubs/{team_id}/profile"
        try:
            res = session.get(url)
            if res.status_code == 200:
                return res.json()
        except Exception as e:
            print(f"Exception fetching team {team_id}: {e}")
        return None

    print("Writing Teams to Iceberg...")
    all_teams = []
    all_team_ids = [
        team_id
        for league_teams in teamsList.values()
        for team_id, _ in league_teams
    ]

    # 👇 20 threads paralelos
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = [executor.submit(fetch_team_profile, team_id) for team_id in all_team_ids]

        for future in as_completed(futures):
            team = future.result()
            if team:
                all_teams.append(team)
    return all_teams

def getPlayers(teams):
    print("Getting Players...")
    players = {}

    def fetch_players(team_name, team_id):
        url = f"{base_url}/clubs/{team_id}/players"
        try:
            res = session.get(url)
            if res.status_code != 200:
                print(f"Error fetching players for {team_name}: {res.status_code}")
                return team_id, team_name, []
            data = res.json()
            team_players = [{"id": p["id"], "name": p["name"], "dateOfBirth": p.get("dateOfBirth")} for p in data.get("players", []) if p.get("id") and p.get("name")]
            return team_id, team_name, team_players
        except Exception as e:
            print(f"Exception fetching players for {team_name}: {e}")
            return team_id, team_name, []

    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = [
            executor.submit(fetch_players, team_name, team_id)
            for league in teams.values()
            for team_id, team_name in league
        ]
        for future in as_completed(futures):
            try:
                team_id, team_name, team_players = future.result()  # ✅ nunca crashea
                players[team_id] = (team_name, team_players)
            except Exception as e:
                print(f"Unhandled exception: {e}")

    print("Players retrieved successfully.")
    return (writePlayersToIceberg(players))

def writePlayersToIceberg(playersList):

    def fetch_player_profile(player_dict):
        player_id = player_dict["id"]
        url = f"{base_url}/players/{player_id}/profile"
        for attempt in range(3):  # 3 intentos
            try:
                time.sleep(0.2 * (attempt))  # espera más en cada reintento
                res = session.get(url)
                if res.status_code == 200:
                    profile = res.json()
                    profile["dateOfBirth"] = player_dict.get("dateOfBirth")
                    return profile
                elif res.status_code == 403:
                    print(f"403 on player {player_id}, attempt {attempt + 1}/3, waiting...")
                    time.sleep(2)  # espera 2ss
                else:
                    print(f"Error {res.status_code} on player {player_id}")
                    break
            except Exception as e:
                print(f"Exception fetching player {player_id}: {e}")
        return None
    
    print("Writing Players to Iceberg...")

    all_player_dicts = [
        player_dict
        for team_name, team_players in playersList.values()
        for player_dict in team_players  # ✅ ahora son dicts, no tuplas
    ]

    all_players = []

    print("Total player IDs:", len(all_player_dicts))

    # 👇 20 threads paralelos
    with ThreadPoolExecutor(max_workers=15) as executor:
        futures = [executor.submit(fetch_player_profile, player_dict) for player_dict in all_player_dicts]

        for future in as_completed(futures):
            try:
                player = future.result()
                if player:
                    print(player.get("name", "Unknown Player"))
                    all_players.append(player)
            except Exception as e:
                print(f"Exception in player: {e}")
    return all_players
