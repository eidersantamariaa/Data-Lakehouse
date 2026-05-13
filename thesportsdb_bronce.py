# SportsDB/bronce.py
import json
from logging import config
import time

import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

import pyspark
from pyspark.sql import SparkSession

api_key = 123
base_url = f"https://www.thesportsdb.com/api/v1/json/{api_key}"
session = requests.Session()

API = "thesportsdb"  

def get_data(test_mode=False):
    leagues_dict, leagues_list = getBig5(test_mode=test_mode)
    teams_dict, teams_list = getTeams(leagues_dict, test_mode=test_mode)
    players_list = getPlayers(teams_dict, test_mode=test_mode)

    return {
        "leagues": leagues_list,
        "teams": teams_list,
        "players": players_list
    }

def getBig5(test_mode=False):
    print("Getting Big 5 Leagues...")
    url = f"https://www.thesportsdb.com/api/v1/json/{api_key}/all_leagues.php"
    headers = {
        "Content-Type": "application/json"
    }

    response = requests.get(url, headers = headers)

    data = response.json()
    if not data["leagues"]:
        pass
    
    BIG5_NAMES = {
        ("English Premier League"),
        ("Spanish La Liga"),
        ("Italian Serie A"),
        ("German Bundesliga"),
        ("French Ligue 1")
    }

    big5 = {}
    all_leagues = []

    for league in data["leagues"]:
        time.sleep(0.5)  # Para no saturar la API
        if league["strLeague"] in BIG5_NAMES:
            print(f"Checking league: {league['strLeague']}...")
            league_id = league["idLeague"]
            league_name = league["strLeague"]
            big5[league_name] = league_id
            response = session.get(f"https://www.thesportsdb.com/api/v1/json/{api_key}/lookupleague.php?id={league_id}", headers=headers)
            league_data = response.json()
            all_leagues.append(league_data["leagues"][0])

    print("Big 5 Leagues retrieved successfully")
    return big5, all_leagues

def getTeams(big5, test_mode=False):
    print("Getting Teams...")
    teams = {}
    all_teams = []
    
    for league_name, league_id in big5.items():
        time.sleep(2)
        url = f"https://www.thesportsdb.com/api/v1/json/{api_key}/search_all_teams.php?l={league_name}"
        headers = {
            "Content-Type": "application/json"
        }
        response = session.get(url, headers = headers)

        data = response.json()
        if not data["teams"]:
            continue
        
        league_teams = data["teams"][:3] if test_mode else data["teams"]

        for team in league_teams:
            team_name = team["strTeam"]
            team_id = team["idTeam"]
            teams[team_name] = team_id

    for team_name, team_id in teams.items():
        time.sleep(2)
        print(f"Getting details for team: {team_name} (ID: {team_id})")

        url = f"https://www.thesportsdb.com/api/v1/json/{api_key}/lookupteam.php?id={team_id}"
        headers = {
            "Content-Type": "application/json"
        }
        response = session.get(url, headers = headers)
        if response.status_code != 200 or not response.text:
            print(f"⚠️  Skipping team {team_name}: status {response.status_code} or empty response")
            continue
        try:
            team_data = response.json()
        except Exception as e:
            print(f"⚠️  Skipping team {team_name}: JSON decode error: {e}")
            continue
        if not team_data or not team_data.get("teams"):
            continue
        all_teams.append(team_data["teams"][0])

    print("Teams retrieved successfully.")
    return teams, all_teams

def getPlayers(teams, test_mode=False):
    print("Getting Players...")
    players = {}
    all_players = []

    for team_name, team_id in teams.items():
        time.sleep(2)
        url = f"https://www.thesportsdb.com/api/v1/json/{api_key}/lookup_all_players.php?id={team_id}"
        headers = {
            "Content-Type": "application/json"
        }
        response = session.get(url, headers = headers, params={"id": team_id})
        try:
            data = response.json()
        except ValueError:
            continue

        players_data = data["player"][:5] if test_mode else data["player"]

        for player in players_data:
            print(f"Getting details for player: {player['strPlayer']} (ID: {player['idPlayer']})")
            all_players.append(player)

    print("Players retrieved successfully.")
    return all_players
                