# SportsDB/bronce.py
import json
from logging import config
import time

import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

import pyspark
from pyspark.sql import SparkSession
from ingesta import get_spark

base_url = "http://127.0.0.1:8001"
session = requests.Session()

NAMESPACE = "mockapi"  

def get_data():
    leagues_list = getBig5()
    teams_list = getTeams()
    players_list = getPlayers()

    print(leagues_list)

    return {
        "leagues": leagues_list,
        "teams": teams_list,
        "players": players_list
    }

def getBig5():
    print("Getting Big 5 Leagues...")
    url = f"{base_url}/leagues"
    headers = {
        "Content-Type": "application/json"
    }

    response = requests.get(url, headers = headers)

    leagues = response.json()

    print("Big 5 Leagues retrieved successfully")
    return leagues

def getTeams():
    print("Getting Teams...")
    url = f"{base_url}/teams"
    headers = {
        "Content-Type": "application/json"
    }

    response = requests.get(url, headers = headers)

    teams = response.json()

    print("Teams retrieved successfully")
    return teams

def getPlayers():
    print("Getting Players...")
    url = f"{base_url}/players"
    headers = {
        "Content-Type": "application/json"
    }

    response = requests.get(url, headers = headers)

    players = response.json()

    print("Players retrieved successfully")
    return players
