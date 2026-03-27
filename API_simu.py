from fastapi import FastAPI

app = FastAPI(title="Mock Sports API")

"""
pip install fastapi uvicorn
uvicorn API_simu:app --reload --port 8001
"""

# ---------------------------------------------------------------------------
# Datos de prueba
# ---------------------------------------------------------------------------

LEAGUES = [
    {"id": 1, "name": "La Liga", "country": "Spain", "year": "1929"},
    {"id": 2, "name": "Premier League", "country": "England", "year": "1992"},
    {"id": 3, "name": "Bundesliga", "country": "Germany", "year": "1963"},
]

TEAMS = [
    {"id": 1, "name": "FC Barcelona", "league_id": 1, "city": "Barcelona"},
    {"id": 2, "name": "Real Madrid", "league_id": 1, "city": "Madrid"},
    {"id": 3, "name": "Arsenal", "league_id": 2, "city": "London"},
    {"id": 4, "name": "Manchester City", "league_id": 2, "city": "Manchester"},
    {"id": 5, "name": "Bayern Munich", "league_id": 3, "city": "Munich"},
]

PLAYERS = [
    {"id": 1, "name": "Pedri", "team_id": 1, "position": "Midfielder", "age": 22},
    {"id": 2, "name": "Lamine Yamal", "team_id": 1, "position": "Forward", "age": 17},
    {"id": 3, "name": "Vinicius Jr", "team_id": 2, "position": "Forward", "age": 24},
    {"id": 4, "name": "Bukayo Saka", "team_id": 3, "position": "Forward", "age": 23},
    {"id": 5, "name": "Erling Haaland", "team_id": 4, "position": "Forward", "age": 24},
    {"id": 6, "name": "Harry Kane", "team_id": 5, "position": "Forward", "age": 31},
]

# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.get("/leagues")
def get_leagues():
    return LEAGUES

@app.get("/teams")
def get_teams():
    return TEAMS

@app.get("/players")
def get_players():
    return PLAYERS