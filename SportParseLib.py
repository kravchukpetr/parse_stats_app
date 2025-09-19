import requests
import json
import psycopg2
import os
import dotenv
from datetime import datetime
import pandas as pd

BASE_URL = "https://v3.football.api-sports.io"

url_leagues = f"{BASE_URL}/leagues"
url_fixtures = f"{BASE_URL}/fixtures"
url_teams = f"{BASE_URL}/teams"

headers = {
  'x-rapidapi-key': '316dede52d52efb30d6dcc7cfb2e936b',
  'x-rapidapi-host': 'v3.football.api-sports.io'
}

def get_conn_to_pg():
    from dotenv import load_dotenv
    load_dotenv()
    return psycopg2.connect(
        database=os.environ['PG_DB'],
        user=os.environ['PG_USR'],
        password=os.environ['PG_PWD'],
        host=os.environ['PG_HOST'],
        port=os.environ['PG_PORT']
    )

def get_country_from_db():
    conn = get_conn_to_pg()
    cur = conn.cursor()

    cur.execute("SELECT name, id FROM public.football_app_country")
    rows = cur.fetchall()

    country_name_to_id = {name: id for name, id in rows}

    cur.close()
    conn.close()

    return country_name_to_id

def get_season_from_db(league_id):
    conn = get_conn_to_pg()
    cur = conn.cursor()

    cur.execute(f"SELECT name, id, start_year FROM public.football_app_season where league_id = {league_id}")
    rows = cur.fetchall()

    season_name_to_id = {start_year: id for name, id, start_year in rows}

    cur.close()
    conn.close()

    return season_name_to_id

def get_all_fixtures(params_fixture):
    page = 1
    fixtures = []

    while True:
        response_fixture = requests.get(url_fixtures, headers=headers, params=params_fixture)
        data = response_fixture.json()

        if "response" not in data or not data["response"]:
            break

        fixtures.extend(data["response"])
        
        # Проверяем, есть ли следующая страница
        paging = data.get("paging", {})
        if paging.get("current") == paging.get("total"):
            break

        page += 1
    
    fixture_file_name = f"data/fixtures_league_{params_fixture['league']}_season_{params_fixture['season']}.json"
    with open(fixture_file_name, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    return fixtures, fixture_file_name

def load_fixtures(params_fixture, is_debug=False):

    fixtures, fixture_file_name = get_all_fixtures(params_fixture)
    with open(fixture_file_name, "r", encoding="utf-8") as f:
        fixtures = json.load(f)
    
    if len(fixtures)>0:
        countries = get_country_from_db()
        seasons = get_season_from_db(params_fixture['league'])
        created_at = datetime.now().isoformat()
        updated_at = datetime.now().isoformat()

        conn = get_conn_to_pg()
        cur = conn.cursor()

        for f in fixtures['response']:
            fixture_id = f["fixture"]["id"]
            date = f["fixture"]["date"]
            referee = f["fixture"]["referee"]
            timezone = f["fixture"]["timezone"]
            timestamp = f["fixture"]["timestamp"]
            venue_id = f["fixture"]["venue"]["id"]
            venue_name = f["fixture"]["venue"]["name"]
            venue_city = f["fixture"]["venue"]["city"]
            status_long = f["fixture"]["status"]["long"]
            status_short = f["fixture"]["status"]["short"]
            status_elapsed = f["fixture"]["status"]["elapsed"]
            status_extra = f["fixture"]["status"]["extra"]
            league_id = f["league"]["id"]
            league = f["league"]["name"]
            country = f["league"]["country"]
            country_id = countries[country]
            season = f["league"]["season"]
            season_id = seasons[season]
            round_type = f["league"]["round"].split(" - ")[0] if " - " in f["league"]["round"] else f["league"]["round"]
            round_number = f["league"]["round"].split(" - ")[1] if " - " in f["league"]["round"] else f["league"]["round"]
            home_team = f["teams"]["home"]["name"]
            away_team = f["teams"]["away"]["name"]
            home_team_id = f["teams"]["home"]["id"]
            away_team_id = f["teams"]["away"]["id"]
            home_team_winner = f["teams"]["home"]["winner"]
            away_team_winner = f["teams"]["away"]["winner"]
            home_goals = f["goals"]["home"]
            away_goals = f["goals"]["away"]
            home_score_fulltime = f["score"]["fulltime"]["home"]
            away_score_fulltime = f["score"]["fulltime"]["away"]
            home_score_halftime = f["score"]["halftime"]["home"]
            away_score_halftime = f["score"]["halftime"]["away"]
            home_score_extratime = f["score"]["extratime"]["home"]
            away_score_extratime = f["score"]["extratime"]["away"]
            home_score_penalty = f["score"]["penalty"]["home"],
            away_score_penalty = f["score"]["penalty"]["away"]

            cur.execute(
                    "INSERT INTO public.football_app_fixture (id, date, referee, timezone, timestamp, venue_id," 
                    "venue_name, venue_city, status_long, status_short, status_elapsed, status_extra,"
                    "round_type, round_number, home_team_winner, away_team_winner, home_goals, away_goals,"
                    "home_score_fulltime, away_score_fulltime, home_score_halftime, away_score_halftime,"
                    "home_score_extratime, away_score_extratime, home_score_penalty, away_score_penalty,"
                    "created_at, updated_at, away_team_id, country_id, home_team_id, league_id, season_id) " 
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (id) DO NOTHING",
                    (fixture_id, date, referee, timezone, timestamp, venue_id, 
                    venue_name, venue_city, status_long, status_short, status_elapsed, status_extra,
                    round_type, round_number, home_team_winner, away_team_winner, home_goals, away_goals,
                    home_score_fulltime, away_score_fulltime, home_score_halftime, away_score_halftime,
                    home_score_extratime, away_score_extratime, home_score_penalty, away_score_penalty,
                    created_at, updated_at, away_team_id, country_id, home_team_id, league_id, season_id)
                )
        if is_debug:
            print(f"Fixtures loaded from league {params_fixture['league']} and season {params_fixture['season']}")
        conn.commit()
        cur.close()
        conn.close()
    else:
        print(f"No fixtures found in the provided file. League ID = {params_fixture['league']}, Season = {params_fixture['season']}")
        raise ValueError(f"No fixtures found in the provided file. League ID = {params_fixture['league']}, Season = {params_fixture['season']}")
    

def load_teams_to_db(params_team, is_debug=False):
    
    team_file_name = f"data/teams_league_{params_team['league']}_season_{params_team['season']}.json"
    with open(team_file_name, "r", encoding="utf-8") as f:
        teams = json.load(f)['response']

    if len(teams) > 0:
        conn = get_conn_to_pg()
        cur = conn.cursor()

        is_active = True
        created_at = datetime.now().isoformat()
        updated_at = datetime.now().isoformat()
        countries = get_country_from_db()

        
        for team in teams:
            team_id = team['team']['id']
            team_name = team['team']['name']
            team_country = team['team']['country']
            founded_year = team['team']['founded']
            team_code = team['team']['code']
            team_national = team['team']['national']
            team_logo = team['team']['logo']
            team_venue_id = team['venue']['id']
            team_venue_name = team['venue']['name'] 
            team_venue_address = team['venue']['address']
            team_venue_city = team['venue']['city']
            team_venue_capacity = team['venue']['capacity']
            team_venue_surface = team['venue']['surface']
            team_venue_image = team['venue']['image']
            country_id = countries.get(team_country)

            cur.execute(
                "INSERT INTO public.football_app_team (id, name, code, logo_image, founded_year, is_active," 
                "created_at, updated_at, country_id, national, venue_id, venue_name, " 
                "venue_address, venue_city, venue_capacity, venue_surface, venue_image) " 
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (id) DO NOTHING",
                (team_id, team_name, team_code, team_logo, founded_year, is_active, 
                created_at, updated_at, country_id, team_national, team_venue_id, team_venue_name, 
                team_venue_address, team_venue_city, team_venue_capacity, team_venue_surface, team_venue_image)
            )
        conn.commit()
        cur.close()
        conn.close()
    else:
        print(f"No teams found in the provided file. League ID = {params_team['league']}, Season = {params_team['season']}")
        raise ValueError(f"No teams found in the provided file.  League ID = {params_team['league']}, Season = {params_team['season']}")    

def load_teams(params_team, is_debug=False):
    response_teams = requests.get(url_teams, headers=headers, params=params_team)
    team_file_name = f"data/teams_league_{params_team['league']}_season_{params_team['season']}.json"
    if is_debug:
        print(team_file_name)
    with open(team_file_name, "w", encoding="utf-8") as f:
        json.dump(response_teams.json(), f, ensure_ascii=False, indent=2)
    load_teams_to_db(params_team, is_debug)
    if is_debug:
        print(f"Teams loaded from league {params_team['league']} and season {params_team['season']}")

# Load data from JSON file
def load_leagues_to_db(file_name, is_debug=False):
    major_countries = ['England', 'Spain', 'Germany', 'Italy', 'France', 'Netherlands', 'Portugal', 'Russia', 'Turkey']
    with open(file_name, "r", encoding="utf-8") as f:
        data_league = json.load(f)

    # Connect to PostgreSQL
    conn = get_conn_to_pg()
    cur = conn.cursor()

    countries = set()
    for league in data_league['response']:
        country = league['country']['name']
        code = league['country']['code']
        flag = league['country']['flag']
        is_major = country in major_countries
        countries.add((country, code, flag, is_major))
        

    countries_with_id = [
        (idx, name, code, flag, is_major)
        for idx, (name, code, flag, is_major) in enumerate(sorted(list(countries), key=lambda x: x[0]), start=1)
    ]
    created_at = datetime.now().isoformat()
    updated_at = datetime.now().isoformat()

    for country in countries_with_id:
        id = country[0]
        name = country[1]
        code = country[2]
        flag_image = country[3]
        is_major = country[4]
        cur.execute(
            "INSERT INTO public.football_app_country (id, name, code, flag_image, created_at, updated_at, is_major) VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (id) DO NOTHING",
            (id, name, code, flag_image, created_at, updated_at, is_major)
        )

    for league in data_league['response']:
        id = league['league']['id']
        league_name = league['league']['name']
        country = league['country']['name']
        logo_image = league['league']['logo']
        type = league['league']['type']
        country_id = next((c[0] for c in countries_with_id if c[1] == country), None)
        is_active = False
        if is_debug:
            print(id, league_name)
        cur.execute(
            "INSERT INTO public.football_app_league  (id, name, logo_image, is_active, country_id, type, created_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (id) DO NOTHING",
            (id, league_name, logo_image, is_active, country_id, type, created_at, updated_at)
        )

        for season in league['seasons']:            
            season_start_year = season['year']
            season_current = season['current']
            season_start = season['start']
            season_end = season['end']
            season_end_year = datetime.strptime(season['end'], "%Y-%m-%d").year if season['end'] else season_start
            season_name = f"{league_name} {season_start_year}/{str(season_end_year)[-2:]}"
            season_coverage_fixtures_events = season['coverage']['fixtures']['events']
            season_coverage_fixtures_lineups = season['coverage']['fixtures']['lineups']
            season_coverage_fixtures_statistics_fixtures = season['coverage']['fixtures']['statistics_fixtures']
            season_coverage_fixtures_statistics_players = season['coverage']['fixtures']['statistics_players']
            season_coverage_standings = season['coverage']['standings']
            season_coverage_players = season['coverage']['players']
            season_coverage_top_scorers = season['coverage']['top_scorers']
            season_coverage_top_assists = season['coverage']['top_assists']
            season_coverage_top_cards = season['coverage']['top_cards']
            season_coverage_injuries = season['coverage']['injuries']
            season_coverage_predictions = season['coverage']['predictions']
            season_coverage_odds = season['coverage']['odds']
            season_is_active = True
            cur.execute(
                "INSERT INTO public.football_app_season ("
                "name, start_year, end_year, start_date, end_date, is_current, is_active," 
                "created_at, updated_at, coverage_fixtures_events, coverage_fixtures_lineups," 
                "coverage_fixtures_statistics_fixtures, coverage_fixtures_statistics_players," 
                "coverage_injuries, coverage_odds, coverage_players, coverage_predictions," 
                "coverage_standings, coverage_top_assists, coverage_top_cards, coverage_top_scorers, league_id)" 
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"  
                "ON CONFLICT (id) DO NOTHING",
            (season_name, season_start_year, season_end_year, season_start, season_end, 
             season_current, season_is_active, created_at, updated_at,
             season_coverage_fixtures_events,
             season_coverage_fixtures_lineups,
             season_coverage_fixtures_statistics_fixtures,
             season_coverage_fixtures_statistics_players,
             season_coverage_injuries,
             season_coverage_odds,
             season_coverage_players,
             season_coverage_predictions,
             season_coverage_standings, 
             season_coverage_top_assists,
             season_coverage_top_cards,
             season_coverage_top_scorers,
             id
             )
        )

    conn.commit()
    cur.close()
    conn.close()


def load_leagues(is_debug=False):
    leagues_file_name = "data/leagues_response.json"
    payload={}
    response_leagues = requests.request("GET", url_leagues, headers=headers, data=payload)
    with open(leagues_file_name, "w", encoding="utf-8") as f:
        json.dump(response_leagues.json(), f, ensure_ascii=False, indent=4)
    load_leagues_to_db(leagues_file_name, is_debug)