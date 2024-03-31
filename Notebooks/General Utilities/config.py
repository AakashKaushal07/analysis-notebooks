
from datetime import datetime
from json import JSONDecodeError
import pandas as pd

import requests
def getFotMobUrls():
    return {
    "fixture" : "https://www.fotmob.com/api/fixtures?id={}&season={}",
    "game" : "https://www.fotmob.com/api/matchDetails?matchId={}",
    }

def getFotMobLeagueDict():
    return {
    "ENG-Premier League": 47,
    "ESP-La Liga": 87,
    "ITA-Serie A": 55,
    "GER-Bundesliga":54,
    "FRA-Ligue 1": 53,
    "INT-World Cup":77,
    "UCL":42,
    "UEL":73,
    "UECL":10216,
    # "League Cup":133,
    # "FA Cup":132,
    "Eredivisie": 57,
    "Pro_League": 40 ,
    "Primeira_Liga": 61 
    }

def getGeneralConfig():
    return {
        'team_name_path' : r"C:\Users\Hp\soccerdata\config\teamname_replacements.json",
        "initial_year" : 2017,"final_year" : datetime.now().year,
        "pitch_size" : (120,80,12.75),
    }

def getFotMobSeasonFixture(league,season):
    FOTMOB_URL = getFotMobUrls()
    FOTMOB_LEAGUE = getFotMobLeagueDict()
    season_name = "%2F".join(['20'+str(x)for x in season.split('_')])
    # print(FOTMOB_URL)
    fixture_url = FOTMOB_URL['fixture'].format(FOTMOB_LEAGUE[league],season_name)
    fixture_res = requests.get(fixture_url)
    if fixture_res.status_code == 200 :
        try:
            fixtures = fixture_res.json()
            df = pd.DataFrame()
            for match in fixtures :
                i = df.shape[0]
                df.loc[i, 'id']=match['id']
                df.loc[i, 'url']=match['pageUrl']
                df.loc[i, 'home_team']=match['home']['name']
                df.loc[i, 'away_team']=match['away']['name']
                df.loc[i, 'utcTime']=match['status']['utcTime']
            return df
        except JSONDecodeError as jde :
            print("Json Error ")
            print("Data Response ",fixture_res.content.decode())
            return pd.DataFrame()
        except Exception as e :
            print("General Error ")
            print("Exception as  : ",str(e))
            return pd.DataFrame()
    else:
        return pd.DataFrame()