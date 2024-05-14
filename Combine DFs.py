import pandas as pd
from datetime import datetime
import soccerdata as sd
import os,sys,requests
import sql_connector as SQL
import config

FOTMOB_URL = config.getFotMobUrls()
FOTMOB_LEAGUE = config.getFotMobLeagueDict()
CONFIG = config.getGeneralConfig()

matches = SQL.execute_select_query("select * from match_schedule where id > 58995")

def prepare_shot_data_df(shot_data,team_data):
    shot_df = []
    for shot in shot_data:
        t = {}
        t['event_type'] = shot['eventType']
        t['is_blocked'] = shot['isBlocked']
        t['is_on_target'] = shot['isOnTarget']
        t['team'] = team_data[shot['teamId']]
        t['player'] = shot['playerName']
        t['shot_x'] = shot['x']
        t['shot_y'] = shot['y']
        t['minute'] = shot['min']
        t['min_added'] = shot['minAdded'] if shot['minAdded'] else 0
        t['goal_crossed_y'] = shot['goalCrossedY']
        t['goal_crossed_z'] = shot['goalCrossedZ']
        t['xG'] = shot['expectedGoals']
        t['xGOT'] = shot['expectedGoalsOnTarget'] if shot['expectedGoalsOnTarget'] else 0
        t['shot_type'] = shot['shotType']
        t['is_own_goal'] = shot['isOwnGoal']
        t['play_type'] = shot['situation']
        shot_df.append(t)
    return pd.DataFrame(shot_df)

def prepare_momentum_data_df(mmt_info):
    momentum_df = []
    for mmt in mmt_info:
        mmt['value'] = mmt['value']/10
        momentum_df.append(mmt)
    return momentum_df
    
def get_fotmob_data_of_match_by_id(FOTMOB_URL,match_id):
    url = FOTMOB_URL['game'].format(match_id)
    
    try:
        res = requests.get(url)
        data =res.json()
        team_data = {
            data['general']['homeTeam']['id'] : data['general']['homeTeam']['name'],
            data['general']['awayTeam']['id'] : data['general']['awayTeam']['name'],
        }
        shot_data = data['content']['shotmap']['shots']
        momentum = data['content']['momentum']
        if momentum :
            momentum = prepare_momentum_data_df(momentum['main']['data'])
        else:
            momentum = pd.DataFrame()
        return prepare_shot_data_df(shot_data,team_data),momentum
    except Exception as e :
        print("Exception " , e)

def get_dfs(ws_id,fb_id,fm_id,season,league):
    ws = sd.WhoScored(leagues=league, seasons=2000+int(season))
    fb = sd.FBref(leagues=league, seasons=2000+int(season))
    
    whoscored_events = ws.read_events(match_id=ws_id)
    fbref_events = fb.read_shot_events(match_id=fb_id)
    fotmob_shot_events,fotmob_momentum_transition = get_fotmob_data_of_match_by_id(FOTMOB_URL,fm_id)
    if fbref_events.shape[0] == fotmob_shot_events.shape[0]:
        fotmob_shot_events['shot_distance'] = [dis for dis in fbref_events['distance']]
    return whoscored_events.reset_index(),fbref_events.reset_index(),fotmob_shot_events,fotmob_momentum_transition

def change_db_match_state(state,id,db_lock=False):
    if db_lock:
        db_lock.acquire()    
    SQL.execute_update_query(f"update match_schedule set read_status='{state}' where id = {id}")
    if db_lock:
        db_lock.release()     

def add_momentum_values(ws,FM_mmt):
    ws['momentum'] = None
    period = "FirstHalf"
    if FM_mmt:
        ht_flag = True
    
        for i,row in enumerate(FM_mmt):
            # print(row)
            current_min = row['minute']
            m = row['minute']
            if ht_flag:
                if i==0:
                    prev_min = int(current_min - 1)
                else:
                    prev_min = int(FM_mmt[i-1]['minute'])
            momentum = row['value']
            if int(current_min) > 45:
                period = "SecondHalf"
            if ((int(current_min) == 45 ) & (current_min%45 != 0)) or ((int(current_min) == 90 ) & (current_min%90 != 0)):
                current_min = int(int(current_min)+(current_min%int(current_min))*10)
                min_index_query = ws[(ws['minute'] > prev_min) & (ws['minute'] <= current_min) & (ws['period'] == period)]
                ht_flag=False
                prev_min = current_min
            else:
                ht_flag=True
                min_index_query = ws[(ws['minute'] > prev_min) & (ws['minute'] <= current_min) & (ws['period'] == period)]
    
            ws.loc[ min_index_query.index , 'momentum'] = momentum
    else:
        ws['momentum'] = 0
    return ws

def merge_shot_data(df,FB,FM,shot_indexes):
    df['x_g'] = None
    df['x_got'] = None
    df['shot_distance'] = None
    df['body_part'] = None
    df['play_type'] = None
    for i,l in enumerate(shot_indexes) :
        try:
        # if 1==1:
            ws_row = df.loc[l].copy()
            fm_row = FM.iloc[i].copy()  # Assuming 'l' corresponds to the index in FM
            fb_row = FB.iloc[i].copy()  # Assuming 'l' corresponds to the index in FB
            
            # Check types and assign values
            ws_row['x_g'] = float(fm_row['xG']) if pd.notnull(fm_row['xG']) else None
            ws_row['x_got'] = float(fm_row['xGOT']) if pd.notnull(fm_row['xGOT']) else None
            ws_row['shot_distance'] = float(fm_row['shot_distance']) if pd.notnull(fm_row['shot_distance']) else None
            ws_row['body_part'] = fm_row['shot_type'] if pd.notnull(fm_row['shot_type']) else None
            ws_row['play_type'] = fm_row['play_type'] if pd.notnull(fm_row['play_type']) else None
    # Update the DataFrame
            df.loc[l] = ws_row
        except Exception as e:
            print(i,l)
            print(e)
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)
    return df

def merge_dfs(DB_id,WS,FB,FM_shot,FM_mmt,season,league):
    shot_indexes_in_ws = WS[WS['type'].str.contains('shot|goal', case=False)]
    # df = WS.copy()
    try:
        change_db_match_state('In Progress',DB_id)
        merged_shot_data = merge_shot_data(WS,FB,FM_shot,shot_indexes_in_ws.index)
        df = add_momentum_values(merged_shot_data,FM_mmt)
        change_db_match_state('Done',DB_id)
    except Exception as e :
        print(e)
        change_db_match_state('Error',DB_id)
        df=pd.DataFrame()
    return df

def get_complete_event_dataframe(DB_id,ws_id,fb_id,fm_id,season_name,league):
    season = season_name.split('-')[0]
    WS,FB,FM_shot,FM_mmt = get_dfs(ws_id,fb_id,fm_id,season,league)
    df = merge_dfs(DB_id,WS,FB,FM_shot,FM_mmt,season,league)
    directory_path = rf"D:/Visual Analysis/event dfs/{league}/{season_name}"
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
        print(f"Directory '{directory_path}' created successfully.")
    else:
        print(f"Directory '{directory_path}' already exists.")
    df.to_excel(rf"D:/Visual Analysis/event dfs/{league}/{season_name}/{DB_id}.xlsx")

match = matches[0]
ws_id = match['whoscored_id']
db_id = match['id']
fb_id = match['fbref_id']
fm_id = match['fotmob_id']
season_name = match['season']
league = match['competition']

get_complete_event_dataframe(db_id,ws_id,fb_id,fm_id,season_name,league)
