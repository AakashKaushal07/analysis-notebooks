import soccerdata as sd
import pandas as pd
from datetime import date, datetime,timedelta
import requests,json,config

FOTMOB_URL = config.getFotMobUrls()
FOTMOB_LEAGUE = config.getFotMobLeagueDict()
CONFIG = config.getGeneralConfig()


def getFotMobSeasonFixture(league,seasons): # Example [2016,2017] or "23_24"
    # # making season list
    if "list" in str(type(seasons)):
        # "%2F".join(['20'+str(x)for x in season.split('_')])
        season_name_str = [(f'{x%2000}-{(x+1)%2000}',f'20{str(x)}%2F20{str(x+1)}')for x in seasons]
        fixture_urls = [(season,FOTMOB_URL['fixture'].format(FOTMOB_LEAGUE[league],season_str)) for season,season_str in season_name_str]
        
    else:
        season_name_str = "%2F".join(['20'+str(x)for x in seasons.split('_')])
        fixture_urls = [FOTMOB_URL['fixture'].format(FOTMOB_LEAGUE[league],season_name)]
    all_fixtures = []
    for season_nm,fixture_url in fixture_urls:
        fixture_res = requests.get(fixture_url)
        # return
        df = pd.DataFrame()
        
        if fixture_res.status_code == 200 :
            try:
                fixtures = fixture_res.json()
                for match in fixtures :
                    i = df.shape[0]
                    df.loc[i, 'id']=match['id']
                    df.loc[i, 'league']=league.split('-')[-1].strip(' ')
                    df.loc[i, 'season']=season_nm
                    df.loc[i, 'url']=match['pageUrl']
                    df.loc[i, 'home_team']=match['home']['name']
                    df.loc[i, 'away_team']=match['away']['name']
                    df.loc[i, 'date']=match['status']['utcTime']
                all_fixtures.append(df)
            except JSONDecodeError as jde :
                print("Json Error ")
                print("Data Response ",fixture_res.content.decode())
                all_fixtures.append(df)
            except Exception as e :
                print("General Error ")
                print("Exception as  : ",str(e))
                all_fixtures.append(df)
    print(f"Returning {len(all_fixtures)} DFs combination")            
    return pd.concat(all_fixtures, ignore_index=True)    

def pre_process_dataframe_for_schedules(df_name,df_under):
    
    df_under = df_under
    if df_name == "WS":
        df_under = df_under.reset_index()
        df_under['date'] = pd.to_datetime(df_under['date']).dt.tz_localize(None).dt.tz_localize('UTC') + timedelta(hours=-1)
        df_under['season'] = df_under['season'].astype(str).apply(lambda x: x[:2] + '-' + x[2:])
    elif df_name == "FB" :
        df_under = df_under.reset_index()
        df_under['season'] = df_under['season'].astype(str).apply(lambda x: x[:2] + '-' + x[2:])
        df_under['date'] = df_under['date'].astype(str)
        df_under['time'] = df_under['time'].astype(str).replace('<NA>',"00:00")
        df_under['date'] = df_under['date']+' '+df_under['time']+":00"
        df_under['date'] = pd.to_datetime(df_under['date']).dt.tz_localize(None).dt.tz_localize('UTC') + timedelta(hours=-2)
        
    else:
        df_under = df_under.reset_index()
        df_under['date'] = pd.to_datetime(df_under['date'].str.split('Z').str[0].str.split(".").str[0],format='%Y-%m-%dT%H:%M:%S')
        # df_under['date'] = pd.to_datetime(df_under['date'].str.split('.').str[0],format='%Y-%m-%dT%H:%M:%SZ')
        # df_under['date'] = pd.to_datetime(df_under['date']) + timedelta(hours=1)
        # df_under['date'] = pd.to_datetime(df_under['date'].str.split('T').str[0])
        # df_under['date'] = pd.to_datetime(df_under['date'].str.split('T').str[0])
        df_under['date'] = df_under['date'].dt.tz_localize(None).dt.tz_localize('UTC')

    # df_under['date'] = df_under['date'].dt.date
    return df_under.sort_values(['date', 'home_team', 'away_team'])

def update_team_alias(WS_df,FB_df,FM_df,file_path,write_mode=True):
    try:
        season_set = set([x.year for x in WS_df['date']])
        if write_mode or 1:
            with open(file_path,'r') as f :
                existing_alias = json.load(f)
        else:
            existing_alias = {}
        team_alias = existing_alias      
        for season in season_set:
            # print("")
            # print(team_alias)
            WS_mw1,FB_mw1,FM_mw1 = getFirstMatchweekOfSeason(WS_df,FB_df,FM_df,season)
            if WS_mw1.empty or FB_mw1.empty or FM_mw1.empty:
                continue
            # x = getFirstMatchweekOfSeason(WS_df,FB_df,FM_df,season)
            # WS = sorted(list(set(sortedlist(WS_mw1['home_team'].unique()) + list(WS_mw1['away_team'].unique())))))
            # FB = sorted(list(set(sorted(list(FB_mw1['home_team'].unique()) + list(FB_mw1['away_team'].unique())))))
            # FM = sorted(list(set(sorted(list(FM_mw1['home_team'].unique()) + list(FM_mw1['away_team'].unique())))))
            
            WS = list(WS_mw1['home_team'].unique()) + list(WS_mw1['away_team'].unique())
            FB = list(FB_mw1['home_team'].unique()) + list(FB_mw1['away_team'].unique())
            FM = list(FM_mw1['home_team'].unique()) + list(FM_mw1['away_team'].unique())
            print(len(WS),len(FB),len(FM))
            # print(WS)
            # print(FM)
            # print(FB)
            for i,team in enumerate(WS):
                flag = False
                if team not in team_alias.keys():
                    team_alias[team] = [team]
                    flag = True
                if flag : 
                    if team != FB[i] :
                        team_alias[team].append(FB[i])
                    if team != FM[i] :
                        team_alias[team].append(FM[i])
                team_alias[team] = list(set(team_alias[team]))
        merged_alias = {**existing_alias,**team_alias}
        print(team_alias)
        if write_mode :
            with open(file_path,'w') as f :
                f.write(json.dumps(merged_alias))
        return merged_alias
    except Exception as e:
        print("Err : update_team_alias : ",e)
        import sys,os
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
        
    return False

def getFirstMatchweekOfSeason(WS,FB,FM,season):
    print(f" ===================== SEASON NAME : {season} ===================== ")
    season = season%2000
    season_name = str(season)+'-'+str(season+1)
    print(season_name)
    
    season_wide_df_WS = WS[WS['season'] == season_name]
    season_wide_df_FM = FM[FM['season'] == season_name]
    season_wide_df_FB = FB[FB['season'] == season_name]
    if season_wide_df_WS.empty or season_wide_df_FM.empty or season_wide_df_FB.empty:
        return pd.DataFrame(),pd.DataFrame(),pd.DataFrame()
    
    ht_team_count_arr =[len(list(season_wide_df_WS['home_team'].unique())),
        len(list(season_wide_df_FB['home_team'].unique())),
        len(list(season_wide_df_FM['home_team'].unique()))
    ]
    at_team_count_arr =[len(sorted(list(season_wide_df_WS['home_team'].unique()))),
        len(sorted(list(season_wide_df_FB['home_team'].unique()))),
        len(sorted(list(season_wide_df_FM['home_team'].unique())))
    ]
    
    ht_team_count = sum(ht_team_count_arr)/len(ht_team_count_arr)
    at_team_count = sum(at_team_count_arr)/len(at_team_count_arr)
    GW_WS = []
    GW_FB = []
    GW_FM = []
    if ht_team_count == at_team_count:
        team_list = list(season_wide_df_WS['home_team'].unique())
        i=0
        while len(team_list):
            # print(f"Left Teams : {len(team_list)}  | {team_list}")
            ht = season_wide_df_WS.iloc[i]['home_team'] 
            at = season_wide_df_WS.iloc[i]['away_team']
            c = 0
            if ht in team_list:
                # print("Removing HT... ",ht)
                team_list.remove(ht)
                c+=1
            if at in team_list:
                # print("Removing AT... ",at)
                team_list.remove(at)
                c+=1
            # print()
            if c != 0:
                # print(f"Adding Game : {ht} v {at} at index = {i}")
                # if i == 15:
                    # display(season_wide_df_WS.iloc[i])
                    # display(season_wide_df_FB.iloc[i])
                    # display(season_wide_df_FM.iloc[i])
                
                GW_WS.append(season_wide_df_WS.iloc[i])
                GW_FB.append(season_wide_df_FB.iloc[i])
                GW_FM.append(season_wide_df_FM.iloc[i])
                # print(f"Counts = WS : {len(GW_WS)} | FB : {len(GW_FB)} | FM : {len(GW_FM)}")
            else:
                # print(f"Can't add game : {ht} v {at} at index = {i}")
                # print("Kela and i hogya,",i, " With  HT : ",ht, " and AT : ",at)
                # print("Kela : ",team_list)
                # print(season_wide_df_WS.shape)
                if i > 120 :
                    break
            i+=1
            # print('=====================================')
    else:
        return f"Error : HT Count : {ht_team_count} | AT Count : {at_team_count}"
    return pd.DataFrame(GW_WS),pd.DataFrame(GW_FB),pd.DataFrame(GW_FM)

def combine_dataframes(WS_init,FB_init,FM_init,team_alias):
    WS = WS_init
    FB = FB_init
    FM = FM_init
    print(WS.shape)
    WS['source'] = "WhoScored"
    FM['source'] = "FotMob"
    FB['source'] = "FBref"
    try:
        WS['date'] = WS['date'].dt.date
        FM['date'] = FM['date'].dt.date
        FB['date'] = FB['date'].dt.date
    except :
        print("Ik about the dt thing")
        
    df = pd.DataFrame()
    dfs = [WS,FM,FB]
    # Find the DataFrame with the least number of rows
    loop_df = min(dfs, key=len)
    try:
        for x in WS.index:
            msg = None
            # Get the row data
            WS_row = WS.loc[x]
            # row = loop_df.loc[x]
            # print("Source is :",row['source'])
            row_home_team = WS_row['home_team']
            row_away_team = WS_row['away_team']
            row_date = WS_row['date']
            row_season = WS_row['season']
            row_round = str(WS_row['stage'])
            acceptable_rounds = ['Regular season', league, "None",None,'nan',league.split('-')[-1].strip(' '),league.replace('_',' '),
                                'Eredivisie ECL Playoff','Europa League Playoff', # Dutch League
                                 'Jupiler League','First Division A','Playoff Championship', # belgian League
                                "Champions League Group Stages","Champions League Final Stage", # UCL
                                 "Europa League Group Stages","Europa League Final Stage" #UEL
                                ]
            # print(acceptable_rounds)
            if row_round.strip(' ') not in acceptable_rounds:
                print("Row Round nikla : ",row_round)
                continue
    
            home_team_alias = team_alias[row_home_team]
            away_team_alias = team_alias[row_away_team]
    
            #Get DF ROWS
            # WS_row = WS[(WS['home_team'].isin(home_team_alias)) & (WS['away_team'].isin(away_team_alias)) & (WS['date'] == row_date)]
            FM_row = FM[(FM['home_team'].isin(home_team_alias)) & (FM['away_team'].isin(away_team_alias)) & (FM['season'] == row_season)]
            FB_row = FB[(FB['home_team'].isin(home_team_alias)) & (FB['away_team'].isin(away_team_alias)) & (FB['season'] == row_season)]
    
            if FB_row.empty or FM_row.empty or WS_row.empty:
                print("Guilty index : ",x)
                print(" HT ALias : ",home_team_alias)
                print(" AT ALias : ",away_team_alias)
                print()
                print(WS.loc[x])
                print()
                msg = f"Problem | Home : {row_home_team} | Away : {row_away_team} | Date : {row_date} \n "
                msg += f"DFs | WhoScored : {WS_row.empty} | FBRef : {FB_row.empty} | FotMob : {FM_row.empty}. \n"
                # msg += f"TIME | WhoScored : ({WS['date'][x]}) | FBRef : ({FM['date'][x]}) | FotMob : ({FB['date'][x]})."
                print(msg)
                if not FB_row.empty:
                    print("FB ROW")
                    if "Cancelled" in str(FB_row.iloc[0]['notes']):
                        continue
                return msg
            else:
                # Keeping only first row if all exist
                # print(WS_row.shape,FB_row.shape,FM_row.shape)
                try:
                    FB_row = FB_row.iloc[0]
                    FM_row = FM_row.iloc[0]
                except Exception as e:
                    print(f"DATE : {row_date} | HT : {row_home_team} | AT :{row_away_team}")
                    if "Cancelled" in str(FB_row['notes']):
                        continue
                    print("Excepitoin : ",e)
                    print("FB ROW IS  : ")
                    print(FB_row)
                    print()
                    print("FM ROW IS  : ")
                    print(FM_row)
                    print()
            # avoiding matches which are not played yet
            if str(FB_row['game_id']) == 'nan' or FB_row['match_report'] is None:
                continue
                
            i = df.shape[0]
            
            df.loc[i, 'competition']=WS_row['league']
            df.loc[i, 'season']=WS_row['season']
            df.loc[i, 'stage']=WS_row['stage']
            
            df.loc[i, 'date'] = row_date
            df.loc[i, 'home_team'] = row_home_team        
            df.loc[i, 'away_team'] = row_away_team
            # print("INDEX",df)
            df.loc[i, 'score'] = FB_row['score']
            
            df.loc[i, 'whoscored_id']=WS_row['game_id']
            df.loc[i, 'whoscored_url']=WS_row['url']
            
            df.loc[i, 'fbref_id']=FB_row['game_id']
            df.loc[i, 'fbref_url']=FB_row['match_report']
    
            df.loc[i, 'fotmob_id']=FM_row['id']
            df.loc[i, 'fotmob_url']=FM_row['url']
        return df
    except Exception as e:
        print("Merge DF Exception Occured : ",str(e))
        import sys,os
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
        print('-------')
        print('-------', msg)
        raise e

def fetch_all_dfs_for_season_of_league(league,latest_season=False):
    # league = leagues[0]
    if latest_season:
        season = [latest_season]
    else:
        season = [x for x in range(17,24)]
    # season = [x for x in range(21,24)] # For UECL ONLY
    ws = sd.WhoScored(leagues=league, seasons=season)
    fb = sd.FBref(leagues=league, seasons=season)

    # # get all 3 dfs
    # print("Fetching DataFrames ... ")
    whoscored_df = ws.read_schedule()
    fbref_df = fb.read_schedule()
    fotmob_df = getFotMobSeasonFixture(league,season)
    
    return whoscored_df,fbref_df,fotmob_df

def make_dfs_uniform(league,whoscored_df,fbref_df,fotmob_df):
    print("Preparing Fetched DataFrames ... ")
    # # prepare DFs
    WS = pre_process_dataframe_for_schedules('WS',whoscored_df)
    FB = pre_process_dataframe_for_schedules('FB',fbref_df)
    FM = pre_process_dataframe_for_schedules('FM',fotmob_df)
    print(league)
    if league == 'UCL' or league =='UEL' or league == 'UECL':
        FM_dfs = []
        WS_dfs = []
        for season in FM['season'].unique():
            temp_df = FM[FM['season'] == season]
            group_stage_start = min(FB[FB['season'] == season]['date'])
            print(f"Season {season} | Group Stage Start {group_stage_start}")
            sw_FM_df = temp_df[temp_df['date'].dt.date >=group_stage_start.date()]
            FM_dfs.append(sw_FM_df)
            if league == "UECL" :
                t_ws_df = WS[WS['season'] == season]
                print(t_ws_df.shape)
                sw_WS_df = t_ws_df[t_ws_df['date'] >=group_stage_start]
                print("MIN: ",min(t_ws_df['date']).date(),"MAX : ",max(t_ws_df['date']).date(), " VS  ",group_stage_start.date())
                WS_dfs.append(sw_WS_df)
            print(f"WS : {WS[WS['season'] == season].shape} | FB : {FB[FB['season'] == season].shape} | FM : {sw_FM_df.shape}")
        FM = pd.concat(FM_dfs)
        if league != "UECL":
            WS = WS[WS['stage'].str.contains('Final|Group Stages', case=False)]
        else:
            print([x.shape for x in WS_dfs])
            WS = pd.concat(WS_dfs)
    return WS,FB,FM
# team_aliases = update_team_alias(WS,FB,FM,CONFIG['team_name_path'])
# team_aliases = update_team_alias(WS,FB,FM,CONFIG['team_name_path'],write_mode=False)

def get_team_aliases():
    with open(CONFIG['team_name_path'],'r') as f :
        team_aliases = json.load(f)
    return team_aliases
# with open(CONFIG['team_name_path'],'r') as f :
#     team_aliases = json.load(f)

def merge_and_save_df(WS,FB,FM,team_aliases):
    print("Merging Fetched DataFrames ... ")
    merged_df = combine_dataframes(WS,FB,FM,team_aliases)
    file_name = datetime.now().strftime("%d-%m-%Y")
    merged_df.to_csv(rf'D:/Visual Analysis/Recent Runs/all_combined_{file_name}.csv')

# # get missing data info
# mergef_dfs = {}
# merged_dfs[league] = merged_df

# for k,v in merged_dfs.items():
#     v.to_csv(rf"./combined_sch/combined_{k}_1.csv")


# # get missing data info
# print("Get missing games |DataFrames ... ")
# miss_WS = WS[~WS['game_id'].isin(merged_df['whoscored_id'])]
# miss_FM = FM[~FM['id'].isin(merged_df['fotmob_id'])]
# miss_FB = FB[~FB['game_id'].isin(merged_df['fbref_id'])]

# miss_WS.to_csv(rf"./missing_matches/miss_WS_{league}.csv")
# miss_FB.to_csv(rf"./missing_matches/miss_FB_{league}.csv")
# miss_FM.to_csv(rf"./missing_matches/miss_FM_{league}.csv")

# for k,v in merged_dfs.items():
#     print(k," : ",max(v['date']))

# all_merged = pd.concat(list(merged_dfs.values()))
print("Import Successful")