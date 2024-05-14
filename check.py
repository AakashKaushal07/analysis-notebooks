import soccerdata as sd

league = "Primeira_Liga"
season = 23
ws_id =  '1748557'
fb_id = '541b66ca'

ws = sd.WhoScored(leagues=league, seasons=2000+int(season))
fb = sd.FBref(leagues=league, seasons=2000+int(season))

whoscored_events = ws.read_events(match_id=ws_id)
fbref_events = fb.read_shot_events(match_id=fb_id)

print(whoscored_events.shape, fbref_events.shape)