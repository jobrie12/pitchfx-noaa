# -*- coding: utf-8 -*-
"""
Created on Sat Apr  7 04:19:02 2018

@author: James
"""

import pandas as pd
import sys
import argparse
from operator import itemgetter
import json
import os.path
import time

"""
parser = argparse.ArgumentParser()
parser.add_argument("input", help="The csv, xls, xlsx file that needs\
                    processing", type=str)
parser.add_argument("--output", help="Will write the aggregated list to this\
                    location/file or else will write to .json file same\
                    as input", type=str)
args = parser.parse_args()
if args.output:
    print(args.output)
"""

true_dtypes = {'game_type':'str', 'game_local_game_time':'str', 'game_gameday_sw':'str', 'stadium_id':'str', 'stadium_name':'str', 'stadium_venue_w_chan_loc':'str', 'stadium_location':'str', 'game_venue':'str', 'game_date':'str', 'team_type':'str', 'team_id':'str', 'team_name':'str', 'player_id':'str', 'player_first':'str', 'player_last':'str', 'player_num':'str', 'player_boxname':'str', 'player_rl':'str', 'player_position':'str', 'player_status':'str', 'player_avg':'float32', 'player_hr':'int8', 'player_rbi':'int8', 'player_bat_order':'int8', 'player_game_position':'str', 'player_wins':'int8', 'player_losses':'int8', 'player_era':'float', 'atbat_pitcher':'str', 'inning_num':'int8', 'inning_next.':'str', 'atbat_num':'str', 'atbat_b':'str', 'atbat_s':'str', 'atbat_o':'str', 'atbat_batter':'str', 'atbat_des':'str', 'atbat_stand':'str', 'atbat_event':'str', 'pitch_des':'str', 'pitch_id':'str', 'pitch_type':'str', 'pitch_x':'str', 'pitch_y':'str', 'pitch_on_1b':'str', 'pitch_on_2b':'str', 'atbat_score':'str', 'pitch_on_3b':'str', 'game_game_pk':'str', 'game_game_time_et':'str', 'pitch_sv_id':'str', 'atbat_pitcher.x':'str', 'inning_num.x':'str', 'inning_away_team':'str', 'inning_home_team':'str', 'atbat_num.x':'str', 'atbat_b.x':'str', 'atbat_s.x':'str', 'atbat_o.x':'str', 'atbat_start_tfs.x':'str', 'atbat_start_tfs_zulu.x':'str', 'atbat_batter.x':'str', 'atbat_b_height':'str', 'atbat_p_throws':'str', 'atbat_des.x':'str', 'atbat_des_es.x':'str', 'atbat_event.x':'str', 'pitch_des.x':'str', 'pitch_des_es.x':'str', 'pitch_type.x':'str', 'pitch_tfs':'str', 'pitch_tfs_zulu':'str', 'pitch_start_speed.x':'str', 'pitch_end_speed':'str', 'pitch_sz_top':'str', 'pitch_sz_bot':'str', 'pitch_pfx_x':'str', 'pitch_pfx_z':'str', 'pitch_px':'str', 'pitch_pz':'str', 'pitch_x0':'str', 'pitch_y0':'str', 'pitch_z0':'str', 'pitch_vx0':'str', 'pitch_vy0':'str', 'pitch_vz0':'str', 'pitch_ax':'str', 'pitch_ay':'str', 'pitch_az':'str', 'pitch_break_y':'str', 'pitch_break_angle':'str', 'pitch_break_length':'str', 'pitch_pitch_type.x':'str', 'pitch_type_confidence':'str', 'pitch_zone':'str', 'pitch_spin_dir':'str', 'pitch_spin_rate':'str', 'pitch_cc':'str', 'pitch_mt':'str', 'atbat_score.x':'str', 'atbat_home_team_runs.x':'str', 'atbat_away_team_runs.x':'str', 'atbat_event2.x':'str', 'player_bats.x':'str', 'player_team_abbrev':'str', 'player_team_id':'str', 'player_parent_team_abbrev':'str', 'player_parent_team_id':'str', 'player_current_position':'str', 'atbat_pitcher.y':'str', 'inning_num.y':'str', 'atbat_num.y':'str', 'atbat_b.y':'str', 'atbat_s.y':'str', 'atbat_o.y':'str', 'atbat_start_tfs.y':'str', 'atbat_start_tfs_zulu.y':'str', 'atbat_batter.y':'str', 'atbat_des.y':'str', 'atbat_des_es.y':'str', 'atbat_event.y':'str', 'atbat_b1':'str', 'atbat_b2':'str', 'atbat_b3':'str', 'pitch_des.y':'str', 'pitch_des_es.y':'str', 'pitch_type.y':'str', 'pitch_start_speed.y':'str', 'pitch_pitch_type.y':'str', 'atbat_score.y':'str', 'atbat_home_team_runs.y':'str', 'atbat_away_team_runs.y':'str', 'atbat_rbi':'str', 'atbat_event2.y':'str', 'player_team':'str', 'player_pos':'str', 'player_type':'str', 'player_first_name':'str', 'player_last_name':'str', 'player_jersey_number':'str', 'player_height':'str', 'player_weight':'str', 'player_bats.y':'str', 'player_throws':'str', 'player_dob':'str', 'season_avg':'str', 'season_ab':'str', 'season_h':'str', 'season_rbi':'str', 'season_hr':'str', 'season_bb':'str', 'season_so':'str', 'season_w':'str', 'season_l':'str', 'season_sv':'str', 'season_ip':'str', 'season_whip':'str', 'season_era':'str', 'career_avg':'str', 'career_ab':'str', 'career_h':'str', 'career_rbi':'str', 'career_hr':'str', 'career_bb':'str', 'career_so':'str', 'career_w':'str', 'career_l':'str', 'career_sv':'str', 'career_ip':'str', 'career_whip':'str', 'career_era':'str', 'Month_des':'str', 'Month_avg':'str', 'Month_ab':'str', 'Month_h':'str', 'Month_rbi':'str', 'Month_hr':'str', 'Month_bb':'str', 'Month_so':'str', 'Month_ip':'str', 'Month_whip':'str', 'Month_era':'str', 'Team_des':'str', 'Team_avg':'str', 'Team_ab':'str', 'Team_h':'str', 'Team_rbi':'str', 'Team_hr':'str', 'Team_bb':'str', 'Team_so':'str', 'Team_ip':'str', 'Team_whip':'str', 'Team_era':'str', 'Empty_avg':'str', 'Empty_ab':'str', 'Empty_h':'str', 'Empty_rbi':'str', 'Empty_hr':'str', 'Empty_bb':'str', 'Empty_so':'str', 'Empty_ip':'str', 'Empty_whip':'str', 'Empty_era':'str', 'Men_On_avg':'str', 'Men_On_ab':'str', 'Men_On_h':'str', 'Men_On_rbi':'str', 'Men_On_hr':'str', 'Men_On_bb':'str', 'Men_On_so':'str', 'Men_On_ip':'str', 'Men_On_whip':'str', 'Men_On_era':'str', 'RISP_avg':'str', 'RISP_ab':'str', 'RISP_h':'str', 'RISP_rbi':'str', 'RISP_hr':'str', 'RISP_bb':'str', 'RISP_so':'str', 'RISP_ip':'str', 'RISP_whip':'str', 'RISP_era':'str', 'Loaded_avg':'str', 'Loaded_ab':'str', 'Loaded_h':'str', 'Loaded_rbi':'str', 'Loaded_hr':'str', 'Loaded_bb':'str', 'Loaded_so':'str', 'Loaded_ip':'str', 'Loaded_whip':'str', 'Loaded_era':'str', 'vs_LHB_avg':'str', 'vs_LHB_ab':'str', 'vs_LHB_h':'str', 'vs_LHB_rbi':'str', 'vs_LHB_hr':'str', 'vs_LHB_bb':'str', 'vs_LHB_so':'str', 'vs_LHB_ip':'str', 'vs_LHB_whip':'str', 'vs_LHB_era':'str', 'vs_RHB_avg':'str', 'vs_RHB_ab':'str', 'vs_RHB_h':'str', 'vs_RHB_rbi':'str', 'vs_RHB_hr':'str', 'vs_RHB_bb':'str', 'vs_RHB_so':'str', 'vs_RHB_ip':'str', 'vs_RHB_whip':'str', 'vs_RHB_era':'str', 'vs_B_des':'str', 'vs_B_avg':'str', 'vs_B_ab':'str', 'vs_B_h':'str', 'vs_B_rbi':'str', 'vs_B_hr':'str', 'vs_B_bb':'str', 'vs_B_so':'str', 'vs_B_ip':'str', 'vs_B_whip':'str', 'vs_B_era':'str', 'atbat_event_num.x':'str', 'atbat_event_es.x':'str', 'pitch_event_num':'str', 'pitch_play_guid':'str', 'pitch_nasty':'str', 'atbat_event_num.y':'str', 'atbat_event_es.y':'str', 'atbat_event2_es.x':'str', 'atbat_event2_es.y':'str', 'series_des':'str', 'series_avg':'str', 'series_ab':'str', 'series_h':'str', 'series_rbi':'str', 'series_hr':'str', 'series_bb':'str', 'series_so':'str', 'series_w':'str', 'series_l':'str', 'series_sv':'str', 'series_ip':'str', 'series_whip':'str', 'series_era':'str', 'vs_B5_des':'str', 'vs_B5_avg':'str', 'vs_B5_ab':'str', 'vs_B5_h':'str', 'vs_B5_rbi':'str', 'vs_B5_hr':'str', 'vs_B5_bb':'str', 'vs_B5_so':'str', 'vs_B5_ip':'str', 'vs_B5_whip':'str', 'vs_B5_era':'str', 'Pitch_out':'str', 'atbat_start_tfs':'str', 'atbat_start_tfs_zulu':'str', 'atbat_home_team_runs':'str', 'atbat_away_team_runs':'str', 'player_bats':'str', 'atbat_end_tfs_zulu':'str', 'atbat_des_es':'str', 'atbat_event_num':'str', 'atbat_event_es':'str', 'pitch_des_es':'str', 'pitch_start_speed':'str', 'pitch_pitch_type':'str', 'atbat_event2':'str', 'atbat_event2_es':'str', 'atbat_event3':'str', 'atbat_event3.x':'str', 'atbat_event3_es.x':'str', 'atbat_event3.y':'str', 'atbat_event3_es.y':'str', 'atbat_event4.x':'str', 'atbat_event4_es.x':'str', 'atbat_event4.y':'str', 'atbat_event4_es.y':'str'}
dtypes = {'game_type':'str', 'game_local_game_time':'str', 'game_gameday_sw':'str', 'stadium_id':'str', 'stadium_name':'str', 'stadium_venue_w_chan_loc':'str', 'stadium_location':'str', 'game_venue':'str', 'game_date':'str', 'team_type':'str', 'team_id':'str', 'team_name':'str', 'player_id':'str', 'player_first':'str', 'player_last':'str', 'player_num':'str', 'player_boxname':'str', 'player_rl':'str', 'player_position':'str', 'player_status':'str', 'player_avg':'str', 'player_hr':'str', 'player_rbi':'str', 'player_bat_order':'str', 'player_game_position':'str', 'player_wins':'str', 'player_losses':'str', 'player_era':'str', 'atbat_pitcher':'str', 'inning_num':'str', 'inning_next.':'str', 'atbat_num':'str', 'atbat_b':'str', 'atbat_s':'str', 'atbat_o':'str', 'atbat_batter':'str', 'atbat_des':'str', 'atbat_stand':'str', 'atbat_event':'str', 'pitch_des':'str', 'pitch_id':'str', 'pitch_type':'str', 'pitch_x':'str', 'pitch_y':'str', 'pitch_on_1b':'str', 'pitch_on_2b':'str', 'atbat_score':'str', 'pitch_on_3b':'str', 'game_game_pk':'str', 'game_game_time_et':'str', 'pitch_sv_id':'str', 'atbat_pitcher.x':'str', 'inning_num.x':'str', 'inning_away_team':'str', 'inning_home_team':'str', 'atbat_num.x':'str', 'atbat_b.x':'str', 'atbat_s.x':'str', 'atbat_o.x':'str', 'atbat_start_tfs.x':'str', 'atbat_start_tfs_zulu.x':'str', 'atbat_batter.x':'str', 'atbat_b_height':'str', 'atbat_p_throws':'str', 'atbat_des.x':'str', 'atbat_des_es.x':'str', 'atbat_event.x':'str', 'pitch_des.x':'str', 'pitch_des_es.x':'str', 'pitch_type.x':'str', 'pitch_tfs':'str', 'pitch_tfs_zulu':'str', 'pitch_start_speed.x':'str', 'pitch_end_speed':'str', 'pitch_sz_top':'str', 'pitch_sz_bot':'str', 'pitch_pfx_x':'str', 'pitch_pfx_z':'str', 'pitch_px':'str', 'pitch_pz':'str', 'pitch_x0':'str', 'pitch_y0':'str', 'pitch_z0':'str', 'pitch_vx0':'str', 'pitch_vy0':'str', 'pitch_vz0':'str', 'pitch_ax':'str', 'pitch_ay':'str', 'pitch_az':'str', 'pitch_break_y':'str', 'pitch_break_angle':'str', 'pitch_break_length':'str', 'pitch_pitch_type.x':'str', 'pitch_type_confidence':'str', 'pitch_zone':'str', 'pitch_spin_dir':'str', 'pitch_spin_rate':'str', 'pitch_cc':'str', 'pitch_mt':'str', 'atbat_score.x':'str', 'atbat_home_team_runs.x':'str', 'atbat_away_team_runs.x':'str', 'atbat_event2.x':'str', 'player_bats.x':'str', 'player_team_abbrev':'str', 'player_team_id':'str', 'player_parent_team_abbrev':'str', 'player_parent_team_id':'str', 'player_current_position':'str', 'atbat_pitcher.y':'str', 'inning_num.y':'str', 'atbat_num.y':'str', 'atbat_b.y':'str', 'atbat_s.y':'str', 'atbat_o.y':'str', 'atbat_start_tfs.y':'str', 'atbat_start_tfs_zulu.y':'str', 'atbat_batter.y':'str', 'atbat_des.y':'str', 'atbat_des_es.y':'str', 'atbat_event.y':'str', 'atbat_b1':'str', 'atbat_b2':'str', 'atbat_b3':'str', 'pitch_des.y':'str', 'pitch_des_es.y':'str', 'pitch_type.y':'str', 'pitch_start_speed.y':'str', 'pitch_pitch_type.y':'str', 'atbat_score.y':'str', 'atbat_home_team_runs.y':'str', 'atbat_away_team_runs.y':'str', 'atbat_rbi':'str', 'atbat_event2.y':'str', 'player_team':'str', 'player_pos':'str', 'player_type':'str', 'player_first_name':'str', 'player_last_name':'str', 'player_jersey_number':'str', 'player_height':'str', 'player_weight':'str', 'player_bats.y':'str', 'player_throws':'str', 'player_dob':'str', 'season_avg':'str', 'season_ab':'str', 'season_h':'str', 'season_rbi':'str', 'season_hr':'str', 'season_bb':'str', 'season_so':'str', 'season_w':'str', 'season_l':'str', 'season_sv':'str', 'season_ip':'str', 'season_whip':'str', 'season_era':'str', 'career_avg':'str', 'career_ab':'str', 'career_h':'str', 'career_rbi':'str', 'career_hr':'str', 'career_bb':'str', 'career_so':'str', 'career_w':'str', 'career_l':'str', 'career_sv':'str', 'career_ip':'str', 'career_whip':'str', 'career_era':'str', 'Month_des':'str', 'Month_avg':'str', 'Month_ab':'str', 'Month_h':'str', 'Month_rbi':'str', 'Month_hr':'str', 'Month_bb':'str', 'Month_so':'str', 'Month_ip':'str', 'Month_whip':'str', 'Month_era':'str', 'Team_des':'str', 'Team_avg':'str', 'Team_ab':'str', 'Team_h':'str', 'Team_rbi':'str', 'Team_hr':'str', 'Team_bb':'str', 'Team_so':'str', 'Team_ip':'str', 'Team_whip':'str', 'Team_era':'str', 'Empty_avg':'str', 'Empty_ab':'str', 'Empty_h':'str', 'Empty_rbi':'str', 'Empty_hr':'str', 'Empty_bb':'str', 'Empty_so':'str', 'Empty_ip':'str', 'Empty_whip':'str', 'Empty_era':'str', 'Men_On_avg':'str', 'Men_On_ab':'str', 'Men_On_h':'str', 'Men_On_rbi':'str', 'Men_On_hr':'str', 'Men_On_bb':'str', 'Men_On_so':'str', 'Men_On_ip':'str', 'Men_On_whip':'str', 'Men_On_era':'str', 'RISP_avg':'str', 'RISP_ab':'str', 'RISP_h':'str', 'RISP_rbi':'str', 'RISP_hr':'str', 'RISP_bb':'str', 'RISP_so':'str', 'RISP_ip':'str', 'RISP_whip':'str', 'RISP_era':'str', 'Loaded_avg':'str', 'Loaded_ab':'str', 'Loaded_h':'str', 'Loaded_rbi':'str', 'Loaded_hr':'str', 'Loaded_bb':'str', 'Loaded_so':'str', 'Loaded_ip':'str', 'Loaded_whip':'str', 'Loaded_era':'str', 'vs_LHB_avg':'str', 'vs_LHB_ab':'str', 'vs_LHB_h':'str', 'vs_LHB_rbi':'str', 'vs_LHB_hr':'str', 'vs_LHB_bb':'str', 'vs_LHB_so':'str', 'vs_LHB_ip':'str', 'vs_LHB_whip':'str', 'vs_LHB_era':'str', 'vs_RHB_avg':'str', 'vs_RHB_ab':'str', 'vs_RHB_h':'str', 'vs_RHB_rbi':'str', 'vs_RHB_hr':'str', 'vs_RHB_bb':'str', 'vs_RHB_so':'str', 'vs_RHB_ip':'str', 'vs_RHB_whip':'str', 'vs_RHB_era':'str', 'vs_B_des':'str', 'vs_B_avg':'str', 'vs_B_ab':'str', 'vs_B_h':'str', 'vs_B_rbi':'str', 'vs_B_hr':'str', 'vs_B_bb':'str', 'vs_B_so':'str', 'vs_B_ip':'str', 'vs_B_whip':'str', 'vs_B_era':'str', 'atbat_event_num.x':'str', 'atbat_event_es.x':'str', 'pitch_event_num':'str', 'pitch_play_guid':'str', 'pitch_nasty':'str', 'atbat_event_num.y':'str', 'atbat_event_es.y':'str', 'atbat_event2_es.x':'str', 'atbat_event2_es.y':'str', 'series_des':'str', 'series_avg':'str', 'series_ab':'str', 'series_h':'str', 'series_rbi':'str', 'series_hr':'str', 'series_bb':'str', 'series_so':'str', 'series_w':'str', 'series_l':'str', 'series_sv':'str', 'series_ip':'str', 'series_whip':'str', 'series_era':'str', 'vs_B5_des':'str', 'vs_B5_avg':'str', 'vs_B5_ab':'str', 'vs_B5_h':'str', 'vs_B5_rbi':'str', 'vs_B5_hr':'str', 'vs_B5_bb':'str', 'vs_B5_so':'str', 'vs_B5_ip':'str', 'vs_B5_whip':'str', 'vs_B5_era':'str', 'Pitch_out':'str', 'atbat_start_tfs':'str', 'atbat_start_tfs_zulu':'str', 'atbat_home_team_runs':'str', 'atbat_away_team_runs':'str', 'player_bats':'str', 'atbat_end_tfs_zulu':'str', 'atbat_des_es':'str', 'atbat_event_num':'str', 'atbat_event_es':'str', 'pitch_des_es':'str', 'pitch_start_speed':'str', 'pitch_pitch_type':'str', 'atbat_event2':'str', 'atbat_event2_es':'str', 'atbat_event3':'str', 'atbat_event3.x':'str', 'atbat_event3_es.x':'str', 'atbat_event3.y':'str', 'atbat_event3_es.y':'str', 'atbat_event4.x':'str', 'atbat_event4_es.x':'str', 'atbat_event4.y':'str', 'atbat_event4_es.y':'str'}

cur_time = time.time()
f = open("./MLB Filter - {}.txt".format(int(cur_time)),"w+")
cur_time = time.asctime(time.gmtime())
range1 = lambda start, end: range(start, end+1)
total = 0
"""
gameFormat : "game_local_game_time stadium_id game_date"
"""
masterList = {'games':[], 'years':{}, 'stadiums':[], }
f.write("MLB Filter\n---------------\n\n")
count = [{}]
mlbList = [
"Baltimore Orioles",
"Boston Red Sox",
"Chicago White Sox",
"Cleveland Indians",
"Detroit Tigers",
"Houston Astros",
"Kansas City Royals",
"Los Angeles Angels",
"Minnesota Twins",
"New York Yankees",
"Oakland Athletics",
"Seattle Mariners",
"Tampa Bay Rays",
"Texas Rangers",
"Toronto Blue Jays",
"Arizona Diamondbacks",
"Atlanta Braves",
"Chicago Cubs",
"Cincinnati Reds",
"Colorado Rockies",
"Los Angeles Dodgers",
"Miami Marlins",
"Milwaukee Brewers",
"New York Mets",
"Philadelphia Phillies",
"Pittsburgh Pirates",
"San Diego Padres",
"San Francisco Giants",
"St. Louis Cardinals",
"Washington Nationals",
"Florida Marlins",
"Anaheim Angels",
"Tampa Bay Devil Rays"
]
total = 0
omit = 0
for i in range1(1,30):
    count.append({'ob':0,'omitted':0, 'games':0, 'years':{}, 'stadiums':0})
    df = pd.read_csv("./gps/{}.csv".format(i), dtype=dtypes, encoding='latin-1')
    f.write("------------------\nProcessing File {}\n------------------\n\n".format(i))
    curTeam = "";
    curGame = "";
    curStadium = ""
    missed = 0
    add = False
    records = []
    for index, row in df.iterrows():
        team = row["team_name"]
        game = "{} {} {}".format(row["game_local_game_time"], row["stadium_id"], row["game_date"]) 
        
        year = str(str(row["game_date"])[-4:])    
        stadium = row["stadium_id"]
        if pd.isnull(team):
            if pd.notna(row['player_team']):
                add = True
            else:
                add = False
                print(row['player_team'])
        elif team != curTeam:
            curTeam = team
            if curTeam in mlbList:
                add = True
            else:
                add = False
                print(team)
                if (missed > 0):
                    f.write("{}\n{} is not an MLB Team - Records Omitted : ".format(missed,team))
                    missed = 0
                else:
                    f.write("{} is not an MLB Team - Records Omitted : ".format(team))
        if add:
            records.append(row)
            count[i]["ob"]+= 1
            if (missed > 0):
                f.write("{}\n".format(missed))
            missed = 0
        else :
            count[i]["omitted"]+= 1
            missed += 1
    total += count[i]["ob"]
    omit += count[i]["omitted"]
    print("Processed {}:\n {}".format(i, count[i]["ob"]))
    df = pd.DataFrame(records)
    df.to_csv('C:/Users/James/Desktop/rliu/mlb-filtered-data-{}.csv'.format(i), index=False)
    f.write("------------------\nObserved in File: {}\nOmitted in File : {}\nOmission %: {}%\n------------------\n\n".format(count[i]["ob"], count[i]["omitted"], (count[i]["omitted"] / (count[i]["ob"] + count[i]["omitted"]))*100.0))
f.write("\n\n---------------\nSummary Statistics\n---------------\nTotal Observations: {}\n".format(total))
f.write("Total Omitted: {}\n".format(omit))
f.write("Total Omission %: {}\n".format(omit/(total+omit)*100.0))

f.close()
