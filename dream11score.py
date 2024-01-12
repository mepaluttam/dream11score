#!/usr/bin/env python
# coding: utf-8

# In[2]:


import mysql.connector

# Connect to the MySQL database
conn = mysql.connector.connect(
    host='******',
    user='***',
    password='*****',
)
# Create a cursor to execute SQL commands
cursor = conn.cursor()


# In[4]:


# Create Schema (MySQL does have explicit schema)
ipl = 'CREATE SCHEMA IF NOT EXISTS ipl;'
cursor.execute(ipl)

# Use the Schema
use_ipl = 'USE ipl;'
cursor.execute(use_ipl)


# In[5]:


# Create Table 1
table1 = '''
CREATE TABLE IF NOT EXISTS delivery (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255),
    age INT
);
'''
cursor.execute(table1)

# Create Table 2
table2 = '''
CREATE TABLE IF NOT EXISTS player_match (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255),
    age INT
);
'''
cursor.execute(table2)

# Create Table 3
table3 = '''
CREATE TABLE IF NOT EXISTS player (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255),
    age INT
);
'''
cursor.execute(table3)


# In[6]:


import pymysql
import pandas as pd
from sqlalchemy import create_engine


# In[8]:


df1 = pd.read_csv('ipl_deliveries - ipl_deliveries.csv')
df2 = pd.read_csv('Player_Match.csv')
df3 = pd.read_csv('Player.csv')


# In[9]:


# creating engine to upload datasets 
engine = create_engine("mysql+pymysql://username:password@endpoint/ipl")
df1.to_sql('delivery', con = engine, index=False, if_exists='replace')
df2.to_sql('player_match', con = engine, index=False, if_exists='replace')
df3.to_sql('player', con = engine, index=False, if_exists='replace')


# In[10]:


# now from uploaded dataset we will import datasets
#since connecion has been made we can directly import otherwise need to make connection
delivery = pd.read_sql_query('SELECT * FROM delivery',conn)
player = pd.read_sql_query('SELECT * FROM player',conn)
player_captain = pd.read_sql_query('SELECT * FROM player_match',conn)


# # bowler points

# In[14]:


bowler_data = delivery.groupby('bowler').apply(lambda x: pd.Series({
    'balls_bowled': x.shape[0],
    'runs_conceded': x['total_run'].sum(),
    'wickets_taken': x['isWicketDelivery'].sum()
})).reset_index()


# In[33]:


delivery['kind'].unique()


# In[16]:


scores = {
    'Wicket': 25,
    'LBW/Bowled': 8,
    '3 Wicket Bonus': 4,
    '4 Wicket Bonus': 8,
    '5 Wicket Bonus': 16,
    'Maiden Over': 12
}


# In[28]:


# Calculate the points for each bowler
bowler_points = delivery[delivery['kind'].isin(['lbw', 'bowled'])].groupby('bowler').apply(
    lambda x: pd.Series({
        'wickets': x['isWicketDelivery'].sum(),
        'maiden_overs': (x.groupby(['ID', 'innings', 'overs'])['total_run'].sum() == 0).sum(),
        'points': x['isWicketDelivery'].sum() * scores['Wicket'] + 
                 ((x['kind'] == 'lbw') | (x['kind'] == 'bowled')).sum() * scores['LBW/Bowled']
    })
).reset_index()


# In[29]:


bowler_points


# In[243]:


maid = delivery.groupby(['ID','innings','overs','bowler']).sum().reset_index()[['ID','innings','overs','bowler','total_run']]


# In[244]:


maid_ov = maid[maid['total_run']==0]


# In[246]:


maiden_over_points = maid_ov['bowler'].value_counts().reset_index()


# In[248]:


maiden_over_points['maiden_over_points'] = maiden_over_points['count']*12


# In[249]:


maiden_over_points


# In[69]:


bowler_per_match_wickets = delivery[delivery['kind'].isin(['caught', 'caught and bowled', 'bowled','stumped', 'lbw', 'hit wicket'])].groupby(['ID','bowler']).sum().reset_index()[['ID','bowler','isWicketDelivery']]


# In[104]:


bowler_per_match_wickets.head()


# In[96]:


# Add bonus points taking 3 or more wicktets in matches
bowler_per_match_wickets['bonus'] = bowler_per_match_wickets['isWicketDelivery'].apply(
    lambda w: scores['3 Wicket Bonus'] if w == 3 else (
             scores['4 Wicket Bonus'] if w == 4 else (
             scores['5 Wicket Bonus'] if w >= 5 else 0
    ))
)


# In[250]:


bowler_bonus = bowler_per_match_wickets.groupby('bowler').sum().reset_index()


# In[269]:


bowler_data1 = bowler_bonus.merge(maiden_over_points,on='bowler',how='left').fillna(0)


# # economy of bowler

# In[265]:


bowler_data = delivery.groupby('bowler').apply(lambda x: pd.Series({
    'balls_bowled': x.shape[0],
    'runs_conceded': x['total_run'].sum()
})).reset_index()


# In[266]:


bowler_data['bowler_economy'] = bowler_data['runs_conceded']/(bowler_data['balls_bowled']/6)
bowler_data['overs_bowled'] = round(bowler_data['balls_bowled']/6,0)


# In[267]:


def calculate_economy_rate_points(overs_bowled, runs_conceded):
    min_overs_required = 2

    # Create an empty list to store the points for each bowler
    economy_rate_points_list = []

    for overs, runs in zip(overs_bowled, runs_conceded):
        if overs < min_overs_required:
            economy_rate_points_list.append(0)
        else:
            economy_rate = runs / overs
            if economy_rate < 5:
                economy_rate_points_list.append(6)
            elif 5 <= economy_rate < 6:
                economy_rate_points_list.append(4)
            elif 6 <= economy_rate <= 7:
                economy_rate_points_list.append(2)
            elif 10 <= economy_rate <= 11:
                economy_rate_points_list.append(-2)
            elif 11.01 <= economy_rate <= 12:
                economy_rate_points_list.append(-4)
            else:
                economy_rate_points_list.append(-6)

    return economy_rate_points_list


bowler_data = pd.DataFrame({
    'bowler': bowler_data['bowler'],
    'overs_bowled': bowler_data['overs_bowled'],
    'runs_conceded': bowler_data['runs_conceded']
})

# Calculating economy rate points for all bowlers
bowler_data['economy_rate_points'] = calculate_economy_rate_points(bowler_data['overs_bowled'], bowler_data['runs_conceded'])


# In[276]:


bowler_final = bowler_data.merge(bowler_data1,on='bowler',how='left').fillna(0)[['ID','bowler','economy_rate_points','isWicketDelivery','bonus','maiden_over_points']]


# In[277]:


bowler_final['total_bowler_points'] = bowler_final['isWicketDelivery']*25 + bowler_final['economy_rate_points'] + bowler_final['bonus'] + bowler_final['maiden_over_points']


# In[280]:


bowler_final = bowler_final[['ID','bowler','total_bowler_points']]


# In[369]:


bowler_final = bowler_final.rename(columns={'bowler': 'Player_Name'})


# In[379]:


bowler_final


# # fielding points

# In[313]:


# Total catches 
caught_df = delivery[delivery['kind'] == 'caught']


# In[314]:


total_catches_by_player = caught_df.groupby('fielders_involved').size().reset_index(name='Total_Catches')


# In[315]:


def calculate_catch_points(total_catches):
    # Points for catches
    catch_points = total_catches * 8
    return catch_points


catches_df = total_catches_by_player

# Calculate catch points for each player
catches_df['Catch_Points'] = calculate_catch_points(catches_df['Total_Catches'])


# In[318]:


# total stumping 
stumped_df = delivery[delivery['kind'] == 'stumped']
total_stumped_by_player = stumped_df.groupby('fielders_involved').size().reset_index(name='Total_stumped')


# In[319]:


def calculate_stumping_points(total_stumped):
    # Points for stumpings
    stumping_points = total_stumped * 12
    return stumping_points

stumps_df = total_stumped_by_player

# Calculating stumping points for each player
stumps_df['Stumping_Points'] = calculate_stumping_points(stumps_df['Total_stumped'])


# In[321]:


stumps_df


# In[333]:


runout_df = delivery[delivery['kind'] == 'run out']


# In[335]:


runout_df = runout_df.groupby('fielders_involved').size()


# In[337]:


runout_df = runout_df.reset_index(name='Total_runouts')


# In[339]:


runout_df['Runout_Points'] = runout_df['Total_runouts'] * 12


# In[341]:


# Merge the DataFrames on the 'fielders_involved' column
merged_df = pd.merge(catches_df, stumps_df, on='fielders_involved', how='outer')
merged_df = pd.merge(merged_df, runout_df, on='fielders_involved', how='outer')

# Fill NaN values with 0 for players without catch, stumping, or runout data
merged_df = merged_df.fillna(0)

# Calculate the total points for each player
merged_df['Fielding_Total_Points'] = merged_df['Catch_Points'] + merged_df['Stumping_Points'] + merged_df['Runout_Points']


# In[342]:


Fielding_Total_Points = merged_df[['fielders_involved','Fielding_Total_Points']]


# In[343]:


Fielding_Total_Points.rename(columns={'fielders_involved': 'Player_Name'}, inplace=True)


# In[344]:


Fielding_Total_Points


# # batting points

# In[345]:


runs = delivery.groupby(['ID','batter'])['batsman_run'].sum().reset_index()
balls = delivery.groupby(['ID','batter'])['batsman_run'].count().reset_index()


# In[346]:


fours = delivery.query('batsman_run == 4').groupby(['ID','batter'])['batsman_run'].count().reset_index()
sixes = delivery.query('batsman_run == 6').groupby(['ID','batter'])['batsman_run'].count().reset_index()


# In[347]:


final_df = runs.merge(balls,on=['ID','batter'],suffixes=('_runs','_balls')).merge(fours,on=['ID','batter'],how='left').merge(sixes,on=['ID','batter'],how='left')


# In[348]:


final_df.fillna(0,inplace=True)


# In[349]:


final_df.rename(columns={
    'batsman_run_runs':'runs',
    'batsman_run_balls':'balls',
    'batsman_run_x':'fours',
    'batsman_run_y':'sixes'
},inplace=True)


# In[350]:


final_df['sr'] = round((final_df['runs']/final_df['balls'])*100,2)


# In[351]:


final_df.drop(columns=['balls'],inplace=True)


# In[353]:


temp_df = player.merge(player_captain,on='Player_Id')[['Player_Name','Match_Id','Is_Captain']]


# In[354]:


final_df = final_df.merge(temp_df,left_on=['ID','batter'],right_on=['Match_Id','Player_Name'],how='left').drop(columns=['Player_Name','Match_Id']).fillna(0)


# In[355]:


final_df = final_df.merge(balls,on=['ID','batter']).rename(columns={'batsman_run':'balls'})


# In[356]:


def batting_points(row):
  score = 0

  score = score + row['runs'] + row['fours'] + 2*row['sixes']

  if row['runs'] >= 100:
    score = score + 16
  elif row['runs'] >= 50 and row['runs'] < 100:
    score = score + 8
  elif row['runs'] >= 30 and row['runs'] < 50:
    score = score + 4
  elif row['runs'] == 0:
    score = score - 2

  if row['balls'] >= 10:
    if row['sr'] > 170:
      score = score + 6
    elif row['sr'] > 150 and row['sr'] <= 170:
      score = score + 4
    elif row['sr'] > 130 and row['sr'] <= 150:
      score = score + 2
    elif row['sr'] > 60 and row['sr'] <= 70:
      score = score - 2
    elif row['sr'] > 50 and row['sr'] <= 60:
      score = score - 4
    elif row['sr'] <= 50:
      score = score - 6
    else:
      pass

  if row['Is_Captain'] == 1:
    score = score*2


  return score


# In[357]:


final_df['score'] = final_df.apply(batting_points,axis=1)


# In[358]:


batting_points = final_df[['ID','batter','score']]


# In[360]:


batting_points = batting_points.groupby('batter').sum()


# In[361]:


batting_points = batting_points.reset_index()


# In[362]:


batting_points.rename(columns={'batter': 'Player_Name'}, inplace=True)


# In[363]:


batting_points


# In[365]:


#merging with player dataset


# In[366]:


df3 = df3.drop(columns=['Unnamed: 7','Is_Umpire'])


# In[370]:


dream11_score = pd.merge(df3,batting_points , on='Player_Name',how='right').merge(Fielding_Total_Points,on='Player_Name',how='left').merge(bowler_final,on='Player_Name',how='left').fillna(0)


# In[373]:


dream11_score['points'] =  dream11_score['score'] +  dream11_score['Fielding_Total_Points'] + dream11_score['total_bowler_points']


# In[376]:


dream11_score = dream11_score[['Player_Id','Player_Name','points']].sort_values('points', ascending=False)


# In[380]:


dream11_score


# In[ ]:




