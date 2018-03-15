'''
RANKER VERSION 1.0 (3.5.18)
Py 3.X
Augustus Urschel

Objective:
1) Import the Main Twitter data

By [X]All-Time []Daily []Hourly:
    2) Pull relevant data from Master
    3) Filter out bad tweets on [X] 0 Score []Profanity []Suspected AI []Other
    4) Score & Rank Tweets on [X]Retweets []Likes []Patronage
    5) Save for Scoreboard
'''
#Setup
import os
import string
import sqlite3
from sqlite3 import Error
import time
import datetime
import sqlalchemy

#1: Prog to Import the Main Twitter Data

def find_file_location(filename):
    cwd = os.getcwd()
    pullerdir = cwd + "\\Puller\\Output Data\\"
    inputloc = pullerdir + filename
    print("Data is : " + inputloc)
    return inputloc

def create_master_connection(dbname):
    try:
        conn_master = sqlite3.connect(dbname)
        return conn_master
    except Error as e:
        print(e)

    return None

# 3 Pull relevant data from Master
def datapull_master(conn_master):
    c = conn_master.cursor()
    c.execute("SELECT (tweet_id) FROM hashtags")
    rows = c.fetchall()

    for row in rows:
        print(row)


#total retweets
#SELECT SUM(retweet_count) FROM hastags


#vars to pull:
#tweet_id, truncated, retweeted_status, created_at, content, from_user_screen_name, from_user_id, retweet_count

#need new vars: str ver of user_created_at
#filters: date of user created, no photos "entities_media_count"


#3: Prog to Score on Retweets

#4: Prog to Filter out 0 Scores

#5: Prog to Rank Tweets & Saveout

#6: Prog to Condense to Scoreboard & Saveout

#Run
if __name__ == "__main__":
    data_master_loc = find_file_location("Data_2.28.2018.sqlite") #define the main data
    conn_master = create_master_connection(data_master_loc) #connect to it
    datapull_master(conn_master) #Pull all data from hastags table and display it
    