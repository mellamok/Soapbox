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
import sqlalchemy
import csv

#1: Prog to Import the Main Twitter Data

def find_file_location(filename):
    """Finds the main twitter data pull."""
    cwd = os.getcwd()
    pullerdir = cwd + "\\Puller\\Output Data\\"
    inputloc = pullerdir + filename
    print("Data is : " + inputloc)
    return inputloc

def create_master_connection(dbname):
    """Connects to the maintwitter data pull"""
    try:
        conn_master = sqlite3.connect(dbname)
        return conn_master
    except Error as e:
        print(e)

    return None

# 3 Pull relevant data from Master
def datapull_master(conn_master):
    """Pulls data from the master"""
    c = conn_master.cursor()
    #TODO: Change the data types in the puller to not be all string.
    c.execute("SELECT tweet_id, created_at, from_user_screen_name, from_user_id, retweet_count, content \
    FROM hashtags WHERE entities_media_count = '' AND retweeted_status = '' AND truncated = 0 ORDER BY retweet_count ASC")
    data = c.fetchall() #note: This reads all data in the cursor to memory. Will cause performance issues. Change to "for row in c: print row"
    return data

    #for row in rows:
    #   print(row)

def to_csv(dataframe):
    cwd = os.getcwd()
    savedir = cwd + "\\Ranker\\Output\\"
    csvpath = savedir + "output.csv"
    with open(csvpath, 'w', newline='', encoding='utf-8') as csvfile: #utf-16 also works(ish)
        fieldnames = ['Tweet_ID', 'Tweet_Date', 'User_Name', 'User_ID', 'Retweet_CT', 'Content']
        dictwriter = csv.DictWriter(csvfile, fieldnames=fieldnames)
        datawriter = csv.writer(csvfile, 'excel')
        dictwriter.writeheader()
        for row in dataframe:
            datawriter.writerow(row)

    #outputcsv = csv.writer(open(csvpath, "w"))
    #for row in dataframe:
    #    outputcsv.writerow(row)



#vars to pull:
#tweet_id, truncated, retweeted_status, created_at, content, from_user_screen_name, from_user_id, retweet_count

#need new vars: str ver of user_created_at
#filters: date of user created


#3: Prog to Score on Retweets

#4: Prog to Filter out 0 Scores

#5: Prog to Rank Tweets & Saveout

#6: Prog to Condense to Scoreboard & Saveout

#Run
if __name__ == "__main__":
    data_master_loc = find_file_location("Data_2.28.2018.sqlite") #define the main data
    conn_master = create_master_connection(data_master_loc) #connect to it
    x = datapull_master(conn_master) #Pull all data from hastags table and display it
    for row in x:
        print(row)
    to_csv(x)