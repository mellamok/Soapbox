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
    pullerdir = cwd + "\\Puller Data\\"
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
    #TODO: Filter out those without soapbox hashtags
    #Also find a way to combine retweets + favs
    c.execute("SELECT tweet_id, created_at, from_user_screen_name, from_user_id, favorite_count, retweet_count, content \
    FROM hashtags WHERE language = 'en' AND entities_media_count = 0 AND retweeted_status = '' AND truncated = 0 ORDER BY favorite_count DESC")
    data = c.fetchall() #note: This reads all data in the cursor to memory. Will cause performance issues. Change to "for row in c: print row"
    return data

    #for row in rows:
    #   print(row)

def to_csv(dataframe):
    cwd = os.getcwd()
    savedir = cwd + "\\CSV Output\\"
    csvpath = savedir + "output.csv"
    with open(csvpath, 'w', newline='', encoding='utf-8') as csvfile: #utf-16 also works(ish)
        fieldnames = ['Tweet_ID', 'Tweet_Date', 'User_Name', 'User_ID', 'Favorite_CT', 'Retweet_CT', 'Content']
        dictwriter = csv.DictWriter(csvfile, fieldnames=fieldnames)
        datawriter = csv.writer(csvfile, 'excel')
        dictwriter.writeheader()
        for row in dataframe:
            datawriter.writerow(row)

    #outputcsv = csv.writer(open(csvpath, "w"))
    #for row in dataframe:
    #    outputcsv.writerow(row)

#filters: date of user created

#Run
if __name__ == "__main__":
    data_master_loc = find_file_location("Testfile 2018 4 18 19 10.sqlite") #define the main data
    conn_master = create_master_connection(data_master_loc) #connect to it
    x = datapull_master(conn_master) #Pull all data from hastags table and display it
    for row in x:
        print(row)
    to_csv(x)