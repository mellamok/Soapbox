'''
RANKER VERSION 1.0 (3.5.18)
Py 3.X
Augustus Urschel

By [X]All-Time []Daily []Hourly:
    2) Pull relevant data from Master
    3) Filter out bad tweets on [X] 0 Score []Profanity []Suspected AI []Other
    4) Score & Rank Tweets on [X]Retweets [X]Likes []Patronage
    5) Save for Scoreboard
'''
#Setup
import os
import string
import sqlite3
from sqlite3 import Error
import sqlalchemy
import csv

class Ranker:
    def __init__(self, iteration, cycle):
        self.cycle = cycle
        self.iteration = iteration
        self.iteration_input = "Puller {iteration}.sqlite".format(iteration=iteration)
        self.iteration_output = "Ranker {iteration} {cycle}.csv".format(iteration=iteration, cycle=cycle)

    def create_master_connection(self):
        """Connects to the maintwitter data pull"""
        self.cwd = os.getcwd()
        self.pullerdir = self.cwd + "\\Puller Data\\"
        self.inputloc = self.pullerdir + self.iteration_input
        try:
            self.conn_master = sqlite3.connect(self.inputloc)
            return self.conn_master
        except Error as e:
            print(e)

        return None

    def datapull_master(self, connect):
        """Pulls data from the master"""
        self.c = connect.cursor()
        self.c.execute("SELECT tweet_id, created_at, from_user_screen_name, from_user_id, favorite_count, retweet_count, content \
        FROM hashtags WHERE language = 'en' AND entities_media_count = 0 AND retweeted_status = '' AND truncated = 0 ORDER BY favorite_count DESC")
        self.data = self.c.fetchall() #note: This reads all data in the cursor to memory. Will cause performance issues. Change to "for row in c: print row"
        return self.data

        #for row in rows:
        #   print(row)

    def to_csv(self, dataframe, iteration_output):
        """Write the data to a csv file"""
        self.cwd = os.getcwd()
        self.savedir = self.cwd + "\\CSV Output\\"
        self.csvpath = self.savedir + iteration_output
        with open(self.csvpath, 'w', newline='', encoding='utf-8') as self.csvfile: #utf-16 also works(ish)
            self.fieldnames = ['Tweet_ID', 'Tweet_Date', 'User_Name', 'User_ID', 'Favorite_CT', 'Retweet_CT', 'Content']
            self.dictwriter = csv.DictWriter(self.csvfile, fieldnames=self.fieldnames)
            self.datawriter = csv.writer(self.csvfile, 'excel')
            self.dictwriter.writeheader()
            for row in dataframe:
                self.datawriter.writerow(row)

        #outputcsv = csv.writer(open(csvpath, "w"))
        #for row in dataframe:
        #    outputcsv.writerow(row)

    def main(self):
        self.connect = self.create_master_connection()
        self.x = self.datapull_master(self.connect)
        for row in self.x:
            print(row)
        self.to_csv(self.x, self.iteration_output)

#Run
if __name__ == "__main__":
    x = Ranker("2018 4 19 20 54", "alltime")
    x.main()