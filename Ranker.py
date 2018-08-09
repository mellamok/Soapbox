'''
RANKER VERSION 1.0 (3.5.18)
Py 3.X
Augustus Urschel

By [X]All-Time [X]Daily [X]Hourly:
    2) Pull relevant data from Master
    3) Filter out bad tweets on [] 0 Score []Profanity []Suspected AI []Other
    4) Score & Rank Tweets on [X]Retweets [X]Likes []Patronage
    5) Save for Scoreboard
'''
#Setup
import os
import string
import sqlite3
import datetime
from sqlite3 import Error
import csv

class Ranker:
    def __init__(self, iteration, iteration_str, cycle):
        self.cycle = cycle
        self.iteration = iteration
        self.iteration_str = iteration_str
        self.iteration_input = "Puller {iteration_str}.sqlite".format(iteration_str=iteration_str)
        self.iteration_output = "Ranker {iteration_str} {cycle}.csv".format(iteration_str=iteration_str, cycle=cycle)
        '{:02d}'.format(self.iteration.month)
        '{:02d}'.format(self.iteration.day)

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
        """Pulls data from the master, scores tweets, and filters."""
        self.newtable = 'modified'
        self.oldtable = 'hashtags'
        self.scorecol = 'score'
        self.coltype = 'INTEGER'
        self.pullvars = 'tweet_id, created_at, from_user_screen_name, from_user_id, favorite_count, retweet_count, content'
        self.modvars ='tweet_id, created_at, from_user_screen_name, from_user_id, favorite_count, retweet_count, score, content'
        self.filtervars ='language, entities_media_count, retweeted_status, truncated'
        self.filters = '''language = 'en' AND entities_media_count = 0 AND retweeted_status = '' AND truncated = 0'''
        self.ordering = 'score DESC'
        self.tweet_dt = 'created_at'
        self.cron_ordering = 'created_at ASC'
        self.c = connect.cursor()
        #Create modified table (drop if exists)
        self.c.execute("DROP TABLE IF EXISTS {newtab}".format(newtab=self.newtable))
        self.c.execute("CREATE TABLE {newtab} AS SELECT {vars}, {filtervars} FROM {oldtab} ORDER BY {order}" \
            .format(newtab=self.newtable, vars=self.pullvars, filtervars=self.filtervars, oldtab=self.oldtable, order=self.cron_ordering))
        #Add in Score
        self.c.execute("ALTER TABLE {tn} ADD COLUMN '{cn}' {ct}"\
            .format(tn=self.newtable, cn=self.scorecol, ct=self.coltype))
        self.c.execute("UPDATE {tn} SET {scorecol} = {fvt} + 5*{rt}"\
            .format(tn=self.newtable, scorecol=self.scorecol, fvt='favorite_count', rt='retweet_count'))
        connect.commit()
        #Pull from table, by cycle

        if self.cycle == "alltime":
            self.c.execute("SELECT {vars_m} FROM {tn} WHERE {filter} ORDER BY {order}" \
                .format(vars_m=self.modvars, tn=self.newtable, filter=self.filters, order=self.ordering))
        elif self.cycle == "daily":
            self.sql_daily_min = ('{year}-{month}-{day} 00:00:00.000000' \
                .format(year=self.iteration.year, month='{:02d}'.format(self.iteration.month), day='{:02d}'.format(self.iteration.day)))
            self.c.execute("SELECT {vars_m} FROM {tn} WHERE {filter} AND {datecol} >= '{datelimit}' ORDER BY {order}" \
                .format(vars_m=self.modvars, tn=self.newtable, filter=self.filters, datecol=self.tweet_dt, datelimit=self.sql_daily_min, order=self.ordering))
        elif self.cycle == "hourly":
            self.sql_hourly_min = ('{year}-{month}-{day} {hour}:00:00.000000' \
                .format(year=self.iteration.year, month='{:02d}'.format(self.iteration.month), day='{:02d}'.format(self.iteration.day), hour='{:02d}'.format(self.iteration.hour)))
            self.c.execute("SELECT {vars_m} FROM {tn} WHERE {filter} AND {datecol} >= '{datelimit}' ORDER BY {order}" \
                .format(vars_m=self.modvars, tn=self.newtable, filter=self.filters, datecol=self.tweet_dt, datelimit=self.sql_hourly_min, order=self.ordering))

        self.data = self.c.fetchall() #note: This reads all data in the cursor to memory. Will cause performance issues. Change to "for row in c: print row"
        return self.data

    def to_csv(self, dataframe, iteration_output):
        """Write the data to a csv file"""
        self.cwd = os.getcwd()
        self.savedir = self.cwd + "\\CSV Output\\"
        self.csvpath = self.savedir + iteration_output
        with open(self.csvpath, 'w', newline='', encoding='utf-8') as self.csvfile: #utf-16 also works(ish)
            self.fieldnames = ['Tweet_ID', 'Tweet_Date', 'User_Name', 'User_ID', 'Favorite_CT', 'Retweet_CT', 'score', 'Content']
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
        #for row in self.x:
            #print(row)
        self.to_csv(self.x, self.iteration_output)
        self.connect.close()

#Run
if __name__ == "__main__":
    fakedate = datetime.datetime(2018, 4, 19, hour=20, minute=52, second=7)
    fakedate_str = "%d %d %d %d %d" % (fakedate.year, fakedate.month, fakedate.day, fakedate.hour, fakedate.minute)
    x = Ranker(fakedate, fakedate_str, "daily")
    x.main()