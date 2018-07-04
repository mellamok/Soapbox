import os
import string
import sqlite3
import datetime
from sqlite3 import Error
'''
POPCORN
Date: 3 June 2018
Author: Augustus Urschel
This file does the following:
1) Receives the latest puller data as an input
2) If not already running, begins at the earliest tweet
3) If already running, receives the last tweetid/tweetdate
4) Locates the next tweet in line and passes it as an output
'''

class Popcorn:
    def __init__(self, iteration, iteration_str, cycle, lasttweetid, lasttweetdate):
        self.cycle = cycle
        self.iteration = iteration
        self.iteration_str = iteration_str
        self.iteration_input = "Puller {iteration_str}.sqlite".format(iteration_str=iteration_str)#change later to real input
        self.lasttweetid = lasttweetid
        self.lasttweetdate = lasttweetdate

        if not lasttweetid: 
            self.nextid = 1
            self.skipfirst = True
        else:
            self.nextid = None


    def create_master_connection(self):
        """Connects to the last data pull"""
        self.cwd = os.getcwd()
        self.pullerdir = self.cwd + "\\Puller Data\\"
        self.inputloc = self.pullerdir + self.iteration_input
        try:
            self.conn_master = sqlite3.connect(self.inputloc)
            return self.conn_master
        except Error as e:
            print(e)
        return None

    # def find_next_id(self):
    # """Given the last tweetid and tweetdate, find the next id to print"""

    def print_tweet(self, connect, nextid):
        """Sent the tweet content to the visualizer"""
        self.newtable = 'modified'
        self.vars ='from_user_screen_name, score, content'
        self.idcolumn = 'rowid'
        self.idnum = nextid
        self.c = connect.cursor()
        self.c.execute("SELECT {vars} FROM {tn} WHERE {idcol} == {idnum}" \
            .format(vars=self.vars, tn=self.newtable, idcol=self.idcolumn, idnum=self.idnum))

        self.data = self.c.fetchall() 
        return self.data

    def main(self):
        self.connect = self.create_master_connection()
        self.x = self.print_tweet(self.connect, self.nextid)
        for row in self.x:
            print(row)
        self.connect.close()

#Run as test
if __name__ == "__main__":
    fakedate = datetime.datetime(2018, 4, 19, hour=20, minute=52, second=7)
    fakedate_str = "%d %d %d %d %d" % (fakedate.year, fakedate.month, fakedate.day, fakedate.hour, fakedate.minute)
    x = Popcorn(fakedate, fakedate_str, "daily", None, None)
    print(x.nextid)
    print(x.lasttweetdate)
    print(x.lasttweetid)
    x.main()