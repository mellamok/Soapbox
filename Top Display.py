import os
import string
import sqlite3
import datetime
from time import sleep
from sqlite3 import Error
'''
TOP DISPLAY
Date: August 2018
Author: Augustus Urschel
This file does the following:
1) Receives the relevent table as an input
2) Outputs the top X tweets with a delay of Y
'''

class Topdisplay:
    def __init__(self, iteration, iteration_str, table, topnum, delay):
        self.table = table
        self.topnum = topnum
        self.delay = delay
        self.iteration = iteration
        self.iteration_str = iteration_str
        self.iteration_input = "Puller {iteration_str}.sqlite".format(iteration_str=iteration_str)#change later to real input

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

    def print_tweet(self, connect, table, id):
        """Sent the tweet content to the visualizer"""
        self.table = table
        self.vars ='from_user_screen_name, score, content'
        self.idcolumn = 'rowid'
        self.id = id
        self.c = connect.cursor()

        self.c.execute("SELECT {vars} FROM {tn} WHERE {idcol} == {idnum}" \
            .format(vars=self.vars, tn=self.table, idcol=self.idcolumn, idnum=self.id))
        self.displaydata = self.c.fetchone() 
        return self.displaydata

    def main(self):
        self.connect = self.create_master_connection()
        for num in range(1,self.topnum):
            self.tweet = self.print_tweet(self.connect, self.table, num)
            print(self.tweet)
            sleep(self.delay)

#Run as test
if __name__ == "__main__":
    fakedate = datetime.datetime(2018, 4, 19, hour=20, minute=52, second=7)
    fakedate_str = "%d %d %d %d %d" % (fakedate.year, fakedate.month, fakedate.day, fakedate.hour, fakedate.minute)
    x = Topdisplay(fakedate, fakedate_str, "daytable", 5, 5)
    x.main()
