"""
PULLER

Version: 1.0
Date: 6 Apr 2018
Author: Kathryn Karasek

This file does the following:
1) Imports necessary packages
2) Sets program parameters
3) Structures the output dataset
4) Pulls relevant tweets from Twitter
5) Builds dataset from relevant tweets
6) Exports dataset to SQL

Outstanding issues:
* Truncating retweets
* Including tweets without #Soapbox (Why?)

"""
# 1) Imports necessary packages
import sys
import urllib
import string
import simplejson
import sqlite3

import time
import datetime
import math
from pprint import pprint

import sqlalchemy
from sqlalchemy.orm import mapper, sessionmaker
from sqlalchemy import Column, Integer, String, ForeignKey, Text, DateTime, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Unicode #
from sqlalchemy import Text #

from sqlalchemy import DECIMAL
from sqlalchemy import Unicode

from sqlalchemy.sql import join
from types import *

from datetime import datetime, date, time

# 2) Sets program parameters

# Choose how often to pull data:
now = datetime.now()
every_x_minutes=1
minute_round = math.floor(now.minute/every_x_minutes)*every_x_minutes

# Set search terms
ids = ["\"#soapbox\"",]

# Store Twitter OAuth information     
from twython import Twython

app_key = "xk2ajQiez4T2FYn9KFaNQ2P4D"
app_secret = "SVvydq1TvzC7toySPhhf0WLbqHLPqY4QUUH2hUGl6U7Lsk5enl"
oauth_token = "899444168132579328-hjXPXWDzX9GTtIsUZuXsbc0U6GoT2Tr"
oauth_token_secret = "CySbVr3cdsSCNrQh2gNT6py2V4JwKOPmOGY3blfne7KGU"

t = Twython(app_key, app_secret, oauth_token, oauth_token_secret)

# Set base class (SQL)
Base = declarative_base()

# 3) Structures the output dataset
class Messages(Base):
    __tablename__ = 'hashtags'
    
    # Set columns
    id = Column(Integer, primary_key=True) #keep
    query = Column(String) #keep
    tweet_id = Column(String) #keep
    truncated = Column(String) #keep
    language = Column(String)
    possibly_sensitive = Column(Integer)  #keep
    retweeted_status = Column(String) #keep
    created_at_text = Column(String)  
    created_at = Column(DateTime) #keep
    content = Column(Text) #keep
    from_user_screen_name = Column(String) #keep
    from_user_id = Column(String) #keep   
    from_user_created_at = Column(String) #keep  
    retweet_count = Column(Integer) #keep
    favorite_count = Column(Integer) #keep       
    entities_urls = Column(Unicode(255))
    entities_urls_count = Column(Integer)        
    entities_hashtags = Column(Unicode(255))
    entities_hashtags_count = Column(Integer)    
    entities_mentions = Column(Unicode(255))    
    entities_mentions_count = Column(Integer)
    entities_media = Column(Unicode(255))
    entities_media_count = Column(Integer)    
    in_reply_to_screen_name = Column(String)    
    in_reply_to_status_id = Column(String)  
    source = Column(String)
    json_output = Column(String)
    inserted_date = Column(DateTime)
    
    # Define intial parameters as objects in self
    def __init__(self, query, tweet_id, truncated, language, possibly_sensitive,
        retweeted_status, created_at_text, created_at, content, from_user_screen_name,
        from_user_id, from_user_created_at, retweet_count, favorite_count, entities_urls, 
        entities_urls_count, entities_hashtags, entities_hashtags_count, entities_mentions, 
        entities_mentions_count, entities_media, entities_media_count, in_reply_to_screen_name, 
        in_reply_to_status_id, source, json_output, inserted_date):        
        self.query = query
        self.tweet_id = tweet_id
        self.truncated = truncated
        self.language = language
        self.possibly_sensitive = possibly_sensitive
        self.retweeted_status = retweeted_status
        self.created_at_text = created_at_text
        self.created_at = created_at 
        self.content = content
        self.from_user_screen_name = from_user_screen_name
        self.from_user_id = from_user_id    
        self.from_user_created_at = from_user_created_at
        self.retweet_count = retweet_count
        self.favorite_count = favorite_count      
        self.entities_urls = entities_urls
        self.entities_urls_count = entities_urls_count        
        self.entities_hashtags = entities_hashtags
        self.entities_hashtags_count = entities_hashtags_count
        self.entities_mentions = entities_mentions
        self.entities_mentions_count = entities_mentions_count
        self.entities_media = entities_media
        self.entities_media_count = entities_media_count 
        self.in_reply_to_screen_name = in_reply_to_screen_name
        self.in_reply_to_status_id = in_reply_to_status_id
        self.source = source
        self.json_output = json_output
        self.inserted_date = inserted_date

# 4) Pulls relevant tweets from Twitter
def get_data(kid, max_id=None):
    try:
        d = t.search(q=kid, count = '100', result_type = 'recent', lang = 'en', max_id = max_id, tweet_mode='extended') # lang = 'en'
        
    except Exception as e:
        print ("Error reading id %s, exception: %s" % (kid, e))
        #print "Error reading id %s, exception: %s" % (kid, e)
        return None
    print ("d.keys(): ", d.keys())   
    print ("Number of statuses: ", len(d['statuses']))
    return d

# 5) Builds dataset from relevant tweets
def write_data(self, d):   

    query = d['search_metadata']['query']
    
    number_on_page = len(d['statuses'])
    ids = []
    for entry in d['statuses']:

        tweet_id = entry['id']
        truncated = entry['truncated']
        language = entry['lang']

        if 'possibly_sensitive' in entry:
            possibly_sensitive= 1
        else:
            possibly_sensitive = 0
            
        if 'retweeted_status' in entry:
            retweeted_status = 'Retweet'
        else:
            retweeted_status = ''

        content = entry['full_text']
        content = content.replace('\n','')         
        
        created_at_text = entry['created_at']     
        created_at = datetime.strptime(created_at_text, '%a %b %d %H:%M:%S +0000 %Y')
    
        from_user_screen_name = entry['user']['screen_name']
        from_user_id = entry['user']['id'] 
        from_user_created_at = entry['user']['created_at']
        
        retweet_count = entry['retweet_count'] 
        favorite_count = entry['favorite_count']

        entities_urls_count = len(entry['entities']['urls'])    
        entities_hashtags_count = len(entry['entities']['hashtags'])   
        entities_mentions_count = len(entry['entities']['user_mentions'])         
                
        entities_hashtags = []
        for hashtag in entry['entities']['hashtags']:
            if 'text' in hashtag:
                tag = hashtag['text']
                entities_hashtags.append(tag)
        
        entities_mentions = []
        for at in entry['entities']['user_mentions']:
            if 'screen_name' in at:
                mention = at['screen_name']
                entities_mentions.append(mention)
        
        entities_media = []
        entities_media_count = 0
        if 'media' in entry['entities']:
            entities_media_count = 1
            for medium in entry['entities']['media']:
                if 'type' in medium:
                    medium_type = medium['type']
                    entities_media.append(medium_type)
        
        entities_urls = []
        
        for link in entry['entities']['urls']:
            if 'url' in link:
                url = link['url']
                entities_urls.append(url)
                
        entities_mentions = string.join(entities_mentions, u", ")
        entities_hashtags = string.join(entities_hashtags, u", ")
        entities_media = string.join(entities_media, u", ") 
        entities_urls = string.join(entities_urls, u", ")
               
        entities_hashtags = unicode(entities_hashtags)
        entities_mentions = unicode(entities_mentions)
        entities_media = unicode(entities_media)
        entities_urls = unicode(entities_urls)

        in_reply_to_screen_name = entry['in_reply_to_screen_name']
        in_reply_to_status_id = entry['in_reply_to_status_id']

        source = entry['source'] 
        json_output = str(entry)

        inserted_date = datetime.now()

        updates = self.session.query(Messages).filter_by(query=query, from_user_screen_name=from_user_screen_name,
                content=content).all() 
        if not updates:             
      
            upd = Messages(query, tweet_id, truncated, language, possibly_sensitive,
            retweeted_status, created_at_text, created_at, content, from_user_screen_name,
            from_user_id, from_user_created_at, retweet_count, favorite_count, entities_urls, 
            entities_urls_count, entities_hashtags, entities_hashtags_count, entities_mentions, 
            entities_mentions_count, entities_media, entities_media_count, in_reply_to_screen_name, 
            in_reply_to_status_id, source, json_output, inserted_date)

            self.session.add(upd)
      
        else:
            if len(updates) > 1:
                print ("Warning: more than one update matching to_user=%s, text=%s"\
                        % (to_user, content))
            else:
                print ("Not inserting, dupe..")
        
        self.session.commit()
        
# 6) Exports dataset to SQL
class Scrape:
    def __init__(self): 
        # Name SQL file after minute created (for Ranker pull)   
        engine = sqlalchemy.create_engine("sqlite:///Puller Data/Testfile %d %d %d %d %d.sqlite" 
        % (now.year, now.month, now.day, now.hour, minute_round), echo=False)  
        Session = sessionmaker(bind=engine)
        self.session = Session()  
        Base.metadata.create_all(engine)

    # Keep searching until a full page with 0 relevant tweets is found
    def main(self):
        for n, kid in enumerate(ids):
            print ("\rprocessing id %s/%s" % (n+1, len(ids)),
            sys.stdout.flush())

            d = get_data(kid)
            if not d:
                continue	 
            
            if len(d['statuses'])==0:
                print ("No new statuses")
                continue
                
            write_data(self, d)

            self.session.commit() 
                    
            last_status = d['statuses'][-1]
            min_id = last_status['id']
            
            max_id = min_id-1        

            if len(d['statuses']) >1:
          
                print ("Now looking earlier...")
              
                count = 2
                while count < 10:
                    print ("Page:", count)
                    d = get_data(kid, max_id)
                    
                    if not d:
                        break
                    elif not d['statuses']:
                        
                        break	
                    
                    last_status = d['statuses'][-1]
                    min_id = last_status['id']
                
                    max_id = min_id-1

                    if not d:
                        continue	       

                    write_data(self, d) 
                    self.session.commit()
                    
                    print ("Pages complete:", len(d['statuses']), count)
                    if not len(d['statuses']) > 0:
    
                        print ("Done. Next ID.")
                        break                    
                    count += 1
                    if count >10:
                        print ("Done")
                        break
            self.session.commit()


        self.session.close()



if __name__ == "__main__":
    s = Scrape()
    s.main()