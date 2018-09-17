"""
PULLER
Date: 4 May 2018
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

import datetime
import pprint

from twython import Twython

#from SoapBox import models


# 6) Exports dataset to SQL
class Scrape:

    """Gets tweets, cleans, and inserts into database"""

    def __init__(self):
        self.ids = ["%23soapbox -filter:retweets -filter:media",
                    "@internetsoapbox -filter:retweets -filter:media",]
        # Store Twitter OAuth information, This will get moved to settings
        self.app_key = "xk2ajQiez4T2FYn9KFaNQ2P4D"
        self.app_secret = "SVvydq1TvzC7toySPhhf0WLbqHLPqY4QUUH2hUGl6U7Lsk5enl"
        self.oauth_token = "899444168132579328-hjXPXWDzX9GTtIsUZuXsbc0U6GoT2Tr"
        self.oauth_token_secret = "CySbVr3cdsSCNrQh2gNT6py2V4JwKOPmOGY3blfne7KGU"
        self.t = Twython(self.app_key, self.app_secret, self.oauth_token, self.oauth_token_secret)

    def main(self):
        """Creates database and updates database.

        Keep searching until a full page with 0 relevant tweets is found
        """
        for n, kid in enumerate(self.ids):
            print('processing id {0}/{1}'.format(n+1, len(self.ids)))

            d = self.get_data(kid)
            if not d:
                continue
            elif len(d['statuses'])==0:
                print ("No new statuses")
                continue

            write_data(d)

            last_status = d['statuses'][-1]
            min_id = last_status['id']

            max_id = min_id-1

            if len(d['statuses']) > 1:

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

                    write_data(d)

                    print ("Pages complete:", len(d['statuses']))
                    if not len(d['statuses']) > 0:
                        print ("Done. Next ID.")
                        break
                    count += 1
                    if count > 10:
                        print ("Done")
                        break

    def write_data(self, d):
        """Builds dataset from relevant tweets"""

        for entry in d['statuses']:

            tweet_id = entry['id']
            truncated = entry['truncated']
            language = entry['lang']
            possibly_sensitive = entry.get('possibly_sensitive', False)
            retweeted_status = entry.get('retweeted_status', False) # why not retweeted?
            content = entry['full_text'].replace('\n','')
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

            entities_mentions = ", ".join(entities_mentions)
            entities_hashtags = ", ".join(entities_hashtags)
            entities_media = ", ".join(entities_media)
            entities_urls = ", ".join(entities_urls)

            entities_hashtags = str(entities_hashtags)
            entities_mentions = str(entities_mentions)
            entities_media = str(entities_media)
            entities_urls = str(entities_urls)

            in_reply_to_screen_name = entry['in_reply_to_screen_name']
            in_reply_to_status_id = entry['in_reply_to_status_id']

            source = entry['source']
            json_output = str(entry)

            else:
                if len(updates) > 1:
                    print ("Warning: more than one update matching to_user=%s, text=%s"\
                            % (from_user_screen_name, content))
                else:
                    print ("Not inserting, dupe..")

    def get_data(self, kid, max_id=None):
        """Pulls relevant tweets from Twitter"""
        try:
            d = self.t.search(q=kid, count='100', result_type='recent', lang='en', max_id=max_id, tweet_mode='extended')
        except Exception as e:
            print('Error reading id {0}, exception: {1}'.format(kid, e))
            return None
        print("d.keys(): ", d.keys())
        print("Number of statuses: ", len(d['statuses']))
        return d
