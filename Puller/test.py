"""
To run the scripts included, you will need the basic packages that come with Anaconda (verion 2.7.14)
You will also need:
    Twython, for accessing the Twitter data
    simplejson, for parsing the JSON data that is returned by the Twitter API
    sqlite3, for exporting to SQLite
    sqlalchemy, for SQLite support

"""
# Import Python packages
import sys
import string
import simplejson
from twython import Twython

# Create day, month, year variables
import datetime
now=datetime.datetime.now()
day=int(now.day)
month=int(now.month)
year=int(now.year)

# Add OAuth tokens
t = Twython(app_key="xk2ajQiez4T2FYn9KFaNQ2P4D",
    app_secret="SVvydq1TvzC7toySPhhf0WLbqHLPqY4QUUH2hUGl6U7Lsk5enl",
    oauth_token="899444168132579328-hjXPXWDzX9GTtIsUZuXsbc0U6GoT2Tr",
    oauth_token_secret="CySbVr3cdsSCNrQh2gNT6py2V4JwKOPmOGY3blfne7KGU")

# ID Twitter users
ids = "ManRepeller,HalSinger"

# Access API
users = t.lookup_user(screen_name=ids)

# Output
outfn="twitter_user_data_%i.%i.%i.txt" % (now.month,now.day,now.year)
fields = "id screen_name name created_at url followers_count friends_count statuses_count \
    favourites_count listed_count \
    contributors_enabled description protected location lang expanded_url".split()
outfp=open(outfn,"w")
outfp.write(string.join(fields,"\t")+"\n") # header

for entry in users:
    # Create empty dictionary
    r={}
    for f in fields:
        r[f] = ""
    # Assign value of ID field in JSON to ID field in our dictionary
    r['id'] = entry['id']
    # Same with other vars
    r['name'] = entry['name']
    r['created_at'] = entry['created_at']
    r['url'] = entry['url']
    r['followers_count'] = entry['followers_count']
    r['friends_count'] = entry['friends_count']
    r['statuses_count'] = entry['statuses_count']
    r['favourites_count'] = entry['favourites_count']
    r['listed_count'] = entry['listed_count']
    r['contributors_enabled'] = entry['contributors_enabled']
    r['description'] = entry['description']
    r['protected'] = entry['protected']
    r['location'] = entry['location']
    r['lang'] = entry['lang']
    if 'url' in entry['entities']:
        r['expanded_url'] = entry['entities']['url']['urls'][0]['expanded_url']
    else:
        r['expanded_url']=''
    print r
    # Create empty list
    lst=[]
    # Add data for each var
    for f in fields:
        lst.append(unicode(r[f]).replace("\/","/"))
    # Write row with data in list
    outfp.write(string.join(lst,"\t").encode("utf-8")+"\n")
outfp.close()

