from django.db import models


class User(models.Model):
    """Represents a SoapBox user"""

    name = models.CharField(
		max_length=15,
		unique=True,
		help_text='Twitter User Name')
    priority = models.BooleanField(
		default=False,
		help_text='Users to be given priority')


class Tweet(models.Model):
    """Represents a tweet and its ranking"""

    tweet_id = models.BigIntegerField(
        primary_key=True,
        help_text='Twitter assigned id for tweet')
    tweet_time = models.DateField()
    scrape_time = models.DateField(
        auto_now_add=True,
        help_text='Time tweet was added')
    last_updated_time = models.DateField(
        auto_now=True,
        help_text='Time of last update')
    content = models.CharField(
        max_length=280,
        help_text='Tweet text content')
    truncated = models.BooleanField(
        default=False,
        help_text='Is tweet truncated')
    language = models.CharField(
        max_length=2,
        help_text='Language of tweet')
    possibly_sensitive = models.BooleanField(
        default=False,
        help_text='Is tweet sensative')
    retweeted_status = models.BooleanField(
        default=False,
        help_text='Is tweet a retweet')
    retweet_count = models.PositiveIntegerField(
        help_text='Number of retweets')
    favorite_count = models.PositiveIntegerField(
        help_text='Number of favorites')
    score = models.PositiveSmallIntegerField(
        help_text='SoapBox score given to tweet')
