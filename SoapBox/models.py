from django.db import models


class User(models.Model):

    name = models.CharField(
		max_length=15,
		unique=True,
		help_text='Twitter User Name')
    priority = models.BooleanField(
		default=False,
		help_text='Users to be given priority')


class Tweet(models.Model):

    message = models.CharField(
        max_length=280,
        help_text='Tweet text content')
    score = models.PositiveSmallIntegerField(
        help_text='SoapBox score given to tweet')
