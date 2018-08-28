import tweepy
import json
import datetime
import re
import spacy
import requests
import time

consumerKey="99G9Y5a5fcJbhhyfoUbgCTiNk"
consumerSecret="dl48tPtdgdMHSbljpJhBWQsLwCtqdQ5GW3z1hYRSxWcM0mCThU"
accessKey="4835047375-LpPcbakJNTJxqgQzYf5Qr63XFtTFbpK8H2sPuP7"
accessSecret = "jYfOca2K7v9OU9QiPVdBlP2uwropYlrxEqQTkQl8VCDix"
auth = tweepy.OAuthHandler(consumerKey, consumerSecret)
auth.set_access_token(accessKey, accessSecret)

api = tweepy.API(auth)
_id = '1030042012391620608'
tweet = api.get_status(_id)
print(tweet.__dict__['_json'])