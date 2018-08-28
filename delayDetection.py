import redis
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

_REDIS_HOST = "54.161.160.206"
_REDIS_PORT = 7379
_FETCH_KEY = 'task:delay'
_LOCS_KEY = 'task:locs'
r = redis.Redis(host=_REDIS_HOST,port=_REDIS_PORT)
class postDetection:
    def __init__(self):
        self.r = r
        self.nlp = spacy.load('en')
        self.cache=[]
        self.dtime = None
    def datetime(self,time_str):
        return datetime.datetime.strptime(time_str,'%a %b %d %H:%M:%S %z %Y')
    def __next__(self):
        if not self.key:
            return None
        raw = self.r.lpop(self.key)
        if not raw:
            return None
        else:
            tw = eval(raw.decode('utf8'))
            self.cache=[tw]
        self.tweet = tw
        self.dtime = self.datetime(tw['created_at'])
        return self.dtime.strftime('%Y-%m-%d %H:%M:%S'),self.tweet['id_str'],self.formatTweets().getEnts().parseLocs()
    def getKey(self,key_ids):
        if r.llen(key_ids)>0:
            key = r.lpop(key_ids)
            return key
        else:
            return None
    def setKey(self,key):
        self.key = key
    def processOne(self,tweet):
        if tweet is not None:
            self.cache = [tweet]
        else:
            self.cache = None
        return self
    def formatTweets(self):
        texts = []
        if self.cache is None or len(self.cache)<1:
            return self
        tweets = self.cache
        for i in tweets:
            txt = re.sub(r'[\r\n]+|&amp;','',i)
            txt = re.sub(r'(http[s]?:[\S]*?\s|http[\s\S]*?…)',' ',txt)
            txt = re.sub(r'(https://[\s\S]*)',' ',txt)
            txt = re.sub(r'(RT|@[\S]+?\s)','',txt)
            txt = re.sub(r'[^\d\w\s@]',' ', txt)
            txt = re.sub(r'[\s]{2,}',' ',txt)
            texts.append(txt.strip())
        self.cache = texts
        return self

    def getEnts(self):
        ent_doc = []
        texts = self.cache
        if self.cache is None or len(self.cache)<1:
            return self
        for t in texts:
            doc = self.nlp(t)
            locs=[]
            for l in doc:
                s = re.sub(r'[^ A-Za-z]', '', l.text)
                if l.ent_type_ in ['GPE'] and len(s.strip())>1:
                    locs.append(s)
            if len(locs)>0:
                ent_doc.append((locs,t))
        self.cache = ent_doc
        return self

    def parseLocs(self):
        condidate=[]
        ent_doc = self.cache
        if self.cache is None or len(self.cache)<1:
            return None        
        for e in ent_doc:
            for p in e[0]:
                url = "http://54.161.160.206:8090/geonames?geo="
                url+=p
                jsons = json.loads(requests.get(url).text)
                for ind in range(len(jsons['data'])):
                    if len(jsons['data'][ind]['features']):
                        loc_name = jsons['data'][ind]['features'][0]['properties']['raw']['name']
                        coord    = list(map(lambda x:float(x),jsons['data'][ind]['features'][0]['geometry']['coordinates']))
                        _text    = e[1]
                        condidate.append({'locName':loc_name,'loc':coord,'text':_text})
        return condidate
'''
{
  'count': 17.0,
  'dtime': datetime.datetime(2018, 8, 20, 14, 22, 27),
  'freq': 1.1177486094761377,
  'id': '1031547012540379136',
  'ratio': 0.328021698363461,
  'tweet': 'RT @btsarmystats: Top 1-5 Most Highest Amount of Twitter Followers gained in (🇰🇷) last week:\n\n1. @BigHitEnt - 140.0k\n\n2. @BTS_twt - 98.0k…'
}
'''    
def pushLocs(record):
    tw = record['tweet']
    locs = pd.processOne(tw).formatTweets().getEnts().parseLocs()
    if not locs:
        pass
    else:
        if len(locs)<1:
            pass
        print(locs)
        for loc in locs:
            loc['created_at'] = record['dtime'].strftime('%Y-%m-%d %H:%M:%S')
            loc['id']         = record['id']
            loc['freq']       = record['freq']
            loc['ratio']      = record['ratio'] 
            r.rpush(_LOCS_KEY,json.dumps(loc))
if __name__=="__main__":
    pd = postDetection()
    count=0
    while True:
        if count==10:
            break
        record = r.lrange(_FETCH_KEY,count,count)[0]
        if not record:
            time.sleep(3)
            print('\twait 3s')
            continue
        else:
            count+=1
            records = eval(record.decode('utf8'))
            for rec in records:
                pushLocs(rec)
#             d_str,kw_str,ids_str = record.decode('utf8').split('\t')
#             dt = eval(d_str)
#             kw = eval(kw_str)
#             ids = eval(ids_str)
#             for _id in ids:
#                 print(_id)
#                 try:
#                     tweet = api.get_status(_id)
#                 except Exception as e:
#                     print(e)
#                     continue
#                 else:
#                     tw = tweet.__dict__['_json']
#                     locs = pd.processOne(tw).formatTweets().getEnts().parseLocs()
#                     if not locs:
#                         continue
#                     else:
#                         if len(locs)<1:
#                             continue
#                         print(locs)
#                         r.rpush(_LOCS_KEY,locs)