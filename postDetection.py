import redis
import spacy
import requests
import re
import json
import time
import datetime
_HOST='54.161.160.206'
_PORT='7379'
r = redis.Redis(_HOST,_PORT)
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
            txt = re.sub(r'[\r\n]+|&amp;','',i['text'])
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
            doc = nlp(t)
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
        #                 print(jsons['data'][ind]['features'][0]['properties']['raw']['name'],
        #                       jsons['data'][ind]['features'][0]['geometry']['coordinates'])
                        condidate.append((jsons['data'][ind]['features'][0]['properties']['raw']['name'],jsons['data'][ind]['features'][0]['geometry']['coordinates'],e[1]))
        return condidate
if __name__=='__main__':
    pd = postDetection()
    while True:
        key = pd.getKey('task:ids')
        if not key:
            time.sleep(1)
            continue
        else:
            while True:
                locs = next(pd)
                if not locas:
                    continue
                else:
                    if len(locs[-1])<1:
                        continue
                    else:
                        r.rpush('task:locs',locs)