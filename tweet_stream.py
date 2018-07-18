import time
import codecs
from datetime import datetime,timedelta
import stream
import re


class tweetStreamFromCSV(stream.ItemStream):
    def __init__(self,filename,skip=1):
        self.filename = filename
        self._fp = codecs.open(filename,'r')
        if skip:
            self._fp.readline()
    def __next__(self):
        row = self._fp.readline().strip().split('\t')
#         while len(row)<5 and len(row)>1:
#             row = self._fp.readline().strip().split('\t')
        if row is "" or len(row)<=1:
            return stream.End_Of_Stream
        while len(row)<5:
            row = self._fp.readline().strip().split('\t')
        _id,_t,_tid,_uid,_tweet = row
        _t = str(int(datetime.strptime(_t,"%Y-%m-%d %H:%M:%S").timestamp()))
        _uid = _uid
        _tweet = re.sub(',',' ',_tweet.strip('\.'))
        item = stream.ItemStream(_t, _uid, _tid, _tweet)
        return item
class tweetStreamFromJSON(stream.ItemStream):
    def __init__(self,filename,skip=0):
        self.filename = filename
        self._fp = codecs.open(filename,'r')
        if skip:
            self._fp.readline()
    def __next__(self):
        line = self._fp.readline().strip()
        flag = True
        while flag:
            try:
                row = eval(line)
            except Exception as e:
                flag=True
                print(line,'eval error')
            else:
                flag = False
        if not row:
            return stream.EndOfStream
        _t,_tid,_tweet = row['ts'],row['id'],row['tweet']
        _t     = str(int(datetime.strptime(_t,"%Y-%m-%d %H:%M:%S").timestamp()))
        _tweet = re.sub(',',' ',_tweet.strip('\.'))
        item   = stream.RawTweetItem(_t, _tid, _tweet)
        return item