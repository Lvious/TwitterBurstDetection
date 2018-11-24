import codecs
from datetime import datetime

from DB.DBClient import DBClient
from Stream import stream
import re
import time
import json

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

class tweetStreamFromRedis(stream.ItemStream):
    def __init__(self,key):
        self.db= DBClient()
        self.key = key
        self.cnt = 0
    def __next__(self):
        while True:
            time.sleep(1)
            raw = self.db.lrange(self.key,self.cnt,self.cnt)[0]
            self.cnt+=1
            if not raw:
                continue
            else:
                break
            # return stream.EndOfStream
        '''raw是redis的字节形式，需要解码'''
        str_raw = raw.decode('utf8')
        tweet = json.loads(str_raw)
        _t,_tid,_tweet = str(tweet['timestamp_ms']),str(tweet['id_str']),str(tweet['text'])
        '''_t：毫秒时间戳，截取后3位到秒，比int截取方便'''
        _t     = str(_t[:-3])
        _tweet = re.sub(',',' ',_tweet.strip('\.'))
        item   = stream.RawTweetItem(_t, _tid, _tweet)
        return item
class tweetStreamFromRedisSimple(stream.ItemStream):
    def __init__(self,key):
        self.db= DBClient().client
        self.key = key
        self.cnt = 0
        self.pre_time = datetime.now().timestamp()
        self.pre_cnt = self.cnt
        self.speed = 10
    def __next__(self):
        while True:
            raw = self.db.conn.lrange(self.key,self.cnt,self.cnt)[0]
            self.cnt+=1
            if not raw:
                continue
            else:
                break
            # return stream.EndOfStream
        '''raw是redis的字节形式，需要解码'''
        try:
            str_raw = raw.decode('utf8')
        except:
            str_raw = raw.decode('gbk')

        now  = datetime.now().timestamp()
        if now-self.pre_time>self.speed:
            print("{}s:\tcurrent speed {}/s \ttotal:{}\ttimelapse:{}".format(round(now-self.pre_time),round((self.cnt-self.pre_cnt)/self.speed),self.cnt,parseHours(int(now-self.start_time))))
            self.pre_time = now
            self.pre_cnt = self.cnt
        _t,_tid,_,_tweet = str_raw.strip().split('\t')
        '''_t：毫秒时间戳，截取后3位到秒，比int截取方便'''
        # _t     = str(_t[:-3])
        # _tweet = re.sub(',',' ',_tweet.strip('\.'))
        item   = stream.RawTweetItem(_t, _tid, _tweet)
        return item
class tweetStreamFromLocalCSV(stream.ItemStream):
    def __init__(self,filename,skip=0):
        self.filename = filename
        self._fp = codecs.open(filename,'rb')
        self.cnt = 0
        self.start_time = datetime.now().timestamp()
        self.pre_time = datetime.now().timestamp()
        self.pre_cnt = self.cnt
        self.speed = 10
        if skip:
            self._fp.readline()
    def __next__(self):
        while True:
            raw = self._fp.readline().strip()
            self.cnt+=1
            if not raw:
                continue
            else:
                break
            # return stream.EndOfStream
        '''raw是redis的字节形式，需要解码'''
        try:
            str_raw = raw.decode('utf8')
        except:
            str_raw = raw.decode('gbk')

        now  = datetime.now().timestamp()
        if (now-self.pre_time)>self.speed:
            print("{}s:\tcurrent speed {}/s \ttotal:{}\ttimelapse:{}".format(round(now-self.pre_time),round((self.cnt-self.pre_cnt)/self.speed),self.cnt,parseHours(int(now-self.start_time))))
            self.pre_time = now
            self.pre_cnt = self.cnt
        _t,_tid,_,_tweet = str_raw.strip().split('\t')
        '''_t：毫秒时间戳，截取后3位到秒，比int截取方便'''
        # _t     = str(_t[:-3])
        # TODO 性能分析：正则拖慢执行速度
        # _tweet = re.sub(',',' ',_tweet.strip('\.'))
        item   = stream.RawTweetItem(_t, _tid, _tweet)
        return item
def parseHours(seconds):
    hours = int(seconds/3600)
    minutes = int((seconds-hours*3600)/60)
    seconds = seconds-hours*3600-minutes*60
    return "{:0>2}:{:0>2}:{:0>2}".format(hours,minutes,seconds)

def main():
    # ts = tweetStreamFromRedisSimple("tweets")
    ts = tweetStreamFromLocalCSV("D:/Datasets/temp/ts_01.json")
    next(ts)

if __name__ == '__main__':
    flag = 1
    if flag:
        import sys
        import line_profiler
        ts = tweetStreamFromLocalCSV("D:/Datasets/temp/ts_01.json")
        # ts = tweetStreamFromRedisSimple("tweets")
        next(ts)
        next(ts)
        next(ts)
        profile = line_profiler.LineProfiler(main)  # 把函数传递到性能分析器
        profile.enable()  # 开始分析
        main()
        profile.disable()  # 停止分析
        profile.print_stats(sys.stdout)  # 打印出性能分析结果
    else:
        main()