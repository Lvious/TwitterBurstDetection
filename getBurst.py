import stream
import preprocessor
import detection
import tweet_stream
from tqdm import tqdm
import sys
from datetime import datetime,timedelta
from parse_config import config as parse_config
import json
import re
import pickle
import redis
r= redis.Redis(host="54.161.160.206",port="7379")


# filename = sys.argv[1]

_HOST = parse_config.get('redis', 'host')

_PORT = eval(parse_config.get('redis', 'port'))

_KEY = parse_config.get('redis', 'key')

_EVENT_KEY = parse_config.get('redis', 'event')
# tw_stream = tweet_stream.tweetStreamFrom1dayCSV(filename)

tw_stream = tweet_stream.tweetStreamFromRedis(_HOST,_PORT,_KEY)


_processor = preprocessor.Preprocessor(tw_stream)

observed_list=['manhattan','york','truck','attack','police','terrorist']

_detection = detection.wapperDetectionComponent(_processor,observed_list)

def main():
    count = 0
    while True:
        try:
            result = next(_detection)
            count+=1
            if count%1000==0:
                print(count)
        except Exception as e:
            now = datetime.now()
            pickle.dump(_detection,open('detection_'+str(int(now.timestamp()))+'.pkl','wb'),2)
            continue
        else:
            if result is stream.End_Of_Stream:
                break
            ptweet,sig_instance = result
            if sig_instance is None:
                continue
            _t, _count, _ewma, _ewmvar, _sig ,_keywords, _tid, _ratio,_freq = sig_instance
            if _sig>0:
                event = {
                    "ratio":_sig,
                    "freq":_freq,
                    "occurtime":ptweet.datetime().strftime('%Y-%m-%d %H:%M:%S'),
                    "id":ptweet.tid,
                    "tokens":_keywords,
                    "count":_count,
                    "ewma":_ewma,
                    "ewmvar":_ewmvar,
                    "ptweet":re.sub(r'[\n]',' ',ptweet.text)
                }
                json.dumps(event)
                r.rpush(_EVENT_KEY,json.dumps(event))
                print('==============================\t'+datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                print(ptweet.datetime().strftime('%Y-%m-%d %H:%M:%S'),event)
                print('==============================')
if __name__=='__main__':
    main()
