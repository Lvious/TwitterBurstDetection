import Stream.stream as stream
import Preprocess.preprocessor as preprocessor
import Detection.detection_component as detection
import Stream.tweet_stream as tweet_stream
from datetime import datetime
import json
import re
import pickle
import pytz
from DB.DBClient import DBClient

# tw_stream = tweet_stream.tweetStreamFrom1dayCSV(filename)
conn = DBClient().client.conn

# tw_stream = tweet_stream.tweetStreamFromRedisSimple("tweets")
tw_stream = tweet_stream.tweetStreamFromLocalCSV("D:/Datasets/temp/ts_01.json")


_processor = preprocessor.Preprocessor(tw_stream)

observed_list=['manhattan','york','truck','attack','police','terrorist']

# _detection = detection.wapperDetectionComponent(_processor,observed_list)
_detection = detection.DetectionComponent(_processor)

is_dual = True

tz = pytz.timezone("America/Virgin")
# pdb.set_trace()

def main():
    count = 0
    while True:
        try:
            result = next(_detection)#0.008s per tweet
            count+=1
            if count%1000==0:
                print(count)

        except Exception as e:
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print('stop at:',now,e)
            continue
        else:
            if result is stream.End_Of_Stream:
                pickle.dump(_detection,open('detection_'+str(int(now.timestamp()))+'.pkl','wb'),2)
                break
            sig_instance = result
            if sig_instance is None:
                continue
            if is_dual:
                if sig_instance.sig>0:
                    event = {
                        "ratio":round(sig_instance.sig,3),
                        "occurtime":datetime.fromtimestamp(sig_instance.timestamp,tz),
                        "id":sig_instance.tid,
                        "tokens":sig_instance.token,
                        "count":sig_instance.count,
                        "ewma":sig_instance.ewma,
                        "ewmvar":sig_instance.ewmvar,
                        "tweet":re.sub(r'[\r\n]',' ',sig_instance.tweet)
                    }
                    # conn.rpush("events",json.dumps(event))
                    print('==============================\t'+datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                    print(datetime.fromtimestamp(sig_instance.timestamp,tz),event)
                    print('==============================')
            else:
                if sig_instance.sig>0:
                    event = {
                        "ratio": round(sig_instance.sig, 3),
                        "occurtime": datetime.fromtimestamp(sig_instance.timestamp, tz),
                        "id": sig_instance.tid,
                        "tokens": sig_instance.token,
                        "count": sig_instance.count,
                        "ewma": sig_instance.ewma,
                        "ewmvar": sig_instance.ewmvar,
                        "tweet": re.sub(r'[\r\n]', ' ', sig_instance.tweet)
                    }    
                    print('==============================\t'+datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                    print(datetime.fromtimestamp(sig_instance.timestamp,tz),event)
                    print('==============================')
if __name__=='__main__':
    flag = 0
    if flag:
        import sys
        import line_profiler
        profile = line_profiler.LineProfiler(main)  # 把函数传递到性能分析器
        profile.enable()  # 开始分析
        main()
        profile.disable()  # 停止分析
        profile.print_stats(sys.stdout)  # 打印出性能分析结果
    else:
        main()
