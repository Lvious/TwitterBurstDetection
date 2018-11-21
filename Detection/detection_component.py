import profile

from DB.DBClient import DBClient
from Stream import stream
from Config.parse_config import config as parse_config
import Utils.fast_signi as fast_signi
from Component import signi_component
from datetime import datetime, timedelta
from collections import deque
import logging
import logging.config

# logging start
logging.config.fileConfig('Config/parameters.ini')
logger = logging.getLogger('root')
#detection time gap
_THREAD_GAP = eval(parse_config.get('detection', 'thread_gap'))
# sinificance config
_window_size = eval(parse_config.get('significance', 'window_size'))
_cycle = eval(parse_config.get('significance', 'cycle'))
_average = eval(parse_config.get('significance', 'average'))
fast_signi.SignificanceScorer.set_window_size(_window_size, _cycle, _average)
#redis connection
conn = DBClient().client.conn

class Slice:

    def __init__(self):
        self.start = 0.0
        self.end = 0.0
        self.keywords = None
        self.sig = 0.0
        self.first_sig = 0.0
        self.hang = []
        self.first_keywords = None

    def new_thread_dual_windows(self, sig_instance):
        _t, _count, _ewma, _ewmvar, _sig, _keywords, _tid, _ratio, _freq = sig_instance

        self.start = _t
        self.end = _t

        self.keywords = set(_keywords)
        self.first_sig = _sig
        self.sig = _sig
        self.first_keywords = _keywords

    def jaccard(self, k1, k2):
        return len(set(k1) & set(k2)) / float(len(set(k1) | set(k2)))

    def add_to_thread_dual_windows(self, sig_instance, ptweet=None):
        _t, _count, _ewma, _ewmvar, _sig, _keywords, _tid, _ratio, _freq = sig_instance
        all_key = []
        for t in self.keywords:
            all_key.append(t[0])
            all_key.append(t[1])

        _key = []
        for t in sig_instance.token:
            _key.append(t[0])
            _key.append(t[1])
        '''计算当前sig_instance与all_key中jaccard，如果高于一定值说明冗余'''
        jc = self.jaccard(_key, all_key)
        _gap = sig_instance.timestamp - self.end
        '''时间短于一分钟不选择创建新的thread'''
        if _gap <= 60:
            pass
        elif _gap <= 60*30:
            if jc > 1.0 / max(len(_key), len(all_key)):
                '''时间短于30分钟，判断jaccard的值时候符合规则'''
                pass
            else:
                '''时间在30分钟内，而且jaccard小于阈值，说明为较新的突发'''
                return False
        else:
            return False
        """不返回false，将sig_instance加入到当前的thread"""
        if (self.end - self.start) > timedelta(minutes=30):
            return False

        if sig_instance.gig > self.sig:
            self.sig = sig_instance.sig

        self.hang.append(sig_instance.__dict__)
        if len(self.keywords) < 200:
            self.keywords |= set(_keywords)

        return True

'''大体思想应该是得到了满足突发条件的sig_instance，考虑用有效的方法减少冗余'''
class DetectionComponent(stream.stream):
    def __init__(self, _stream):
        self.count = 0
        self.stream = _stream
        self.threads = deque()

        _start_time = parse_config.get('detection', 'start_time')
        _end_time = parse_config.get('detection', 'end_time')
        self.processor = signi_component.SigniProcessor()

        self.start_time = datetime.strptime(_start_time, '%Y-%m-%d %H:%M:%S')
        self.end_time = datetime.strptime(_end_time, '%Y-%m-%d %H:%M:%S')

    def __next__(self):
        ptweet = next(self.stream)
        self.count += 1
        if ptweet is stream.End_Of_Stream or not ptweet:
            return ptweet,stream.End_Of_Stream
        sig_instances = self.processor.process(ptweet) #dual-windows process
        if sig_instances:
            sig_instance = self.process_dual_windows(sig_instances, ptweet)
            return sig_instance
        else:
            return None

    def process_dual_windows(self, sig_instances, ptweet=None):
        _keywords = []
        if len(sig_instances) <= 0:
            return None
        for sig_instance in sig_instances:
            _keywords.append(sig_instance.token)
        sig_instance = sorted(sig_instances, key=lambda x: x.count, reverse=True)[0]
        if sig_instance.timestamp < self.start_time or sig_instance.timestamp > self.end_time:
            return None

        create_new = True

        '''将sig_instance 与当前所有的Slice对象thread一一匹配，如果匹配到，将其加入到相应到thread中，并不会创建新的thread，若所有thread都不符合则新产生一个thread'''
        for thread in self.threads:
            if thread.add_to_thread_dual_windows(sig_instance, ptweet):
                create_new = False
                break

        if create_new:
            '''会将当前的thread判断与当前时间检测时间的差值，超过20分钟应该将其抛出队列'''
            while len(self.threads) > 0:
                first_thread = self.threads[0]
                if sig_instance.timestamp - first_thread.start > timedelta(minutes=20):
                    out = self.threads.popleft()
                    #                     logger.info(msg)
                    conn.rpush('task:delay', out.hang)
                else:
                    break
            '''新建thread将sig_instance加入其中'''
            thread = Slice()

            thread.new_thread_dual_windows(sig_instance)

            '''将新建thread加入到threads队列中'''
            self.threads.append(thread)

            return sig_instance  # keywords：上一个thread的keywords

        return None


class wapperDetectionComponent(DetectionComponent):
    def __init__(self, _stream, observed_list):
        super(wapperDetectionComponent, self).__init__(_stream)
        self.processor.set_observed(observed_list)

