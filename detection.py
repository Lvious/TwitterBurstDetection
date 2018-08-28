import stream
from parse_config import config as parse_config
import signi_processor
from datetime import datetime,timedelta
import fast_signi
from functools import reduce
from collections import deque
import logging
import logging.config
from collections import Counter
import redis
r= redis.Redis(host="54.161.160.206",port="7379")
# import pdb
# pdb.set_trace()

# logging start
logging.config.fileConfig('parameters.ini')

logger = logging.getLogger('root')

_THREAD_GAP = eval(parse_config.get('detection', 'thread_gap'))


class Slice:

    def __init__(self):
        self.start = 0.0
        self.end = 0.0
        self.keywords = None
        self.sig = 0.0
        self.first_sig = 0.0
        self.hang = []
        self.first_keywords = None
    def new_thread(self, sig_instance):
        _t, _count, _ewma, _ewmvar, _sig, _keywords = sig_instance
        self.start = _t
        self.end = _t
        _kw1=_keywords[0]
        _kw2=_keywords[1]
        self.keywords = set([_kw1,_kw2])
#         self.thread.append(sig_instance)
        self.first_sig = _sig
        self.sig = _sig
        self.first_keywords = _kw1+','+_kw2
    def new_thread_dual_w(self, sig_instance):
        _t, _count, _ewma, _ewmvar, _sig, _keywords, _tid, _ratio,_freq = sig_instance
        
        self.start = _t
        self.end = _t

        self.keywords = set(_keywords)
#         self.thread.append(sig_instance)
        self.first_sig = _sig
        self.sig = _sig
        self.first_keywords = _keywords
    def add_to_thread(self, sig_instance):
        _t, _count, _ewma, _ewmvar, _sig, _keywords = sig_instance

        kw1, kw2 = _keywords[0],_keywords[1]

        if len(self.keywords) >= 10:
            return False

        if _t - self.end <= timedelta(minutes=1):
            if kw1 not in self.keywords and kw2 not in self.keywords:
#                 return False
                pass
        elif kw1 not in self.keywords or kw2 not in self.keywords:
            return False

        if _t - self.end > timedelta(minutes=_THREAD_GAP):
            return False

        if _sig > self.sig:
            self.sig = _sig

        self.end = _t
#         self.thread.append(sig_instance)
        self.keywords.add(kw1)
        self.keywords.add(kw2)

        return True
    def jaccard(self,k1,k2):
        return len(set(k1)&set(k2))/float(len(set(k1)|set(k2)))
    def add_to_thread_dual_w(self, sig_instance,ptweet=None):
        _t, _count, _ewma, _ewmvar, _sig, _keywords, _tid, _ratio,_freq= sig_instance
        all_key=[]
        for t in self.keywords:
            all_key.append(t[0])
            all_key.append(t[1])
            
        _key=[]
        for t in _keywords:
            _key.append(t[0])
            _key.append(t[1])            

        jc = self.jaccard(_key,all_key)
        _gap = _t - self.end
        
        if _gap<=timedelta(minutes=1):
            pass
        elif _gap<=timedelta(minutes=30):
            if jc>1.0/max(len(_key),len(all_key)):
                pass
            else:
                return False
        else:
            return False
        if self.end-self.start>timedelta(minutes=30):
            return False
        if _sig > self.sig:
            self.sig = _sig

#         self.end = _t
        #add hang result in one list 
        self.hang.append({'id':_tid,'tweet':ptweet.text,'ratio':_ratio,'freq':_freq,'count':_count,'dtime':_t})
        if len(self.keywords)<200:
            self.keywords|=set(_keywords)

        return True
    
    
class DetectionComponent(stream.stream):
    def __init__(self,_stream):
        self.count=0
        self.stream     = _stream
#         self.threads = list()
        self.threads = deque()
        _window_size    = eval(parse_config.get('significance','window_size'))
        _cycle          = eval(parse_config.get('significance','cycle'))
        _average        = eval(parse_config.get('significance','average'))
        
        fast_signi.SignificanceScorer.set_window_size(_window_size,_cycle,_average)
        
        _start_time = parse_config.get('detection','start_time')
        _end_time   = parse_config.get('detection','end_time')
        self.processor = signi_processor.SigniProcessor()
        
        self.start_time = datetime.strptime(_start_time, '%Y-%m-%d %H:%M:%S')
        self.end_time = datetime.strptime(_end_time, '%Y-%m-%d %H:%M:%S')
        
    def process(self,sig_instance, sig_list=None):
        _t, _count, _ewma, _ewmvar, _sig, _keywords= sig_instance

        if _t < self.start_time or _t > self.end_time:
            return 0.
        if eval(parse_config.get('output', 'debug_info')):
            if sig_list:  # for debugging
                print('-----------------------')
                for sig_ in sig_list:
                    print(('__sig__', sig_))
                print('-----------------------')
        create_new = True

        for thread in self.threads:
            if thread.add_to_thread(sig_instance):
                create_new = False
                break

        if create_new:
            thread = Slice()
            thread.new_thread(sig_instance)

            self.threads.append(thread)

            return _sig

        return 0.
    def process_dual_w(self,sig_instances, ptweet=None):
        _keywords=[]
        if len(sig_instances)<=0:
            return None
        for sig_instance in sig_instances:
            _keywords.append(sig_instance[-4])
        _t, _count, _ewma, _ewmvar, _sig,_token, _tid, _ratio ,_freq= sorted(sig_instances,key=lambda x:x[-2],reverse=True)[0]
        sig_instance = (_t, _count, _ewma, _ewmvar, _sig ,_keywords, _tid, _ratio,_freq)
        if _t < self.start_time or _t > self.end_time:
            return None
        
        create_new = True

        for thread in self.threads:
            if thread.add_to_thread_dual_w(sig_instance,ptweet):
                create_new = False
                break

        if create_new:
            while len(self.threads) > 0:
                first_thread = self.threads[0]
                if _t- first_thread.start > timedelta(minutes=20):
                    out = self.threads.popleft()
#                     logger.info(msg)
                    r.rpush('task:delay',out.hang)
                else:
                    break            
            thread = Slice()
            
            thread.new_thread_dual_w(sig_instance)

            self.threads.append(thread)

            return sig_instance#keywords：上一个thread的keywords
        
        return None
    def __next__(self):
        ptweet = next(self.stream)
        self.count+=1
        # if self.count%10==0:
        #     print(self.count)
        if ptweet is stream.End_Of_Stream:
            return stream.End_Of_Stream

        if ptweet is None:
            return None,None
#         sig_instance, sig_list = self.processor.process(ptweet) #signi
        sig_instance, sig_list = self.processor.process_dual_window(ptweet) #dual-windows process
        if sig_instance is not None:
            sig_instance = self.process_dual_w(sig_instance, ptweet)
            #_t, _count, _ewma, _ewmvar, _sig ,_keywords, _tid, _ratio,_freq = sig_instance
            if eval(parse_config.get('output', 'debug_info')):
                print(sig_instance)
            return ptweet, sig_instance

        return ptweet, None
class wapperDetectionComponent(DetectionComponent):
    def __init__(self,_stream,observed_list):
        super(wapperDetectionComponent,self).__init__(_stream)
        self.processor.set_observed(observed_list)
