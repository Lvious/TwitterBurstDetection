from parse_config import config as parse_config
import fast_signi
import stemmer
import hashlib
from collections import deque
from functools import reduce
from datetime import datetime,timedelta
import pdb
import math
# pdb.set_trace()

_SIGNI_THRESHOLD = eval(parse_config.get('detection', 'detection_threshold'))

_SIGNI_TYPE=parse_config.get('detection', 'detection_signi_type')

_BIT_COUNT = eval(parse_config.get('detection', 'bit_count'))

_ACTIVE_WINDOW_SIZE = eval(parse_config.get('detection', 'delay_window_size'))


# hash_available=[hashlib.md5, hashlib.sha1, hashlib.sha224, hashlib.sha256, hashlib.sha384,hashlib.sha512]

hash_available = [hashlib.md5,hashlib.sha1, hashlib.sha224, hashlib.sha256]

def get_hash(hash_function, x: str):
    """Returns a given string's hash value, obtained by the given hashlib instance."""
    hash_function.update(x.encode())
    return int.from_bytes(hash_function.digest(), byteorder="big")


class Container():
    _THRESHOLD_FOR_CLEANING =  eval(parse_config.get('detection','threshold_for_cleaning'))
    _CAPACITY_FOR_CLEANING  = eval(parse_config.get('detection', 'capacity_for_cleaning'))
    def __init__(self):
        self.container={}
    def get(self,_id,_timestamp):
        sig_scores = []
        codes = [get_hash(h(), repr(_id)) % 2 ** _BIT_COUNT for h in hash_available]
        record = []
        for code in codes:
            if code in self.container:
                sig_scores.append(self.container[code])
                record.append(code)
            else:
                if _SIGNI_TYPE=='s':
                    sig_score = fast_signi.SignificanceScorer()
                    self.container[code] = sig_score
                    sig_scores.append(self.container[code])
                    record.append(code)
        return sig_scores,record
class SigniProcessor:
    def __init__(self):
        self.sig_scorers = Container()
        self.timestamp = None
        self.active_sig={}
        self.count=0
    def set_observed(self,observed_list):
        self.observed=observed_list        
    def process(self,_ptweet):
        self.timestamp = _ptweet.timestamp
        _tokens = _ptweet.tokens
        tokens = [stemmer.stem(x) for x in _tokens]
        if len(tokens)<3:
            return None,None
        unique_words = set(tokens)
        unique_word_pairs = set()

        for i in unique_words:
            for j in unique_words - {i}:
                # To us [a, b] = [b, a], and sorting gives us a distinct representation.
                unique_word_pairs.add(tuple(sorted([i, j])))
                
        max_sig = 0
        max_sig_instance = None
        sig_list=list()
        pp = {k:'' for k in self.observed}
        for token in unique_word_pairs:
            if _SIGNI_TYPE=='s':
               
                min_instance = []
                scores,codes = self.sig_scorers.get(token, self.timestamp)
                for x in scores:
                    min_instance.append(x.observe(int(self.timestamp), 1.0))
                count, ewma, ewmavar, sig = min(min_instance,key=lambda x:x[1])
                
                for o in self.observed:
                    is_observed = False
                    if o in token:
                        is_observed = True 
                    if is_observed:
                        pp[o]=[_ptweet.datetime().strftime("%Y-%m-%d %H:%M:%S"),count, ewma, ewmavar, sig, token,_ptweet.tid]
                        print('\t'.join([str(_) for _ in pp[o]]))
#                 count, ewma, ewmavar, sig = min([x.observe(int(self.timestamp), 1.0) for x in self.sig_scorers.get(token, self.timestamp)],key=lambda x:x[1])
                if sig > max_sig and ewma>0:
                    max_sig = sig
                    max_sig_instance = _ptweet.datetime(), count, ewma, ewmavar, sig, token
                if sig > _SIGNI_THRESHOLD and ewma>0:
                    sig_list.append((_ptweet.datetime(), count, ewma, ewmavar, sig, token))
#         for m,n in pp.items():
#             print(m,n)
        if max_sig>_SIGNI_THRESHOLD:
#             print(max_sig_instance)
            return max_sig_instance,sig_list
        return None,None
    def cal_freq(self,window_pre,window_post):
        _pre  = []
        _post = []
        if len(window_pre)>1:
            for k in range(1,len(window_pre[1:])+1):
                if window_pre[k]-window_pre[k-1]==0:
                    _pre.append(0.1)
                else:
                    _pre.append(window_pre[k]-window_pre[k-1])
        if len(window_post)>1:
            for k in range(1,len(window_post[1:])+1):
                if window_post[k]-window_post[k-1]==0:
                    _post.append(0.1)
                else:
                    _post.append(window_post[k]-window_post[k-1])
        if sum(_post)==0 or sum(_pre) ==0:
            return 1
        return math.e**(-1*len(_pre)/sum(_pre)-(-1*len(_post)/sum(_post)))
    def process_dual_window(self,_ptweet):
#         if self.count%1000==0:
#             print(self.count)
        self.count+=1
        self.timestamp = _ptweet.timestamp
        _tokens = _ptweet.tokens
        tokens = [stemmer.stem(x) for x in _tokens]
        if len(tokens)<3:
            return None,None
        unique_words = set(tokens)
        unique_word_pairs = set()

        for i in unique_words:
            for j in unique_words - {i}:
                # To us [a, b] = [b, a], and sorting gives us a distinct representation.
                unique_word_pairs.add(tuple(sorted([i, j])))
                
        max_sig = 0
        max_sig_instance = None
        sig_list=list()
        
        for token in unique_word_pairs:
            if _SIGNI_TYPE=='s':
                min_instance = []
                for o in self.observed:
                    is_observed = False
                    if o in token:
                        is_observed = True
                windows_size = _ACTIVE_WINDOW_SIZE
                now = datetime.fromtimestamp(int(self.timestamp))
                interval = int(now.minute/windows_size)
                
                if interval==0:
                    delay_end = datetime(now.year,now.month,now.day,now.hour,windows_size*interval,0)-timedelta(minutes=1*windows_size) 
                else:
                    delay_end = datetime(now.year,now.month,now.day,now.hour,windows_size*interval,0)
                    
                delay_start = delay_end-timedelta(minutes=1*windows_size)
                
#                 delay_scores,delay_codes = self.delay_sig_scores.get(token,delay_start,delay_end)
                
#                 min_instance = []
#                 for x in delay_scores['sig_score']:
#                     min_instance.append(x.observe(int(self.timestamp), 1.0))
#                 count_, ewma_, ewmavar_, sig_ = min(min_instance,key=lambda x:x[1])
                
                scores,codes = self.sig_scorers.get(token, self.timestamp)
                
                for x,code in zip(scores,codes):
                    if code==0:
                        continue
                    min_instance.append((x.observe(int(self.timestamp),1.0),code))
                instance,code = min(min_instance,key=lambda x:x[1])
                count, ewma, ewmavar, sig = instance
                if code not in self.active_sig.keys():
                    self.active_sig[code]=deque([])
                self.active_sig[code].append((self.timestamp,count, ewma, ewmavar, sig))
                while len(self.active_sig[code]) > 0:
                    _ = self.active_sig[code][0]
                    if int(_[0]) < int(delay_start.timestamp()):
                        self.active_sig[code].popleft()
                    else:
                        break
                        
#                 last_window = self.timestamp,count, ewma, ewmavar, sig
                _sig = sig
                freq_score = 1
                if len(self.active_sig[code]) > 10 and int(self.active_sig[code][10][0])<=(int(self.timestamp)-20*60):
                    #取中值
                    _ = list(self.active_sig[code])
                    _sig = 0
                    _count=0
                    s_sum = 0                    
                    for _s in _[:7]:# 历史窗口，取历史记录水平值
                        s_sum+=_s[-1]
                        if _s[-3]>0:
                            _sig+=_s[-1]
                            if _count<7:
                                _count+=1
                            else:
                                break
                    if _count==0:
                        _sig = s_sum/7
                    else:
                        _sig = _sig/_count
                    
                    _count=0
                    s_sum = 0
                    for _s in _[-3:]: # 当前窗口，取最近三个记录内的均值
                        s_sum+=_s[-1]
                        if _s[-3]>0:
                            sig+=_s[-1]
                            if _count<3:
                                _count+=1
                            else:
                                break
                    if _count==0:
                        sig = s_sum/3
                    else:
                        sig = sig/_count
                    
                    _pre = [int(i[0]) for i in _[:7]]
                    _post = [int(i[0]) for i in _[-3:]]
                    freq_score = self.cal_freq(_pre,_post)

                if freq_score>1.05 and sig>2.0 and ewma>0:
                    sig_list.append((_ptweet.datetime(), count, ewma, ewmavar, sig, token, _ptweet.tid,sig/(_sig+0.001),freq_score))       
                elif sig>_SIGNI_THRESHOLD/2 and sig/(_sig+0.001)>1.7 and ewma>0:
                    sig_list.append((_ptweet.datetime(), count, ewma, ewmavar, sig, token, _ptweet.tid,sig/(_sig+0.001),freq_score))
                if sig > max_sig and ewma>0:
                    max_sig = sig
                    max_sig_instance = _ptweet.datetime(), count, ewma, ewmavar, sig, token
        if len(sig_list)>0:
            return sig_list,sig_list
        else:
            return None,None
