from Stream import stream
from Config.parse_config import config as parse_config
from Component import signi_processor
from datetime import datetime,timedelta
from Utils import fast_signi

_THREAD_GAP = eval(parse_config.get('detection', 'thread_gap'))


class Slice:

    def __init__(self):
        self.start = 0.0
        self.end = 0.0
        self.keywords = None
        self.sig = 0.0
        self.first_sig = 0.0
        self.thread = []
        self.first_keywords = None

    def new_thread(self, sig_instance):
        _t, _count, _ewma, _ewmvar, _sig, _keywords = sig_instance
        self.start = _t
        self.end = _t
        _kw1=_keywords[0]
        _kw2=_keywords[1]
        self.keywords = set([_kw1,_kw2])
        self.thread.append(sig_instance)
        self.first_sig = _sig
        self.sig = _sig
        self.first_keywords = _kw1+','+_kw2

    def add_to_thread(self, sig_instance):
        _t, _count, _ewma, _ewmvar, _sig, _keywords = sig_instance

        kw1, kw2 = _keywords[0],_keywords[1]

        if len(self.keywords) >= 10:
            return False

        if _t - self.end <= timedelta(minutes=1):
            if kw1 not in self.keywords and kw2 not in self.keywords:
                return False
        elif kw1 not in self.keywords or kw2 not in self.keywords:
            return False

        if _t - self.end > timedelta(minutes=_THREAD_GAP):
            return False

        if _sig > self.sig:
            self.sig = _sig

        self.end = _t
        self.thread.append(sig_instance)
        self.keywords.add(kw1)
        self.keywords.add(kw2)

        return True

class DetectionComponent(stream.stream):
    def __init__(self,_stream):
        self.stream     = _stream
        self.threads = list()        
        _window_size    = eval(parse_config.get('significance','window_size'))
        _cycle          = eval(parse_config.get('significance','cycle'))
        _average        = eval(parse_config.get('significance','average'))
        
        fast_signi.SignificanceScorer.set_window_size(_window_size, _cycle, _average)
        
        _start_time = parse_config.get('detection','start_time')
        _end_time   = parse_config.get('detection','end_time')
        self.processor = signi_processor.SigniProcessor()
        
        self.start_time = datetime.strptime(_start_time, '%Y-%m-%d %H:%M:%S')
        self.end_time = datetime.strptime(_end_time, '%Y-%m-%d %H:%M:%S')
        
    def process(self,sig_instance, sig_list=None):
        _t, _count, _ewma, _ewmvar, _sig, _keywords = sig_instance

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
    def __next__(self):
        ptweet = next(self.stream)
        if ptweet is stream.End_Of_Stream:
            return stream.End_Of_Stream

        if ptweet is None:
            return None
        sig_instance, sig_list = self.processor.process(ptweet)
#         print(sig_instance)
        if sig_instance is not None:
            output = self.process(sig_instance, sig_list)

            if eval(parse_config.get('output', 'debug_info')):
                print(sig_instance)
            return ptweet, output

        return ptweet, 0.0        