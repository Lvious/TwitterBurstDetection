from Config.parse_config import config as parse_config
import Utils.fast_signi as fast_signi
import hashlib
from datetime import datetime
import math
import pickle
from collections import deque
import copy
import line_profiler
import sys
from Preprocess.preprocessor import Preprocessor

_SIGNI_THRESHOLD = eval(parse_config.get('detection', 'detection_threshold'))

_SIGNI_TYPE = parse_config.get('detection', 'detection_signi_type')

_BIT_COUNT = eval(parse_config.get('detection', 'bit_count'))

# _ACTIVE_WINDOW_SIZE = eval(parse_config.get('detection', 'delay_window_size'))

_THRESHOLD_FOR_CLEANING = eval(parse_config.get('detection', 'threshold_for_cleaning'))

_CAPACITY_FOR_CLEANING = eval(parse_config.get('detection', 'capacity_for_cleaning'))

_TIME_STEP = int(eval(parse_config.get('detection', 'time_step')))

_CURRENT_TIME = int(eval(parse_config.get('detection', 'current_time')))

class Hashtools:
    def __init__(self):
        self.hash_available = [hashlib.md5, hashlib.sha1, hashlib.sha224, hashlib.sha256]

    def get_hash(self, ind, x: str):
        """Returns a given string's hash value, obtained by the given hashlib instance."""
        hash_function = self.hash_available[ind]()
        hash_function.update(x.encode())
        return int.from_bytes(hash_function.digest(), byteorder="big")


#
# String<->Code<->counter
# 三者之间的转化
#
# 每个token一个滑动窗口 这样的方法不适用，会造成每个token产生一个滑动窗口，不利于数据降维
class Container():
    def __init__(self):
        self.currentTimestamp = None
        self.hashtools = Hashtools()
        self.container = [dict() for i in range(len(self.hashtools.hash_available))]
    '''根据到来的推特设定当前的container的时间'''
    def setCurrentTimestamp(self,timestamp):
        self.currentTimestamp = int(timestamp)
    def getCurrentTimestamp(self):
        return self.currentTimestamp
    '''给定token，计算指定func下的hashcode'''
    def _getC(self, ind, token):
        return self.hashtools.get_hash(ind, repr(token)) % 2 ** _BIT_COUNT
    '''计算指定container下对应的hashcode的sig_core值'''
    def _getS(self, ind, code):
        if code in self.container[ind]:
            sig_score = self.container[ind][code]
        else:
            sig_score = fast_signi.SignificanceScorer()
            self.container[ind][code] = sig_score
        return sig_score
    '''给定token，获得所有的的sig_score'''
    def getScores(self, token):
        sig_scores = list()
        for ind,_code in enumerate(self.getCodes(token)):
            sig_scores.append(self._getS(ind,_code))
        return sig_scores
    '''给定token，观察对应的值，更新对应值'''
    def observe(self,token):
        for score in self.getScores(token):
            score.observe(self.currentTimestamp,1)
    '''给定token,返回所有的hash后的hashcode'''
    def getCodes(self, token):
        codes = list()
        for ind in range(len(self.hashtools.hash_available)):
            _code = self._getC(ind, token)
            codes.append(_code)
        return codes
    '''滑动窗口，暂定将container做滑动，内存消耗便会有限，设定滑动m时间滑动一次，滑动窗口为k'''
    def slideWindow(self):
        pass
    '''按照某属性排序，返回最小score'''
    def getMinScore(self,token,attr="ewma"):
        assert attr in ["count","ewma","ewmvar"]
        minScore = None
        sortedScore = sorted(self.getScores(token),key=lambda x:getattr(x, attr))
        for x in sortedScore:
            if getattr(x,attr)==0:
                continue
            minScore = x
            break
        if minScore is None:
            minScore = sortedScore[0]
        return minScore

class Data:
    def __init__(self, timestamp, tid, count, ewma, ewmvar, sig, token,tweet):
        self.timestamp = int(timestamp)
        self.tid = tid
        self.count = count
        self.ewma = ewma
        self.ewmvar = ewmvar
        self.sig = sig
        self.token = token
        self.tweet = tweet
    def who2Avg(self, who="count"):
        if hasattr(self, who):
            return getattr(self, who)


# 滑动窗口 sliding-window
class SlideWindow:
    def __init__(self):
        self.LEN = 10
        self.windowPool = deque(maxlen=self.LEN)
        self.firstTime = None
        self.lastTime = None
        self.timeStep = _TIME_STEP
        self.currentTime = _CURRENT_TIME
    def setLEN(self, LEN):
        self.LEN = LEN
    '''获得开始时间'''
    def getStartTime(self):
        if len(self.windowPool)>1:
            return self.windowPool[0].getCurrentTimestamp()
        else:
            return None
    '''旧方法'''
    def slideOne(self, data):
        if len(self.windowPool) < self.LEN:
            self.windowPool.append(data)
        else:
            self.lastTime = self.windowPool[-1].timestamp
            # self.windowPool.append((timestamp, data))
            self.firstTime = self.windowPool[0].timestamp
            self.windowPool.pop(0)
    '''当前时间超出滑动窗口区间，则将当前的container作为一个时间切片，加入到当前时间窗口内'''
    def slideOneContainer(self,container):
        if container.getCurrentTimestamp()>self.currentTime+self.timeStep:
            # step方法有问题
            s = datetime.now().timestamp()
            self.windowPool.append(copy.deepcopy(container))#这里需要用到深拷贝，否则所有的container都是同一个引用，这里可能是比较耗时的操作！！！
            e = datetime.now().timestamp()
            #log
            #print(e-s)
            self.currentTime = self.currentTime+self.timeStep
        else:
            pass
    '''返回containers中观察值'''
    def getDataPool(self,token,type="ewma"):
        dataPool = list()
        for container in self.windowPool:
            dataPool.append(container.getMinScore(token,type))
        return dataPool
    '''瞬时冰冻数据'''
    def freeze(self):
        pickle.dump(self, open('container.pkl', 'wb'), protocol=2)

    def isFull(self):
        return True if len(self.windowPool) == 10 else False
    '''计算数据池中每个截取片段计数的平均值'''
    def mean(self):
        count =1
        avg = 0
        for data in self.dataPool:
            avg = avg + (data - avg) / count
            count+=1
        return avg

    # def mean(self, who='count'):
    #     avg = 0
    #     if len(self.dataPool) > 0:
    #         count = 1
    #         for data in self.dataPool:
    #             avg = avg + (data.who2Avg(who) - avg) / count
    #             count += 1
    #     return avg


class SigniProcessor:
    '''M1:uni-gram的container，M2:bi-gram的container'''
    def __init__(self):
        self.containerM1 = Container()
        self.containerM2 = Container()
        self.slideWindow = SlideWindow()
    '''返回word-pairs，PMI减少低置信度的word-pairs待研究'''
    def getUniquePairs(self, tweet):
        self.timestamp = tweet.timestamp
        self.tweet = tweet
        # tags = set(['NN', 'NNS', 'NNP', 'NNPS','VB', 'VBD', 'VBG', 'VBN', 'VBP', 'VBZ','WP','PRP','PRP$'])
        tokens = tweet.tokens
        unique_words = set(tokens)
        unique_word_pairs = set()
        for i in unique_words:
            for j in unique_words - {i}:
                # To us [a, b] = [b, a], and sorting gives us a distinct representation.
                unique_word_pairs.add(tuple(sorted([i, j])))
        return unique_word_pairs

    def getUniqueTokens(self, tweet):
        if len(tweet.tokens) < 3:  # 如果tokens长度小于3，则跳过处理
            return None
        tokens = tweet.tokens
        uniqueTokens = set(tokens)
        return uniqueTokens

    def calPMI(self, i, j):
        pi = self.containerM1.getScores(i)
        pj = self.containerM1.getScores(j)
        pij = self.containerM2.getScores((i, j))
        return math.log(pij / (pi * pj + 1))

    def PMI(self, tweet):
        # PMI降维
        pmi = dict()
        if not self.getUniquePairs(tweet):
            return None
        for i, j in self.getUniquePairs(tweet):
            pmi[(i,j)] = self.calPMI(i, j)
        return sorted(pmi.items(), key=lambda x: x[1], reverse=True)

    def process(self, tweet):
        if len(tweet.tokens) < 3:  # 如果tokens长度小于3，则跳过处理
            return None
        sig_list = list()
        uniquePaires = self.getUniquePairs(tweet)
        if not uniquePaires:
            return None
        preSig, postSig, freq = 0., 0., 0.
        '''这个方法还未完善'''
        # for token in self.getUniqueTokens(tweet):
        #     self.containerM1.getScores(token)
        self.containerM2.setCurrentTimestamp(tweet.timestamp)
        self.slideWindow.slideOneContainer(self.containerM2)
        '''处理wordpair'''
        for token in uniquePaires:
            if _SIGNI_TYPE == 's':
                # min_instance = list()
                # scores = self.containerM2.getScores(token)
                # for x in scores:
                #     min_instance.append(x.observe(int(self.timestamp), 1.0))
                # '''不必关注是哪个槽里的值最小，只需要最小值'''
                # count, ewma, ewmavar, sig = min(min_instance, key=lambda x: x[1])
                # data = Data(self.timestamp, count, ewma, ewmavar, sig, token)
                # slideWindow = self.containerM2.getSlideWindow(token)
                self.containerM2.observe(token)#每个tuple的执行时间大约600个时间单位
                if self.slideWindow.isFull():
                    dataPool = self.slideWindow.getDataPool(token,"count")
                    '''dataPool中最后一个score作为当前的检测值'''
                    count,ewma,ewmvar = dataPool[-1].count,dataPool[-1].ewma,dataPool[-1].ewmvar
                    preSig, postSig = self.checkSig(dataPool)
                    # frequecy = self.checkFrequency(countDataPool)
                    sig_instance = Data(tweet.timestamp, tweet.tid, count,ewma,ewmvar,postSig,token,tweet.text)
                else:
                    continue
                # if postSig > 2.0 and ewma > 0:
                #     sig_list.append(
                #         (self.tweet.datetime(), count, ewma, ewmvar, postSig,token, self.tweet.tid))
                if postSig > _SIGNI_THRESHOLD / 2 and postSig / (preSig + 0.001) > 1.7 and ewma > 0:
                    sig_list.append(sig_instance)
        if len(sig_list) > 0:
            return sig_list
        else:
            return None

    def checkSig(self, dataPool):
        splitCur = int(len(dataPool) / 2)
        mu_pre = dataPool[int(splitCur / 2)].ewma
        mu_post = dataPool[int(splitCur / 2)+splitCur].ewma
        preDataPool, postDataPool = [x.count for x in dataPool[:splitCur]], [x.count for x in dataPool[splitCur:]]
        preSig = self.t_test(preDataPool,mu_pre)
        postSig = self.t_test(postDataPool,mu_post)
        return preSig, postSig
    '''无法实现--'''
    # def checkFrequency(self, ):
    #     splitCur = int(window.LEN / 2)
    #     preWindow, postWindow = window, window
    #     preWindow.setLEN(splitCur)
    #     postWindow.setLEN(splitCur)
    #     preWindow.windowPool, postWindow.windowPool = window.windowPool[:splitCur], window.windowPool[splitCur:]
    #     preV = list()
    #     postV = list()
    #     for k in range(1, len(preWindow.windowPool)):
    #         if preWindow.windowPool[k] - preWindow.windowPool[k - 1] == 0:
    #             preV.append(0.1)
    #         else:
    #             preV.append(postWindow.windowPool[k] - preWindow.windowPool[k])
    #     for k in range(1, len(postWindow.windowPool)):
    #         if postWindow.windowPool[k] - postWindow.windowPool[k - 1] == 0:
    #             postV.append(0.1)
    #         else:
    #             postV.append(postWindow.windowPool[k] - postWindow.windowPool[k])
    #     if sum(preV) == 0 or sum(postV) == 0:
    #         return 1
    #     return math.e ** (-1 * len(preV) / sum(preV) - (-1 * len(postV) / sum(postV)))

    def t_test(self, dataPool,mu):
        meanValue = sum(dataPool)/float(len(dataPool))
        middleCur = len(dataPool)/2
        t_value = 0
        for d in dataPool:
            t_value += (d - meanValue)**2
        if t_value==0:
            sig = 0
            return sig
        sig = (meanValue - mu) / math.sqrt(t_value / (middleCur - 1))
        return sig
def main():
    import spacy
    nlp = spacy.load('en')
    sig = SigniProcessor()
    from Stream.tweet_stream import tweetStreamFromRedisSimple as ts_stream
    st = ts_stream("tweets")
    pst = Preprocessor(st)
    ptweet = next(pst)
    sig.process(ptweet)
    ptweet = next(pst)
    sig.process(ptweet)
    ptweet = next(pst)
    sig.process(ptweet)
    ptweet = next(pst)
    sig.process(ptweet)
    ptweet = next(pst)
    sig.process(ptweet)
    ptweet = next(pst)
    sig.process(ptweet)
    doc = nlp(" ".join(ptweet.tokens))
if __name__ == '__main__':
    sig = SigniProcessor()
    from Stream.tweet_stream import tweetStreamFromRedisSimple as ts_stream
    st = ts_stream("tweets")
    pst = Preprocessor(st)
    ptweet = next(pst)
    sig.process(ptweet)
    ptweet = next(pst)
    sig.process(ptweet)
    ptweet = next(pst)
    sig.process(ptweet)
    ptweet = next(pst)
    sig.process(ptweet)
    ptweet = next(pst)
    sig.process(ptweet)
    ptweet = next(pst)
    sig.process(ptweet)
    ptweet = next(pst)
    print(ptweet.tokens)
    profile = line_profiler.LineProfiler(sig.process)  # 把函数传递到性能分析器
    profile.enable()  # 开始分析
    sig.process(ptweet)
    profile.disable()  # 停止分析
    profile.print_stats(sys.stdout)  # 打印出性能分析结果