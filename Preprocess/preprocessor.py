from Stream import stream
from Preprocess import twokenize as tokenize
from Preprocess.stopwords import words as stopwords
import re

class Preprocessor:
    def __init__(self,tweet_stream):
        self.stream = tweet_stream
    def __next__(self):
        _tweet = next(self.stream)
        if _tweet is stream.End_Of_Stream:
            return stream.End_Of_Stream
        _t     = _tweet.timestamp
        _tid   = _tweet.tid
        _text  = _tweet.tweet
        _tokens = self.tokens(_text)
        return stream.PreprocessedTweetItem(_t, _tid, _tokens,_text)
    def tokens(self,text):
        _tokens = tokenize.tokenizeRawTweetText(text)
        _tokens = list(set(filter(lambda x:not x is ""
                                           and x not in stopwords
                                           and x not in tokenize.e_punc
                                           and not x.startswith('http')
                                           and not x.startswith('@')
                                           and not x.startswith('#'),
                                  [re.sub(r'[^A-Za-z0-9\':/.&$|@%\\]','',tokenize.deRepeatWords(i.lower())) for i in _tokens])))
        return _tokens

def main():
    import Stream.tweet_stream as tweet_stream
    stream = tweet_stream.tweetStreamFromRedisSimple("tweets")
    _p = Preprocessor(stream)
    next(_p)

if __name__ == '__main__':
    import sys
    import line_profiler
    profile = line_profiler.LineProfiler(main)  # 把函数传递到性能分析器
    profile.enable()  # 开始分析
    main()
    profile.disable()  # 停止分析
    profile.print_stats(sys.stdout)  # 打印出性能分析结果