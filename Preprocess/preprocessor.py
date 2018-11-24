from Stream import stream
from Preprocess import twokenize as tokenize
from Preprocess.stopwords import words as stopwords
import re
import nltk
from nltk.stem import WordNetLemmatizer
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
        tags = set(['NN', 'NNS', 'NNP', 'NNPS','VB', 'VBD', 'VBG', 'VBN', 'VBP', 'VBZ','WP','PRP','PRP$'])
        _tokens = self.tokens(_text)
        pos_tokens = nltk.pos_tag(_tokens)
        _tokens = [w for w,pos in pos_tokens if pos in tags]
        lemmatizer = WordNetLemmatizer()
        _tokens = [lemmatizer.lemmatize(w) for w in _tokens]
        return stream.PreprocessedTweetItem(_t, _tid, _tokens,_text)
    def tokens(self,text):
        _tokens = tokenize.tokenizeRawTweetText(text)
        _tokens = list(set(filter(lambda x:not x is ""
                                           and x not in stopwords
                                           and x not in tokenize.e_punc
                                           and not x.startswith('http')
                                           and not x.startswith('@')
                                           and not x.startswith('#')
                                  # ,_tokens)))
        ,[re.sub(r'[^A-Za-z0-9\':/.&$|@\\]', '', tokenize.deRepeatWords(i.lower())) for i in _tokens])))
        return _tokens

def main():
    import Stream.tweet_stream as tweet_stream
    # ts = tweet_stream.tweetStreamFromLocalCSV("D:/Datasets/temp/ts_01.json")
    ts = tweet_stream.tweetStreamFromRedisSimple("tweets")
    _p = Preprocessor(ts)
    next(_p)


if __name__ == '__main__':
    import sys
    import line_profiler
    import Stream.tweet_stream as tweet_stream
    ts = tweet_stream.tweetStreamFromLocalCSV("D:/Datasets/temp/ts_01.json")
    # ts = tweet_stream.tweetStreamFromRedisSimple("tweets")

    _p = Preprocessor(ts)
    next(_p)
    next(_p)
    next(_p)
    next(_p)
    next(_p)
    next(_p)
    t =_p.stream.__next__().tweet
    profile = line_profiler.LineProfiler(_p.__next__)  # 把函数传递到性能分析器
    profile.enable()  # 开始分析
    _p.__next__()
    profile.disable()  # 停止分析
    profile.print_stats(sys.stdout)  # 打印出性能分析结果