import stream
import twokenize as tokenize
from stopwords import words as stopwords
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
        _tokens = tokenize.tokenizeRawTweetText(_text)
        _tokens = list(set(filter(lambda x:not x is "" and x not in stopwords and x not in tokenize.e_punc and not x.startswith('http'),[re.sub(r'[^A-Za-z0-9\':/.&$|@%\\]','',tokenize.deRepeatWords(i.lower())) for i in _tokens])))
        return stream.PreprocessedTweetItem(_t,_tid,_tokens)
        