from datetime import datetime,timedelta
import calendar

class stream:
    
    def __init__(self):
        pass
    def __next__(self):
        pass
class EndOfSteam(stream):
    
    def __init__(self):
        pass
    def __next__(self):
        pass
class ItemStream(stream):
    
    def __init__(self,timestamp,uid,tid,tweet):
        self.timestamp = timestamp
        self.uid = uid
        self.tid = tid
        self.tweet = tweet
class RawTweetItem(stream):
    def __init__(self,timestamp,tid,tweet):
        self.timestamp = timestamp
        self.tid = tid
        self.tweet = tweet
    def datetime(self):
        return datetime.utcfromtimestamp(int(self.timestamp))
class PreprocessedTweetItem:

    def __init__(self, _t, _tid, _tokens):
        if isinstance(_t, datetime):
            self.timestamp = calendar.timegm(_t.timetuple())
        else:
            self.timestamp = _t
        self.tokens = _tokens
        self.tid = _tid

    def datetime(self):
        return datetime.utcfromtimestamp(int(self.timestamp))
    
End_Of_Stream = EndOfSteam()