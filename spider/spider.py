import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
from tweepy.utils import import_simplejson
import staticconf
json = import_simplejson()
import redis

r= redis.Redis(host="54.161.160.206",port="7379")

CONFIG_FILE = 'config.yaml'

staticconf.YamlConfiguration(CONFIG_FILE)
auth = OAuthHandler(
    staticconf.read_string('twitter.consumer_key'),
    staticconf.read_string('twitter.consumer_secret'),
)
auth.set_access_token(
    staticconf.read_string('twitter.access_token'),
    staticconf.read_string('twitter.access_token_secret'),
)

class MyStreamListener(tweepy.StreamListener):
    def on_data(self, data):
        """Routes the raw stream data to the appropriate method."""
        self.on_status(data.strip())
        return True
    def on_status(self, status):
#	print(status)
        r.rpush('task:tbd',status)
    def on_error(self, status):
        """Prints any error to the console but does not halt the stream."""
        print('ON ERROR:', status)

    def on_limit(self, track):
        """Prints any limit notice to the console but doesn't halt.

        Limit notices indicate that additional tweets matched a filter,
        however they were above an artificial limit placed on the stream
        and were not sent. The value of 'track' indicates how many tweets
        in total matched the filter but were not sent since the stream
        was opened.
        """
        print('ON LIMIT:', track)

def main():
    myStreamListener = MyStreamListener()

    stream = Stream(auth,listener=myStreamListener)

    stream.sample(languages=['en'])
if __name__=='__main__':
    main()
