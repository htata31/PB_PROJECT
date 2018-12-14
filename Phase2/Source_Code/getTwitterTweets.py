from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

ACCESS_TOKEN = ""
ACCESS_SECRET  = ""
CONSUMER_KEY = ""
CONSUMER_SECRET = ""

class StdOutListener(StreamListener):

    def on_data(self, data):
        with open('extractTweetsM.json', 'a') as tf:
            tf.write(data)
        return True

    def on_error(self, status):
        print (status)

if __name__ == '__main__':
    #This handles Twitter authetification and the connection to Twitter Streaming API
    l = StdOutListener()
    auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)
    stream = Stream(auth, l)

stream.filter(track=['IPL','Cricket','Yuvraj','Dhoni','Virat','Cricbuzz','Star Sports','Cricinfo','Yahoo Cricket','ESPN','NDTV Cricket','ESPN cricinfo'])