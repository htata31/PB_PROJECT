from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

ACCESS_TOKEN = "1042056400560680961-AdlgRfGTiyfj6aE0nmOylj2EEtTbeK"
ACCESS_SECRET  = "Uquvk5RrlhIT4oFsMm7VNBCRe8r4rMibXJXZhPau0culW"
CONSUMER_KEY = "rFXo7y4lDLzBCa6edAbyJPXO6"
CONSUMER_SECRET = "kRgSC8luGa3J5pFu4AEE37YoAeRs9HkFmgoM5cg5Nv2ieNGUaU"

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