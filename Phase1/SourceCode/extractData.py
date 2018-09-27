import os
from dotenv import load_dotenv
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy import API
from tweepy import Cursor
import simplejson as json
import twitter_credentials

load_dotenv('D:\\Neeraj\\Big Data Management\\.env')

class twitterValidation():

    def __init__(self):
        print("check")
        self.accessToken=os.environ.get("ACCESS_TOKEN")
        self.accessTokenSecret=os.environ.get("ACCESS_TOKEN_SECRET")
        self.consumerKey=os.environ.get("CONSUMER_KEY")
        self.consumerSecret=os.environ.get("CONSUMER_SECRET")
    
    def getTweets(self,fetched_tweets_filename):
        listener=StdOutListener(fetched_tweets_filename)
        print("hello")
        auth = OAuthHandler(self.consumerKey, self.consumerSecret)
        auth.set_access_token(self.accessToken, self.accessTokenSecret)
        stream = Stream(auth, listener)
        print("no issue")
        stream.filter(track='food')



class TwitterAuthenticator():

    def __init__(self):
        
        self.accessToken=os.environ.get("ACCESS_TOKEN")
        self.accessTokenSecret=os.environ.get("ACCESS_TOKEN_SECRET")
        self.consumerKey=os.environ.get("CONSUMER_KEY")
        self.consumerSecret=os.environ.get("CONSUMER_SECRET")
    
    def authenticate(self):
        auth = OAuthHandler(self.consumerKey, self.consumerSecret)
        auth.set_access_token(self.accessToken, self.accessTokenSecret)
        return auth

class twitterValidationGetUser():
    
    def __init__(self,user=None):
        self.auth = TwitterAuthenticator().authenticate()
        self.twitter_client = API(self.auth)
        self.twitter_user = user

    def getUserTweets(self,num_of_tweets):
        tweets = []
        for tweet in Cursor(self.twitter_client.user_timeline,q='#', id=self.twitter_user).items(num_of_tweets):
            tweets.append(tweet)
        return tweets
    
class StdOutListener(StreamListener):

    def __init__(self, filename):
        self.filename = filename
        print("std")

    def on_data(self, data):
        with open(self.filename, 'a') as tf:
            tf.write(data)
        return True
        
    def on_error(self, status):
        print("fail")
        print(status)
        return True

if __name__ == '__main__':

    # getdata=twitterValidation()
    # getdata.getTweets('extractTweets.json')

    users =['taylorswift13',
    'TheEllenShow',
    'Cristiano',
    'YouTube',
    'jtimberlake',
    'KimKardashian']

    for y in users:
        twitter_user = twitterValidationGetUser(y)
        data=twitter_user.getUserTweets(10000)
        print(type(data))
        with open('tweetsExtraction.json', "a") as myfile:
            for x in data:
                myfile.write('%s\n' % x)
        # print(data))
