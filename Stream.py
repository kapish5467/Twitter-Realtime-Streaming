import json 
import requests
import sys
import tweepy
from datetime import datetime
import re
from textblob import TextBlob
from elasticsearch import Elasticsearch
from config import *  #import twitter keys and tokens 


def filter_tweet(tweet):
    emoji_pattern = re.compile("["
                           u"\U0001F600-\U0001F64F"  # emoticons
                           u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                           u"\U0001F680-\U0001F6FF"  # transport & map symbols
                           u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                           u"\U00002702-\U000027B0"
                           u"\U000024C2-\U0001F251"
                           "]+", flags=re.UNICODE)
    tweet = emoji_pattern.sub(r'', tweet) 
    tweet = tweet.replace("RT", "")
    tweet= ' '.join(re.sub("([^a-zA-Z0-9 .#])|(@\S*[\s, .])|(\w+:\/\/\S+)|http\S+", " ", tweet).split())
    return tweet

def get_geolatlong(loc):
    #print(loc)
    if loc is not  None:
        par = {'q': loc,'key':location_API_key}
        r = requests.get('https://api.opencagedata.com/geocode/v1/json', params=par)
        output = r.json()['results']
        print("this is geoloc func")
        if(len(output)>0):
            coord=output[0]['annotations']['OSM']['url'].split('/')
            #print(coord)
            loc=coord[-2]+","+coord[-1]
            return loc
        else:
            return None
    
def get_sentiment(tweet_txt):
    tweet= TextBlob(tweet_txt)
    print("this is get_sentiment")
    #print(tweet.sentiment.polarity)
    polarity=tweet.sentiment.polarity
    subjectivity= tweet.sentiment.subjectivity
    if(tweet.sentiment.polarity<0):
        sentiment="negative"
    elif(tweet.sentiment.polarity==0):
        sentiment="neutral"
    else:
        sentiment="positive"
    return sentiment,polarity,subjectivity

def create_ES():
    es = Elasticsearch(['http://localhost:9200/'], verify_certs=True)
    print("index created")
    request_body = {
	    'mappings':
            {
	                'properties': {
	                'tweet': { 'type': 'text'},
	                'location': { 'type': 'geo_point'},
	                'timestamp': {'type': 'text'},
	                'sentiment': {'type': 'text'},
	                'polarity': {'type': 'keyword'},
                    'subjectivity': {'type': 'keyword'},
	            }}
	}
    es.indices.create(index = 'twitter', body = request_body)
    

def send_ES(item):
    es = Elasticsearch(['http://localhost:9200/'], verify_certs=True)

    if not es.ping():
        raise ValueError("Connection failed")
    #es.indices.delete(index='sentiments', ignore=[400, 404])
    try:
        print("reached ES")
        list_sentiment=get_sentiment(item['text'])
        print(list_sentiment)
        es.index(index='twitter', 
                 body={
                        'tweet': item['text'],
                        'location':item['location'],
                        'timestamp':item['timestamp'],
                        'sentiment':list_sentiment[0],
                        'polarity':list_sentiment[1],
                        'subjectivity':list_sentiment[2]
                        })
    except:
        print ("Error on elasticsearch" , sys.exc_info()[0])

count =1
class TweetStreamListener(tweepy.StreamListener):
    def on_data(self,status):
        print("this is on_status")
        doc=json.loads(status)
        timestamp=doc['created_at']
 
        #print(get_geolatlong(doc['user']['location']))
        
        if(get_geolatlong(doc['user']['location']) is not None):
            docs = {'text':filter_tweet(doc['text']),'location':get_geolatlong(doc['user']['location']),'timestamp':datetime.strftime(datetime.strptime(timestamp,'%a %b %d %H:%M:%S +0000 %Y'), '%Y-%m-%d %H:%M:%S')}
            print(docs)
            send_ES(docs)
            global count
            count=count+1
            print(count)
            if(count>200):
                stream.disconnect()
        
       
    def on_error(self, status_code):
       if status_code== 420:
           return False
       else:
           return True
       

if __name__ == "__main__":
    count=1
    listener = TweetStreamListener(count)
    auth= tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)
    stream = tweepy.Stream(auth,listener)
    print("just before filter")
    count = stream.filter(track=['biden'])
    print("just after filter")
 
    


    