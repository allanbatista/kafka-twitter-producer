import os

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from confluent_kafka import Producer

access_token = os.environ.get('TWITTER_ACCESS_TOKEN')
access_token_secret = os.environ.get('TWITTER_ACCESS_TOKEN_SECRET')
consumer_key = os.environ.get('TWITTER_CONSUMER_KEY')
consumer_secret = os.environ.get('TWITTER_CONSUMER_SECRET')
filter_track = os.environ.get('TWITTER_FILTER_TRACK') or "a,e,i,o,u"
filter_languages = os.environ.get('TWITTER_FILTER_LANGUAGES') or "pt"

topic_name = os.environ['KAFKA_TOPIC_NAME']

producer = Producer({
    'bootstrap.servers': os.environ['KAFKA_BOOTSTRAP_SERVERS'],
    'queue.buffering.max.ms': 1000
})


class Listener(StreamListener):
    def on_data(self, data):
        producer.produce(topic_name, data.strip().encode('utf-8'))
        return True

    def on_error(self, status):
        print(status)


auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
stream = Stream(auth, Listener())
stream.filter(track=filter_track.split(","), languages=filter_languages.split(","))