import os, time

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

kafka_topic_name = os.environ['KAFKA_TOPIC_NAME']
kafka_queue_buffering_max_messages = int(os.environ.get('KAFKA_QUEUE_BUFFERING_MAX_MESSAGES') or 32)
kafka_queue_buffering_max_ms = int(os.environ.get('KAFKA_QUEUE_BUFFERING_MAX_MS') or 1000)


producer = Producer({
    'bootstrap.servers': os.environ['KAFKA_BOOTSTRAP_SERVERS'],
    'queue.buffering.max.messages': kafka_queue_buffering_max_messages,
    'queue.buffering.max.ms': kafka_queue_buffering_max_ms
})


class Listener(StreamListener):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.count = 0
        self.total = 0
        self.time = time.time()

    def on_data(self, data):
        producer.produce(kafka_topic_name, data.strip().encode('utf-8'))

        self.count += 1
        self.total += 1
        if (time.time() - self.time) >= 1:
            print(f"processed: {self.count} | total: {self.total}")
            self.time = time.time()
            self.count = 0

        return True

    def on_error(self, status):
        print(status)

try:
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, Listener())
    stream.filter(track=filter_track.split(","), languages=filter_languages.split(","))
finally:
    producer.flush()