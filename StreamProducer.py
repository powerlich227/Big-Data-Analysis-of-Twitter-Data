from kafka import KafkaProducer
import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener

# TWITTER API CONFIGURATIONS
consumer_key = "BhtS6SO0tvGVJMKu38Ms0KcgN"
consumer_secret = "fWvrqHWPQMto4iTHbfLFf8yGxgKPpJPwyuJKhqKs7H5vNRjxdJ"
access_token = "1143702378-9wNHPNeZWEKRs3kWyq7gXtMw0jyOEvOC7dMus19"
access_secret = "1t8KPC10npD0moGPXcwaQi0I8355nGThvKYBdZVQNsOo1"

# TWITTER API AUTH
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)

api = tweepy.API(auth)

topic = "HW3"


# Twitter Stream Listener
class KafkaPushListener(StreamListener):
    def __init__(self):
        # localhost:9092 = Default Zookeeper Producer Host and Port Adresses
        self.producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

    # Get Producer that has topic name is Twitter
    # self.producer = self.client.topics[bytes("twitter")].get_producer()

    def on_data(self, data):
        # Producer produces data for consumer
        # Data comes from Twitter
        self.producer.send(topic, data.encode('utf-8'))
        print(data)
        return True

    def on_error(self, status):
        print(status)
        return True


if __name__ == "__main__":
    # Twitter Stream Config
    twitter_stream = Stream(auth, KafkaPushListener())

    # Produce Data that has trump and coronavirus hashtag (Tweets)
    twitter_stream.filter(track=['#trump', '#coronavirus'])
