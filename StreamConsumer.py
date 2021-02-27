from kafka import KafkaConsumer
import json
from elasticsearch import Elasticsearch
from textblob import TextBlob
from nltk.sentiment.vader import SentimentIntensityAnalyzer

es = Elasticsearch()


def main():
    '''
    main function initiates a kafka consumer, initialize the tweetdata database.
    Consumer consumes tweets from producer extracts features, cleanses the tweet text,
    calculates sentiments and loads the data into postgres database
    '''
    # set-up a Kafka consumer
    consumer = KafkaConsumer("HW3")
    sentiment = None
    hash_tag = None
    analyzer = SentimentIntensityAnalyzer()

    for msg in consumer:
        # output = []
        dict_data = json.loads(msg.value)
        # print(dict_data)
        tweet = dict_data.get("text")
        print(tweet)

        score = analyzer.polarity_scores(tweet)
        print(score)

        if score["compound"] > 0:
            sentiment = "positive"
        if score["compound"] < 0:
            sentiment = "negative"
        if score["compound"] == 0:
            sentiment = "neutral"
        if "#trump" in tweet and "#coronavirus" not in tweet:
            hash_tag = "#trump"
        if "#coronavirus" in tweet and "#trump" not in tweet:
            hash_tag = "#coronavirus"
        if "#trump" in tweet and "#coronavirus" in tweet:
            hash_tag = "#trump&#coronavirus"

        # add text and sentiment info to elasticsearch
        es.index(index="demo",
                 doc_type="tweet_sentiment",
                 body={"author": dict_data["user"]["screen_name"],
                       "date": dict_data["created_at"],
                       "message": dict_data["text"],
                       "sentiment": sentiment,
                       "hashtag": hash_tag})
        print('\n')


if __name__ == "__main__":
    main()
