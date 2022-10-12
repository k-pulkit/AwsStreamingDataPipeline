import boto3
import logging
from time import sleep
import json
import pythonjsonlogger
import logging.config


class TweetProducer(object):
    def __init__(self):
        self.text = "Created by pulkit!"
        self.id = 12312213
        self.region = "America"
        self.timestamp = 1664604088
        self.created_at = 1664604088
        self.tags = ['tag-a', 'tag-b']

    def get_tweet(self):
        return json.dumps({
                 "twitter-id": 12313,
                 "text": "tweet adsgaaf pulkit",
                 "tags": ['APPL', 'GOOGL'],
                 "timestamp" : 1664604088,
                 "created_at" : 1664604088,
                 "region" : "America"
                 })
    
    def generate_tweets(self):
        while True:
            sleep(4)
            yield self.get_tweet()
    

class KinesisFirehoseDeliveryStreamHandler(logging.StreamHandler):

   def __init__(self):
       # By default, logging.StreamHandler uses sys.stderr if stream parameter is not specified
       logging.StreamHandler.__init__(self)

       self.__firehose = None
       self.__stream_buffer = []

       try:
           self.__firehose = boto3.client('firehose')
       except Exception:
           print('Firehose client initialization failed.')

       self.__delivery_stream_name = "twitter-delivery-test"

   def emit(self, record):
       try:
           msg = self.format(record)

           if self.__firehose:
               self.__stream_buffer.append({
                   'Data': msg.encode(encoding="UTF-8", errors="strict")
               })
           else:
               stream = self.stream
               stream.write(msg)
               stream.write(self.terminator)

           self.flush()
       except Exception:
           self.handleError(record)

   def flush(self):
       self.acquire()

       try:
           if self.__firehose and self.__stream_buffer:
               self.__firehose.put_record_batch(
                   DeliveryStreamName=self.__delivery_stream_name,
                   Records=self.__stream_buffer
               )

               self.__stream_buffer.clear()
       except Exception as e:
           print("An error occurred during flush operation.")
           print(f"Exception: {e}")
           print(f"Stream buffer: {self.__stream_buffer}")
       finally:
           if self.stream and hasattr(self.stream, "flush"):
               self.stream.flush()

           self.release()
           
           
config = {
  "version": 1,
  "disable_existing_loggers": False,
  "formatters": {
      "standard": {
          "format": "%(asctime)s %(name)s %(levelname)s %(message)s",
          "datefmt": "%Y-%m-%dT%H:%M:%S%z",
      },
      "json": {
          "format": "%(asctime)s %(name)s %(levelname)s %(message)s",
          "datefmt": "%Y-%m-%dT%H:%M:%S%z",
          "class": "pythonjsonlogger.jsonlogger.JsonFormatter"
      }
  },
  "handlers": {
      "standard": {
          "class": "logging.StreamHandler",
          "formatter": "json"
      },
      "kinesis": {
          "class": "tweet-producer.KinesisFirehoseDeliveryStreamHandler",
          "formatter": "json"
      }
  },
  "loggers": {
      "": {
          "handlers": ["standard", "kinesis"],
          "level": logging.INFO
      }
  }
}
           
    
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    logging.config.dictConfig(config)
    logger = logging.getLogger(__name__)
    tweet_producer = TweetProducer()
    for tweet in tweet_producer.generate_tweets():
        if type(tweet) == str:
            logger.info(tweet)
 