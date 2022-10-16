import boto3
from producertwitterstream.producer_twitter import TweetProducer
import multiprocessing
import logging
from time import sleep
import json
import pythonjsonlogger
import logging.config
import sys
from queue import Empty

class Formatter():
    def __init__(self) -> None:
        pass

class KinesisFirehoseDeliveryJsonStreamHandler():
    """
    // Assumption: Data is json data. To make more generic can implement a Formatter class.
    This class encapsulates the logic to handle the streams and put records to kinesis.
    We can pass multiple producers to the instance and the handler will upload the data to kinesis.
    """

    def __init__(self):
       # By default, logging.StreamHandler uses sys.stderr if stream parameter is not specified
    
       self.__firehose = None
       self.__stream_buffer = []
       self.producers = {}          # holds key: value (DICT)  .. streamname: Producer
       self.stream_names = []
       self.BATCH_SIZE = 1000

       try:
           self.__firehose = boto3.client('firehose', region_name='us-east-1')
       except Exception:
           raise RuntimeError('Firehose client initialization failed.')
       
    def register_producer_stream(self, producer, stream_name):
        self.stream_names.append(stream_name)
        self.producers[stream_name] = producer
        return True
    
    def begin_streams(self):
        try:
            for producer in self.producers.values():
                # The below lines of code will start the stream on each producer
                producer.connect_source()
                producer.start_stream()
        except:
            raise RuntimeError("Failed to initialize one of the producers")
    
    def format(self, record: dict) -> str:
        return json.dumps(record) + "\n"
    
    def run(self):
        # start all the producers
        self.begin_streams()
        while True:
            for stream in self.stream_names:
                self.publish_stream(stream)

    def publish_stream(self, stream_name):
       try:
           # some function to format the message we want to send
           
           stream_producer = self.producers[stream_name]
           for i in range(self.BATCH_SIZE):
               # read data from producer
               try:
                   record = stream_producer.read1()
               except Empty:
                   break
               # format the record
               msg = self.format(record)
               self.__stream_buffer.append({
                    'Data': msg.encode(encoding="UTF-8", errors="strict")
                             })
               
            # Once we have the tweets, publish them
           sleep(30)
           if self.__firehose and self.__stream_buffer:
               print(f'Sending {len(self.__stream_buffer)} records')
               self.__firehose.put_record_batch(
                   DeliveryStreamName = stream_name,
                   Records = self.__stream_buffer
               )
               # clear the list for new stream
               self.__stream_buffer.clear()
           
       except Exception:
           raise RuntimeError(f"Error in putting the data to Kinesis, for streamname: {stream_name}")

    
if __name__ == "__main__":
    # logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    # logging.config.dictConfig(config)
    # logger = logging.getLogger(__name__)
    
    tweet_producer = TweetProducer()
    
    handler = KinesisFirehoseDeliveryJsonStreamHandler()
    handler.register_producer_stream(tweet_producer, "test")
    handler.run()
    
    
 