import os.path, sys
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))

import multiprocessing
from typing import overload
from AbsProducer import Producer
import tweepy as tw
import logging
from keys import *
import pandas as pd
import re
from queue import Queue as Q
import ast
import time

class TClient(tw.StreamingClient):
    def __init__(self, *args, **kwargs):
        self._process_queue = kwargs.pop('processQueue')
        self._i = 0
        super(TClient, self).__init__(*args, **kwargs)
    
    def on_connect(self):
        print("Connected")
        
    def on_tweet(self, tweet):
        if type(tweet.referenced_tweets) is list:
            rtype = 'Original' if not tweet.referenced_tweets[0] else tweet.referenced_tweets[0].type
        else:
            rtype = 'Original' if not tweet.referenced_tweets else tweet.referenced_tweets.type
        pattern = re.compile('[#|\$]\w+')
        res = {
            'text': tweet.text,
            'tweet_type': rtype,
            'sensitive': tweet.possibly_sensitive,
            'created_at': time.asctime(tweet.created_at.timetuple()),
            'hashcashtags': [i.upper() for i in pattern.findall(tweet.text)]
        }
        
        self._i += 1
        logging.info(f"Adding new tweet to queue: #{self._i}")
        self._process_queue.put(res)
        
class TweetProducer(Producer):
    def __init__(self):
        self.client = None
        self.stream_process = None
        self.process_queue = multiprocessing.Queue()
        
        # stream specific variables below
        self.logger = logging.getLogger("TWEET PRODUCER")
        self.logger.info("Reading list of stocks to pull")
        df = pd.read_csv(os.path.join(os.path.dirname(os.path.realpath(__file__)), "sp500.csv")).iloc[:100]
        print(df.head(), "\n")
        self.logger.info("Creating symbol list to search on twitter")
        self.symbols_list = df.Symbol.map(lambda x: '$'+x).to_list()\
                        + df.Symbol.map(lambda x: '#'+x).to_list()
        symbols = set(self.symbols_list)
        print(self.symbols_list[:5], " ...")
    
    def connect_source(self):    
        # Creating the client
        self.logger.info("Trying to create connection with Twitter API")
        self.client = TClient(BEARER_TOKEN, processQueue=self.process_queue)
            
        # create rules to pull the data
        q = Q()
        g = 30
        # divide the symbols into groups of 30
        for i in range(0, len(self.symbols_list), g):
            q.put((i,i+g))
        
        # Delete rules from previous runs
        try:
            existing_rules = [i.id for i in self.client.get_rules().data]
            self.client.delete_rules(existing_rules)
        except:
            pass
        
        # Set a base pattern for search
        #base_pat = "(buy OR sell OR win OR lose OR markets OR trend OR index OR bullish OR bearish OR stock OR market OR investing OR investment OR economy OR buy OR opinion)"
        base_pat = "(has:hashtags OR has:cashtags) lang:en -algo -software -alerts"
        
        # Add the symbols to create multiple filter rules for the incoming stream
        while not q.empty():
            a, b = q.get()
            symbols_subset = self.symbols_list[a:b]
            pat = f"-is:retweet ({' OR '.join(symbols_subset)}) {base_pat}"
            
            # add rules to client
            self.client.add_rules(tw.StreamRule(value = pat, tag = str(symbols_subset)))
        
        self.logger.info("All rules added to client, client is ready to stream")  
        self.logger.debug(self.client.get_rules())
    
    def start_stream(self):
        tweet_fields = ['created_at', 'text', \
            'geo', 'lang', 'referenced_tweets', 'organic_metrics', 'public_metrics', 'possibly_sensitive', 'in_reply_to_user_id']
        self.stream_process = multiprocessing.Process(target=self.client.filter, kwargs={'tweet_fields': tweet_fields})
        self.stream_process.daemon=True
        self.stream_process.start()
        self.logger.info("Streaming has been started")
        return True
        
    def read1(self):
        if self.stream_process.is_alive():
            return self.process_queue.get()
        else:
            raise RuntimeError("Stream is no longer alive")
              
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    producer = TweetProducer()
    producer.connect_source()
    producer.start_stream()
    
    for i in range(20):
        print(producer.read1())
        time.sleep(2)
        
