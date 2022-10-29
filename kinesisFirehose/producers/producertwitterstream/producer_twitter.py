import os, os.path, sys
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__))))

import multiprocessing
from typing import overload
from AbsProducer import Producer
import tweepy as tw
import logging
from keys2 import *
import pandas as pd
import re
from queue import Queue as Q
from queue import Empty
import ast
import time
from datetime import timezone
import watchtower

class TClient(tw.StreamingClient):
    def __init__(self, *args, **kwargs):
        """
        Inititlizes the parent class, and sets up Queue where the results are pushed to be consumed by
        the main process
        """
        self._process_queue = kwargs.pop('processQueue')
        self._i = 0
        super(TClient, self).__init__(*args, **kwargs)
        
        self.__client_v1 = None      # client for the Twitter1.1 api calls
        self.__auth = None
        self.logger = logging.getLogger("TWEET PRODUCER")
    
    def on_connect(self):
        """
        Once connection is established, we start another client for Twitterv1.1 API interface
        """
        print("Connected to Twitterv2")
        
        # initialize client for Twitterv1 to enrich tweets
        self.__auth = tw.OAuth1UserHandler(API_KEY, API_SECRET_KEY, ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
        self.__client_v1 = tw.API(self.__auth)
        self.logger.info("Connection verification for Twitter1.1 " + self.__client_v1.verify_credentials().screen_name)
        
    def __helper_on_tweet(self, tweet_id):
        """
        Used to enrich the non-original tweets
        with the tweet text for the main tweet
        """
        get_twit = lambda id: self.__client_v1.get_status(id)        # type: ignore
        get_par_twit_id = lambda r: r.in_reply_to_status_id
        is_original = lambda r: r.in_reply_to_status_id is None
        
        id = get_par_twit_id(get_twit(tweet_id))   # start with parent tweet
        DEPTH = 4
        while DEPTH >=0:
            # code
            try:
                twit = get_twit(id)
            except Exception as e:
                self.logger.warn(f"Tweet not found, id: {id}")
                break
            if not is_original(twit):
                id = get_par_twit_id(twit)
            else:
                return {
                    'tweet_id': twit.id,
                    'tweet_type': 'original',
                    'text': twit.text
                }            
            DEPTH -= 1
        
        return None
        
    def on_tweet(self, tweet):
        """
        This function is called when a new Tweet is received by the stream.
        We extract all the attributes and stage them in required format for consumption later
        
        Change - 
        For the tweets that are not original, we call a helper method to get the original tweet
        This, will help to produce cash and hashtag for the tweet even if user text had none.
        """
        if type(tweet.referenced_tweets) is list:
            rtype = 'original' if not tweet.referenced_tweets[0] else tweet.referenced_tweets[0].type
        else:
            rtype = 'original' if not tweet.referenced_tweets else tweet.referenced_tweets.type
        pattern = re.compile('[#|\$](\w+)')   # Only extract the symbol not # or $
        res = {
            'tweet_id': tweet.id,
            'author_id': tweet.author_id,
            'text': tweet.text,
            'tweet_meta': {
                'created_at': tweet.created_at.replace(tzinfo=timezone.utc).timestamp(),     # convert timestamp to utc epoch
                                                                                             # datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
                'tweet_type': rtype,
                'sensitive': tweet.possibly_sensitive,
                'hashcashtags': list(set([i.upper() for i in pattern.findall(tweet.text)]))  # Make a unique list
            }                                                # contains detail of parent tweets for non-original tweets
        }
        
        if rtype != 'original':
            o = self.__helper_on_tweet(tweet.id)
            if o:
                o['hashcashtags'] = list(set([i.upper() for i in pattern.findall(o['text'])]))  # type: ignore
                res['reference_tweets'] = o
        
        self._i += 1
        print(f"Adding new tweet to queue: #{self._i}")
        self.logger.info(f"Adding new tweet to queue: #{self._i}")
        self._process_queue.put(res)
        
class TweetProducer(Producer):
    def __init__(self):
        self.client = None
        
        self.stream_process = None
        self.process_queue = multiprocessing.Queue()
        
        # stream specific variables below
        self.logger = logging.getLogger("TWEET PRODUCER")
        self.logger.setLevel(logging.INFO)
        
        # handlers for logging
        console_handler = logging.StreamHandler()
        cw_handler = watchtower.CloudWatchLogHandler(\
                log_group='pknn-twit-api-1',
                stream_name='python-script' + str(time.time()))  # type: ignore
        
        # add handlers
        self.logger.addHandler(console_handler)
        self.logger.addHandler(cw_handler)
        
        self.logger.info("Reading list of stocks to pull")
        df = pd.read_csv(os.path.join(os.path.dirname(os.path.realpath(__file__)), "sp500v2.csv")).iloc[:100]
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
            existing_rules = [i.id for i in self.client.get_rules().data]  # type: ignore
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
        self.logger.info(self.client.get_rules())
    
    def start_stream(self):
        tweet_fields = ['id', 'author_id', 'created_at', 'text', \
            'geo', 'lang', 'referenced_tweets', 'conversation_id', 'organic_metrics', 'public_metrics', 'possibly_sensitive', 'in_reply_to_user_id']
        self.stream_process = multiprocessing.Process(target=self.client.filter, kwargs={'tweet_fields': tweet_fields})  # type: ignore
        self.stream_process.daemon=True
        self.stream_process.start()
        self.logger.info("Streaming has been started")
        return True
        
    def read1(self):
        if self.stream_process.is_alive():  # type: ignore
            # get_nowait will throw Empty error if no elements in the queue
            out = self.process_queue.get_nowait()
            self.logger.info(out)
            return out
        else:
            raise RuntimeError("Stream is no longer alive")

if __name__ == "__main__":
    # logging.basicConfig(level=logging.INFO)
    
    producer = TweetProducer()
    producer.connect_source()
    producer.start_stream()
    
    for i in range(200):
        try:
            print(producer.read1())
        except Empty:
            pass
        time.sleep(2)
        
