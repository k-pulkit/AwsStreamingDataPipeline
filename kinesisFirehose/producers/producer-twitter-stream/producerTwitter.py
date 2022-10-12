import multiprocessing
import tweepy as tw
import logging
import json
from keys import *
from io import BufferedReader
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
           
def start_twitter_client(processQueue):
    logger = logging.getLogger("TWEET PRODUCER")
    
    logger.info("Reading list of stocks to pull")
    df = pd.read_csv("sp500.csv").iloc[:100]
    print(df.head(), "\n")
    
    logger.info("Creating symbol list to search on twitter")
    symbols_list = df.Symbol.map(lambda x: '$'+x).to_list()\
                        + df.Symbol.map(lambda x: '#'+x).to_list()
    symbols = set(symbols_list)
    print(symbols_list[:5], " ...")
           
    # create rules to pull the data
    q = Q()
    g = 30
    # divide the symbols into groups of 30
    for i in range(0, len(symbols_list), g):
        q.put((i,i+g))
        
    # Creating the client
    logger.info("Trying to create connection with Twitter API")
    client = TClient(BEARER_TOKEN, processQueue=processQueue)
    
    # Delete rules from previous runs
    try:
        existing_rules = [i.id for i in client.get_rules().data]
        client.delete_rules(existing_rules)
    except:
        pass
    
    # Set a base pattern for search
    #base_pat = "(buy OR sell OR win OR lose OR markets OR trend OR index OR bullish OR bearish OR stock OR market OR investing OR investment OR economy OR buy OR opinion)"
    base_pat = "(has:hashtags OR has:cashtags) lang:en -algo -software -alerts"
    
    # Add the symbols to create multiple filter rules for the incoming stream
    while not q.empty():
        a, b = q.get()
        symbols_subset = symbols_list[a:b]
        pat = f"-is:retweet ({' OR '.join(symbols_subset)}) {base_pat}"
        
        # add rules to client
        client.add_rules(tw.StreamRule(value = pat, tag = str(symbols_subset)))
        
    # print the added rules
    # print(client.get_rules())
        
    # start the stream
    client.filter(tweet_fields = ['created_at', 'text', 'geo', 'lang', 'referenced_tweets', 'organic_metrics', 'public_metrics', 'possibly_sensitive', 'in_reply_to_user_id'])
              
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    #
    queue = multiprocessing.Queue()
    process = multiprocessing.Process(target=start_twitter_client, kwargs={'processQueue': queue})
    process.daemon=True
    process.start()
    
    for i in range(20):
        print(queue.qsize())
        print(queue.get())
        time.sleep(5)
        
